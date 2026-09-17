//! Real AppBuilder/listener/shutdown ownership, with user-supplied workers.
use super::*;
use crate::{shutdown_report::HttpShutdownCompletion, ExecutionCancellation};
use lily_injection::{
    ApplicationScopeFactory, Injectable, InjectionError, ProcessContext, ServiceTrait,
};
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
                        futures::future::pending::<Result<(), InjectionError>>().await
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
            futures::future::pending::<()>().await;
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
                        futures::future::pending::<()>().await;
                    }
                    token.cancelled().await;
                    service.probe.event("cancel-observed");
                    Ok::<_, InjectionError>(())
                })
            })
            .await
    }
}

async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .expect("background HTTP barrier")
}
fn shorten(app: &mut App) {
    let lifecycle = Arc::get_mut(&mut app.lifecycle).unwrap();
    lifecycle.budget =
        ShutdownBudget::for_state(Duration::from_secs(2), lifecycle.shutdown_state.clone());
}
async fn application<const MODE: u8>() -> (App, Arc<BackgroundProbe>) {
    let mut app = AppBuilder::new("127.0.0.1:0")
        .tracing_disabled()
        .add_background_service::<BackgroundWorker<MODE>>()
        .build()
        .await
        .unwrap();
    shorten(&mut app);
    let probe = app
        .container
        .resolve::<BackgroundProbe>(None)
        .await
        .unwrap();
    (app, probe)
}
fn start(app: &App) -> tokio::task::JoinHandle<io::Result<()>> {
    let runtime = app.clone();
    tokio::spawn(async move {
        runtime
            .start_with_cancellation(CancellationToken::new())
            .await
    })
}
async fn ready(app: &App) {
    bounded(async {
        while app.bound_address().is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await;
}

#[tokio::test]
async fn background_build_and_close_without_start_never_execute() {
    let (app, probe) = application::<0>().await;
    assert_eq!(probe.count("constructed"), 1);
    assert_eq!(probe.count("executing"), 0);
    bounded(app.close()).await.unwrap();
    let report = app.shutdown_report().unwrap();
    assert!(report.succeeded(), "{report:?}");
    assert_eq!(report.dependencies.background.stopped_before_start, 1);
    assert_eq!(probe.count("di-disposed"), 1);
    app.close().await.unwrap();
    assert_eq!(probe.count("di-disposed"), 1);
    assert!(app
        .clone()
        .start_with_cancellation(CancellationToken::new())
        .await
        .is_err());
}

#[tokio::test]
async fn background_bind_failure_never_executes_and_joins_prepared_workers() {
    let held = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let app = AppBuilder::new(&held.local_addr().unwrap().to_string())
        .add_background_service::<BackgroundWorker<0>>()
        .build()
        .await
        .unwrap();
    let probe = app
        .container
        .resolve::<BackgroundProbe>(None)
        .await
        .unwrap();
    assert!(bounded(start(&app)).await.unwrap().is_err());
    let report = app.shutdown_report().unwrap();
    assert!(report.terminal(), "{report:?}");
    assert_eq!(report.dependencies.background.started, 0);
    assert_eq!(report.dependencies.background.stopped_before_start, 1);
    assert_eq!(probe.count("di-disposed"), 1);
}

#[tokio::test]
async fn background_disposal_is_a_barrier_before_http_di_shutdown() {
    let (app, probe) = application::<0>().await;
    probe.block_disposal.store(true, Ordering::Release);
    let running = start(&app);
    ready(&app).await;
    probe.until("scope-created", 1).await;
    assert_eq!(probe.count("cancel-observed"), 0);
    let observer = app.clone();
    let closing = tokio::spawn(async move { observer.close().await });
    probe.until("disposing", 1).await;
    assert_eq!(probe.count("di-disposed"), 0);
    assert!(!lily_injection::__private::container_shutdown_started(
        &app.container
    ));
    let snapshot = app
        .lifecycle
        .dependencies
        .background
        .get()
        .unwrap()
        .snapshot();
    assert_eq!(snapshot.scopes.outstanding, 1);
    probe.dispose_release.cancel();
    bounded(closing).await.unwrap().unwrap();
    bounded(running).await.unwrap().unwrap();
    assert!(app.shutdown_report().unwrap().succeeded());
    probe.before("cancel-observed", "disposing");
    probe.before("disposed", "scope-dropped");
    probe.before("scope-dropped", "di-disposed");
}

#[tokio::test]
async fn background_error_and_panic_stop_http_host_and_fail_terminal_report() {
    for panic in [false, true] {
        let (app, probe) = if panic {
            application::<3>().await
        } else {
            application::<2>().await
        };
        let root = start(&app);
        ready(&app).await;
        probe.until("executing", 1).await;
        probe.release.cancel();
        assert!(bounded(root).await.unwrap().is_err());
        let report = app.shutdown_report().unwrap();
        assert!(report.terminal(), "{report:?}");
        assert_eq!(report.completion, HttpShutdownCompletion::TerminalFailed);
        assert_eq!(report.dependencies.background.panicked, usize::from(panic));
        assert_eq!(report.dependencies.background.failed, usize::from(!panic));
        assert_eq!(probe.count("di-disposed"), 1);
    }
}

#[tokio::test]
async fn background_one_shot_completion_does_not_stop_listener() {
    let (app, probe) = application::<4>().await;
    let root = start(&app);
    ready(&app).await;
    probe.until("execution-dropped", 1).await;
    assert!(!root.is_finished());
    assert!(!app.lifecycle.shutdown_state.is_shutdown_initiated());
    app.close().await.unwrap();
    bounded(root).await.unwrap().unwrap();
    assert_eq!(
        app.shutdown_report()
            .unwrap()
            .dependencies
            .background
            .completed,
        1
    );
}

#[tokio::test]
async fn background_ignoring_workers_share_budget_and_survive_close_waiter_abort() {
    let mut app = AppBuilder::new("127.0.0.1:0")
        .add_background_service::<BackgroundWorker<1, 0>>()
        .add_background_service::<BackgroundWorker<1, 0>>()
        .add_background_service::<BackgroundWorker<1, 1>>()
        .build()
        .await
        .unwrap();
    shorten(&mut app);
    let probe = app
        .container
        .resolve::<BackgroundProbe>(None)
        .await
        .unwrap();
    let root = start(&app);
    ready(&app).await;
    probe.until("scope-created", 2).await;
    let observer = app.clone();
    let close = tokio::spawn(async move { observer.close().await });
    bounded(async {
        while app.lifecycle.budget.deadlines().is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let original = app.lifecycle.budget.begin();
    close.abort();
    assert!(close.await.unwrap_err().is_cancelled());
    app.close().await.unwrap();
    bounded(root).await.unwrap().unwrap();
    let report = app.shutdown_report().unwrap();
    assert_eq!(
        report.completion,
        HttpShutdownCompletion::ForcedCompleted,
        "{report:?}"
    );
    assert_eq!(report.dependencies.background.aborted, 2);
    assert_eq!(report.dependencies.background.joined, 2);
    assert!(report.observed_at >= original.at(ShutdownStage::Cooperative));
    assert!(report.observed_at < original.at(ShutdownStage::Reconcile));
    assert_eq!(probe.count("scope-dropped"), 2);
    assert_eq!(probe.count("di-disposed"), 1);
}

#[tokio::test]
async fn background_force_does_not_wait_for_cooperative_cutoff() {
    let (app, probe) = application::<1>().await;
    let root = start(&app);
    ready(&app).await;
    probe.until("scope-created", 1).await;
    app.lifecycle.begin_shutdown();
    let limits = app.lifecycle.budget.begin();
    app.lifecycle.shutdown_state.request_force();
    bounded(app.close()).await.unwrap();
    bounded(root).await.unwrap().unwrap();
    let report = app.shutdown_report().unwrap();
    assert!(report.observed_at < limits.at(ShutdownStage::Cooperative));
    assert_eq!(report.dependencies.background.aborted, 1);
    probe.before("scope-dropped", "di-disposed");
}

#[tokio::test]
async fn background_constructor_failure_leaves_external_container_reusable() {
    let container = Arc::new(ApplicationContainer::build().await.unwrap());
    let probe = container.resolve::<BackgroundProbe>(None).await.unwrap();
    let result = AppBuilder::new("127.0.0.1:0")
        .container(container.clone())
        .add_background_service::<BackgroundWorker<5>>()
        .build()
        .await;
    assert!(matches!(result, Err(AppBuildError::BackgroundService(_))));
    assert_eq!(container.active_scope_count(), 0);
    assert_eq!(probe.count("di-disposed"), 0);
    let app = AppBuilder::new("127.0.0.1:0")
        .container(container.clone())
        .build()
        .await
        .unwrap();
    app.close().await.unwrap();
    container.close().await.unwrap();
}

#[tokio::test]
async fn background_cancelled_build_retains_constructor_and_its_scope_cleanup() {
    let configuration = tempfile::tempdir().unwrap();
    let path = configuration.path().join("lily.toml");
    std::fs::write(&path, "[lifecycle]\nshutdown_timeout_secs = 2\n").unwrap();
    let container = Arc::new(
        ApplicationContainer::builder()
            .seed_singleton(ConfigService::new(lily_config::ConfigOptions::test(path)))
            .build()
            .await
            .unwrap(),
    );
    let probe = container.resolve::<BackgroundProbe>(None).await.unwrap();
    let mut build = Box::pin(
        AppBuilder::new("127.0.0.1:0")
            .container(container.clone())
            .add_background_service::<BackgroundWorker<6>>()
            .build(),
    );
    tokio::select! {
        result = &mut build => panic!("constructor must remain pending: {:?}", result.err()),
        _ = probe.until("scope-created", 1) => {},
    }
    drop(build);
    // The independent rollback receives the same budget; force constructor
    // cleanup through its normal cutoff, without cancelling its owner task.
    probe.until("scope-dropped", 1).await;
    assert!(bounded(BUILD_ROLLBACK_TASKS.get().unwrap().wait())
        .await
        .is_terminal());
    assert_eq!(container.active_scope_count(), 0);
    assert_eq!(probe.count("executing"), 0);
    assert_eq!(probe.count("di-disposed"), 0);
    container.close().await.unwrap();
}

#[tokio::test]
async fn background_drop_of_unstarted_app_reconciles_prepared_tasks_before_dependencies() {
    let (app, probe) = application::<0>().await;
    drop(app);
    probe.until("di-disposed", 1).await;
    assert!(bounded(BUILD_ROLLBACK_TASKS.get().unwrap().wait())
        .await
        .is_terminal());
    assert_eq!(probe.count("executing"), 0);
}

#[tokio::test]
async fn background_shutdown_closes_only_its_own_scopes_in_external_container() {
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
    let app = AppBuilder::new("127.0.0.1:0")
        .container(container.clone())
        .add_background_service::<BackgroundWorker<0>>()
        .build()
        .await
        .unwrap();
    let root = start(&app);
    ready(&app).await;
    probe.until("scope-created", 2).await;
    app.close().await.unwrap();
    bounded(root).await.unwrap().unwrap();
    let report = app.shutdown_report().unwrap();
    assert!(report.succeeded(), "{report:?}");
    assert_eq!(container.active_scope_count(), 1);
    assert_eq!(probe.count("di-disposed"), 0);
    assert_eq!(probe.active.load(Ordering::Acquire), 1);
    unrelated.close().await.unwrap();
    container.close().await.unwrap();
    assert_eq!(probe.count("di-disposed"), 1);
}

#[tokio::test]
async fn background_start_waiter_abort_requests_shutdown_without_losing_owner() {
    let (app, probe) = application::<0>().await;
    let root_waiter = start(&app);
    ready(&app).await;
    probe.until("scope-created", 1).await;
    root_waiter.abort();
    assert!(root_waiter.await.unwrap_err().is_cancelled());
    bounded(app.close()).await.unwrap();
    assert_eq!(probe.count("cancel-observed"), 1);
    assert!(app.shutdown_report().unwrap().succeeded());
    probe.before("scope-dropped", "di-disposed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn background_blocked_destructor_keeps_di_alive_and_freezes_incomplete_evidence() {
    let (app, probe) = application::<7>().await;
    let (release, wait) = std::sync::mpsc::channel();
    *probe.drop_gate.lock().unwrap() = Some(wait);
    // Always release the deliberately blocked OS thread if the assertion path
    // fails. This guard is a test watchdog, never the framework's deadline.
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
    let root = start(&app);
    ready(&app).await;
    probe.until("executing", 1).await;
    app.lifecycle.begin_shutdown();
    app.lifecycle.shutdown_state.request_force();
    probe.until("drop-blocked", 1).await;
    let closed = bounded(app.close()).await;
    let root_result = bounded(root).await.unwrap();
    let report = app.shutdown_report().unwrap();
    let di_started = lily_injection::__private::container_shutdown_started(&app.container);
    // Release before assertions so any failing assertion still joins the task.
    release.send(()).unwrap();
    done.send(()).unwrap();
    assert!(!watchdog.join().unwrap());
    let background = app.lifecycle.dependencies.background.get().unwrap();
    assert!(
        background
            .wait_stopped_before(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
    );
    assert!(closed.is_err());
    assert!(root_result.is_err());
    assert!(!di_started);
    assert_eq!(report.completion, HttpShutdownCompletion::Incomplete);
    assert_eq!(report.dependencies.background.outstanding, 1);
    assert_eq!(report.dependencies.background.scopes.outstanding, 0);
    assert!(report.dependencies.background.execution_deadline_missed);
    assert_eq!(probe.count("di-disposed"), 0);
    assert!(app.close().await.is_err());
    assert_eq!(app.shutdown_report().unwrap(), report);
    assert!(!lily_injection::__private::container_shutdown_started(
        &app.container
    ));
    // Explicit fixture teardown outside the frozen HTTP attempt.
    let teardown =
        ShutdownBudget::from_started(Duration::from_secs(2), tokio::time::Instant::now());
    app.lifecycle
        .dependencies
        .close_di(&teardown)
        .await
        .unwrap();
    app.lifecycle
        .dependencies
        .close_tracing(&teardown)
        .await
        .unwrap();
    app.lifecycle
        .dependencies
        .reconcile(&teardown)
        .await
        .unwrap();
    probe.before("drop-released", "di-disposed");
}
