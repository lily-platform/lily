use lily_background_service::{
    BackgroundServiceRuntime, BackgroundServiceTrait, BackgroundServices,
    BackgroundShutdownDeadlines, ExecutionCancellation,
};
use lily_injectable_derive::Injectable;
use lily_injection::{
    ApplicationContainer, ApplicationScopeFactory, InjectionError, ProcessContext, ServiceTrait,
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::Duration;
use tokio::{sync::Notify, time::Instant};
use tokio_util::sync::CancellationToken;

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
struct Probe {
    events: Mutex<Vec<(&'static str, u8, Option<u64>)>>,
    changed: Notify,
    block_disposal: AtomicBool,
    fail_disposal: AtomicBool,
    release: CancellationToken,
}
impl Probe {
    fn record(&self, event: &'static str, id: u8) {
        self.events.lock().unwrap().push((
            event,
            id,
            ProcessContext::current().map(|c| c.process_id),
        ));
        self.changed.notify_waiters();
    }
    async fn until(&self, event: &str, count: usize) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.count(event) >= count {
                    return;
                }
                changed.await;
            }
        })
        .await
        .expect("lifecycle barrier");
    }
    fn count(&self, event: &str) -> usize {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.0 == event)
            .count()
    }
}
#[async_trait::async_trait]
impl ServiceTrait for Probe {
    async fn dispose(&self) -> Result<(), InjectionError> {
        self.record("di-disposed", 0);
        Ok(())
    }
}

#[derive(Default, Injectable)]
#[service(lifetime = "Scoped")]
struct Scoped {
    #[inject]
    probe: Arc<Probe>,
    process: u64,
}
#[async_trait::async_trait]
impl ServiceTrait for Scoped {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        self.process = ProcessContext::current().unwrap().process_id;
        self.probe.record("scope-created", 0);
        Ok(())
    }
    async fn dispose(&self) -> Result<(), InjectionError> {
        assert_eq!(ProcessContext::current().unwrap().process_id, self.process);
        let _guard = Mark(self.probe.clone(), "disposer-dropped", 0);
        self.probe.record("disposing", 0);
        if self.probe.block_disposal.load(Ordering::Acquire) {
            self.probe.release.cancelled().await;
        }
        if self.probe.fail_disposal.load(Ordering::Acquire) {
            return Err(InjectionError::DisposeError("probe".into()));
        }
        self.probe.record("disposed", 0);
        Ok(())
    }
}
struct Mark(Arc<Probe>, &'static str, u8);
impl Drop for Mark {
    fn drop(&mut self) {
        self.0.record(self.1, self.2);
    }
}

struct Worker<const MODE: u8, const ID: u8 = 0> {
    scopes: Arc<ApplicationScopeFactory>,
    probe: Arc<Probe>,
}
#[async_trait::async_trait]
impl<const MODE: u8, const ID: u8> BackgroundServiceTrait for Worker<MODE, ID> {
    type Error = InjectionError;
    async fn new(scopes: Arc<ApplicationScopeFactory>) -> Result<Self, Self::Error> {
        let probe = scopes
            .create_scope(ProcessContext::new())?
            .run(|extensions| {
                Box::pin(async move {
                    let probe = extensions.get_service::<Probe>(None).await?;
                    probe.record("constructing", ID);
                    if MODE == 7 {
                        extensions.get_service::<Scoped>(None).await?;
                        futures::future::pending::<()>().await;
                    }
                    Ok::<_, InjectionError>(probe)
                })
            })
            .await?;
        if MODE == 8 {
            return Err(InjectionError::DisposeError("constructor test".into()));
        }
        if MODE == 9 {
            panic!("constructor test");
        }
        Ok(Self { scopes, probe })
    }
    async fn execute_async(&mut self, token: ExecutionCancellation) -> Result<(), Self::Error> {
        let _guard = Mark(self.probe.clone(), "execution-dropped", ID);
        self.probe.record("executing", ID);
        assert!(!token.is_cancelled());
        if MODE == 2 {
            return Err(InjectionError::DisposeError("worker test".into()));
        }
        if MODE == 3 {
            panic!("worker test");
        }
        if MODE == 4 {
            return Ok(());
        }
        if MODE == 5 {
            token.cancelled().await;
        }
        self.scopes
            .create_scope(ProcessContext::new())?
            .run(move |extensions| {
                Box::pin(async move {
                    let scoped = extensions.get_service::<Scoped>(None).await?;
                    let again = extensions.get_service::<Scoped>(None).await?;
                    assert!(Arc::ptr_eq(&scoped, &again));
                    if MODE == 1 {
                        futures::future::pending::<()>().await;
                    }
                    token.cancelled().await;
                    scoped.probe.record("cancel-observed", ID);
                    Ok::<_, InjectionError>(())
                })
            })
            .await
    }
}

fn deadlines(start: Instant) -> BackgroundShutdownDeadlines {
    BackgroundShutdownDeadlines {
        cooperative: start + Duration::from_millis(600),
        execution_stop: start + Duration::from_millis(750),
        cleanup: start + Duration::from_millis(850),
        reconcile: start + Duration::from_millis(900),
    }
}
async fn runtime(
    services: BackgroundServices,
) -> (
    ApplicationContainer,
    Arc<Probe>,
    Arc<BackgroundServiceRuntime>,
) {
    let container = ApplicationContainer::build().await.unwrap();
    let probe = container.resolve::<Probe>(None).await.unwrap();
    let runtime = services.into_runtime(&container);
    (container, probe, runtime)
}

#[tokio::test(start_paused = true)]
async fn construction_is_separate_from_execution_deduplicated_and_started_once() {
    let mut services = BackgroundServices::default();
    services.add::<Worker<0>>();
    services.add::<Worker<0>>();
    let (container, probe, runtime) = runtime(services).await;
    runtime.initialize().await.unwrap();
    runtime.initialize().await.unwrap();
    assert_eq!(probe.count("constructing"), 1);
    assert_eq!(probe.count("executing"), 0);
    runtime.start().unwrap();
    runtime.start().unwrap();
    probe.until("scope-created", 1).await;
    assert_eq!(probe.count("executing"), 1);
    tokio::time::advance(Duration::from_secs(5)).await;
    assert_eq!(probe.count("cancel-observed"), 0);
    let limits = deadlines(Instant::now());
    runtime.begin_shutdown(limits);
    assert!(runtime.wait_stopped_before(limits.reconcile).await);
    let snapshot = runtime.snapshot();
    assert!(snapshot.succeeded(), "{snapshot:?}");
    assert_eq!(
        (snapshot.registered, snapshot.started, snapshot.completed),
        (1, 1, 1)
    );
    assert_eq!(snapshot.aborted, 0);
    assert_eq!(probe.count("cancel-observed"), 1);
    assert_eq!(probe.count("disposed"), 1);
    assert_eq!(probe.count("di-disposed"), 0);
    container.close().await.unwrap();
    assert_eq!(probe.count("di-disposed"), 1);
}

#[tokio::test(start_paused = true)]
async fn close_before_start_never_executes_and_one_shot_completion_does_not_signal_failure() {
    for start in [false, true] {
        let mut services = BackgroundServices::default();
        services.add::<Worker<4>>();
        let (container, probe, runtime) = runtime(services).await;
        runtime.initialize().await.unwrap();
        if start {
            runtime.start().unwrap();
            probe.until("execution-dropped", 1).await;
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(1), runtime.wait_for_failure())
                .await
                .is_err()
        );
        let limits = deadlines(Instant::now());
        runtime.begin_shutdown(limits);
        assert!(runtime.wait_stopped_before(limits.reconcile).await);
        assert!(runtime.snapshot().succeeded());
        assert_eq!(runtime.snapshot().started, usize::from(start));
        assert_eq!(runtime.snapshot().stopped_before_start, usize::from(!start));
        container.close().await.unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn error_and_panic_notify_host_and_remain_failure_after_all_joins() {
    for panic in [false, true] {
        let mut services = BackgroundServices::default();
        if panic {
            services.add::<Worker<3>>();
        } else {
            services.add::<Worker<2>>();
        }
        let (container, _, runtime) = runtime(services).await;
        runtime.initialize().await.unwrap();
        runtime.start().unwrap();
        tokio::time::timeout(Duration::from_secs(1), runtime.wait_for_failure())
            .await
            .unwrap();
        let limits = deadlines(Instant::now());
        runtime.begin_shutdown(limits);
        assert!(runtime.wait_stopped_before(limits.reconcile).await);
        let report = runtime.snapshot();
        assert!(report.is_terminal());
        assert!(!report.succeeded());
        assert_eq!(report.panicked, usize::from(panic));
        assert_eq!(report.failed, usize::from(!panic));
        assert_eq!(report.joined, 1);
        container.close().await.unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn ignoring_workers_share_original_deadline_even_after_waiter_cancellation() {
    let mut services = BackgroundServices::default();
    services.add::<Worker<1, 0>>();
    services.add::<Worker<1, 1>>();
    let (container, probe, runtime) = runtime(services).await;
    runtime.initialize().await.unwrap();
    runtime.start().unwrap();
    probe.until("scope-created", 2).await;
    let started = Instant::now();
    let limits = deadlines(started);
    runtime.begin_shutdown(limits);
    assert!(
        !runtime
            .wait_stopped_before(started + Duration::from_millis(10))
            .await
    );
    assert_eq!(runtime.snapshot().joined, 0);
    runtime.begin_shutdown(deadlines(started + Duration::from_secs(60)));
    assert!(runtime.wait_stopped_before(limits.reconcile).await);
    assert_eq!(Instant::now(), limits.cooperative);
    let report = runtime.snapshot();
    assert!(report.succeeded(), "{report:?}");
    assert_eq!(
        (report.aborted, report.abort_requested, report.joined),
        (2, 2, 2)
    );
    assert_eq!(probe.count("execution-dropped"), 2);
    assert_eq!(probe.count("disposed"), 2);
    let ids: Vec<_> = probe
        .events
        .lock()
        .unwrap()
        .iter()
        .filter(|e| e.0 == "scope-created")
        .map(|e| e.2.unwrap())
        .collect();
    assert_ne!(ids[0], ids[1]);
    container.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn force_joins_execution_then_observes_actual_disposer_timeout() {
    let mut services = BackgroundServices::default();
    services.add::<Worker<1>>();
    let (container, probe, runtime) = runtime(services).await;
    probe.block_disposal.store(true, Ordering::Release);
    runtime.initialize().await.unwrap();
    runtime.start().unwrap();
    probe.until("scope-created", 1).await;
    let limits = deadlines(Instant::now());
    runtime.begin_shutdown(limits);
    runtime.force_stop();
    probe.until("disposing", 1).await;
    assert_eq!(runtime.snapshot().outstanding, 0);
    assert_eq!(runtime.snapshot().scopes.outstanding, 1);
    assert!(!runtime.snapshot().is_terminal());
    assert!(runtime.wait_stopped_before(limits.reconcile).await);
    assert_eq!(Instant::now(), limits.cleanup);
    assert_eq!(probe.count("disposer-dropped"), 1);
    assert_eq!(probe.count("disposed"), 0);
    assert_eq!(runtime.snapshot().scopes.failed, 1);
    assert!(!runtime.snapshot().succeeded());
    assert!(
        tokio::time::timeout(Duration::from_millis(1), runtime.wait_for_failure())
            .await
            .is_err(),
        "a completed shutdown with a cleanup failure must not invent an unhandled worker error"
    );
    assert!(container.close().await.is_err());
}

#[tokio::test(start_paused = true)]
async fn cooperative_worker_can_open_finalization_scope_after_cancellation() {
    let mut services = BackgroundServices::default();
    services.add::<Worker<5>>();
    let (container, probe, runtime) = runtime(services).await;
    runtime.initialize().await.unwrap();
    runtime.start().unwrap();
    probe.until("executing", 1).await;
    assert_eq!(probe.count("scope-created"), 0);
    let limits = deadlines(Instant::now());
    runtime.begin_shutdown(limits);
    assert!(runtime.wait_stopped_before(limits.reconcile).await);
    assert!(runtime.snapshot().succeeded());
    assert_eq!(probe.count("disposed"), 1);
    container.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn cancelled_initialization_waiter_does_not_drop_constructor_ownership() {
    let mut services = BackgroundServices::default();
    services.add::<Worker<7>>();
    let (container, probe, runtime) = runtime(services).await;
    let owner = runtime.clone();
    let waiter = tokio::spawn(async move { owner.initialize().await });
    probe.until("scope-created", 1).await;
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    assert_eq!(runtime.snapshot().outstanding, 1);
    let limits = deadlines(Instant::now());
    runtime.begin_shutdown(limits);
    assert!(runtime.wait_stopped_before(limits.reconcile).await);
    assert_eq!(runtime.snapshot().started, 0);
    assert_eq!(runtime.snapshot().aborted, 1);
    assert_eq!(probe.count("disposed"), 1);
    container.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn constructor_error_or_panic_never_releases_prepared_workers_into_execution() {
    for panic in [false, true] {
        let mut services = BackgroundServices::default();
        services.add::<Worker<0>>();
        if panic {
            services.add::<Worker<9>>();
        } else {
            services.add::<Worker<8>>();
        }
        let (container, probe, runtime) = runtime(services).await;
        assert!(runtime.initialize().await.is_err());
        assert!(runtime.start().is_err());
        let limits = deadlines(Instant::now());
        runtime.begin_shutdown(limits);
        assert!(runtime.wait_stopped_before(limits.reconcile).await);
        let report = runtime.snapshot();
        assert!(report.is_terminal());
        assert!(!report.succeeded());
        assert_eq!(report.started, 0);
        assert_eq!(report.joined, 2);
        assert_eq!(probe.count("executing"), 0);
        container.close().await.unwrap();
    }
}
