use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use lily_injection::Injectable;
use lily_injection::{InjectionError, ServiceTrait, async_trait::async_trait};
use lily_websocket::{
    BackplaneRequirement, Extensions, ServerConfig, WebSocketBackplane, WebSocketBackplaneError,
    WebSocketBackplaneEvent, WebSocketBackplaneFrame, WebSocketBackplaneInboundAdmission,
    WebSocketBackplaneInitError, WebSocketBackplanePublishReceipt, WsAppBuilder,
};
use tokio::sync::Semaphore;
use tokio::time::timeout;

const TEST_DEADLINE: Duration = Duration::from_secs(3);

static ROOT_INITIALIZE_CALLS: AtomicUsize = AtomicUsize::new(0);
static ROOT_DISPOSE_CALLS: AtomicUsize = AtomicUsize::new(0);
static ROOT_DROP_CALLS: AtomicUsize = AtomicUsize::new(0);
static EAGER_INITIALIZE_CALLS: AtomicUsize = AtomicUsize::new(0);
static EAGER_DISPOSE_CALLS: AtomicUsize = AtomicUsize::new(0);
static EAGER_DROP_CALLS: AtomicUsize = AtomicUsize::new(0);
static TOTAL_SERVICE_DROPS: AtomicUsize = AtomicUsize::new(0);

static BACKPLANE_CONSTRUCTOR_ENTERED: Semaphore = Semaphore::const_new(0);
static BACKPLANE_CONSTRUCTOR_DROPPED: Semaphore = Semaphore::const_new(0);
static EAGER_DISPOSE_ENTERED: Semaphore = Semaphore::const_new(0);
static RELEASE_EAGER_DISPOSE: Semaphore = Semaphore::const_new(0);
static ROOT_DISPOSED: Semaphore = Semaphore::const_new(0);
static ALL_SERVICES_DROPPED: Semaphore = Semaphore::const_new(0);

static EVENTS: Mutex<Vec<LifecycleEvent>> = Mutex::new(Vec::new());

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LifecycleEvent {
    RootInitialize,
    EagerInitialize,
    BackplaneConstructorEntered,
    BuildAbortRequested,
    BackplaneConstructorDropped,
    EagerDisposeEntered,
    EagerDisposeCompleted,
    RootDispose,
}

fn record(event: LifecycleEvent) {
    EVENTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push(event);
}

fn events() -> Vec<LifecycleEvent> {
    EVENTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
}

async fn wait_for(signal: &'static Semaphore, failure: &'static str) {
    let permit = timeout(TEST_DEADLINE, signal.acquire())
        .await
        .expect(failure)
        .expect("test lifecycle semaphore must remain open");
    permit.forget();
}

fn record_service_drop(counter: &AtomicUsize) {
    counter.fetch_add(1, Ordering::SeqCst);
    if TOTAL_SERVICE_DROPS.fetch_add(1, Ordering::SeqCst) == 1 {
        ALL_SERVICES_DROPPED.add_permits(1);
    }
}

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
struct CancellationRootService;

#[async_trait]
impl ServiceTrait for CancellationRootService {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        ROOT_INITIALIZE_CALLS.fetch_add(1, Ordering::SeqCst);
        record(LifecycleEvent::RootInitialize);
        Ok(())
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        ROOT_DISPOSE_CALLS.fetch_add(1, Ordering::SeqCst);
        record(LifecycleEvent::RootDispose);
        ROOT_DISPOSED.add_permits(1);
        Ok(())
    }
}

impl Drop for CancellationRootService {
    fn drop(&mut self) {
        record_service_drop(&ROOT_DROP_CALLS);
    }
}

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
struct CancellationEagerService {
    #[inject]
    _root: Arc<CancellationRootService>,
}

#[async_trait]
impl ServiceTrait for CancellationEagerService {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        EAGER_INITIALIZE_CALLS.fetch_add(1, Ordering::SeqCst);
        record(LifecycleEvent::EagerInitialize);
        Ok(())
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        EAGER_DISPOSE_CALLS.fetch_add(1, Ordering::SeqCst);
        record(LifecycleEvent::EagerDisposeEntered);
        EAGER_DISPOSE_ENTERED.add_permits(1);

        let permit = RELEASE_EAGER_DISPOSE.acquire().await.map_err(|_| {
            InjectionError::DisposeError(
                "build-cancellation test release semaphore closed".to_owned(),
            )
        })?;
        permit.forget();

        record(LifecycleEvent::EagerDisposeCompleted);
        Ok(())
    }
}

impl Drop for CancellationEagerService {
    fn drop(&mut self) {
        record_service_drop(&EAGER_DROP_CALLS);
    }
}

struct PendingConstructionDropGuard;

impl Drop for PendingConstructionDropGuard {
    fn drop(&mut self) {
        record(LifecycleEvent::BackplaneConstructorDropped);
        BACKPLANE_CONSTRUCTOR_DROPPED.add_permits(1);
    }
}

struct PendingConstructionBackplane;

#[async_trait]
impl WebSocketBackplane for PendingConstructionBackplane {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketBackplaneInitError> {
        let _drop_guard = PendingConstructionDropGuard;
        record(LifecycleEvent::BackplaneConstructorEntered);
        BACKPLANE_CONSTRUCTOR_ENTERED.add_permits(1);
        std::future::pending().await
    }

    async fn publish(
        &self,
        _frame: WebSocketBackplaneFrame,
    ) -> Result<WebSocketBackplanePublishReceipt, WebSocketBackplaneError> {
        unreachable!("a pending constructor cannot publish")
    }

    async fn receive(
        &self,
        _admission: WebSocketBackplaneInboundAdmission,
    ) -> Result<WebSocketBackplaneEvent, WebSocketBackplaneError> {
        unreachable!("a pending constructor cannot receive")
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aborting_app_owned_build_completes_reverse_di_rollback_exactly_once() {
    let builder = WsAppBuilder::new("127.0.0.1:0")
        .config(ServerConfig::default())
        .backplane::<PendingConstructionBackplane>(BackplaneRequirement::Required);
    let mut build = tokio::spawn(async move { builder.build().await });

    wait_for(
        &BACKPLANE_CONSTRUCTOR_ENTERED,
        "pending backplane constructor was not entered",
    )
    .await;
    assert_eq!(ROOT_INITIALIZE_CALLS.load(Ordering::SeqCst), 1);
    assert_eq!(EAGER_INITIALIZE_CALLS.load(Ordering::SeqCst), 1);

    record(LifecycleEvent::BuildAbortRequested);
    build.abort();
    let join = timeout(TEST_DEADLINE, &mut build)
        .await
        .expect("aborted app build did not terminate");
    assert!(
        matches!(join, Err(error) if error.is_cancelled()),
        "app build must terminate as a cancelled task"
    );

    wait_for(
        &BACKPLANE_CONSTRUCTOR_DROPPED,
        "pending backplane constructor future was not dropped",
    )
    .await;
    wait_for(
        &EAGER_DISPOSE_ENTERED,
        "app-owned DI rollback did not enter the eager disposer",
    )
    .await;

    assert_eq!(EAGER_DISPOSE_CALLS.load(Ordering::SeqCst), 1);
    assert_eq!(ROOT_DISPOSE_CALLS.load(Ordering::SeqCst), 0);
    assert_eq!(
        events(),
        vec![
            LifecycleEvent::RootInitialize,
            LifecycleEvent::EagerInitialize,
            LifecycleEvent::BackplaneConstructorEntered,
            LifecycleEvent::BuildAbortRequested,
            LifecycleEvent::BackplaneConstructorDropped,
            LifecycleEvent::EagerDisposeEntered,
        ],
        "root cleanup must not overtake the pending eager disposer"
    );

    RELEASE_EAGER_DISPOSE.add_permits(1);
    wait_for(
        &ROOT_DISPOSED,
        "app-owned DI rollback did not continue to the root disposer",
    )
    .await;
    wait_for(
        &ALL_SERVICES_DROPPED,
        "rolled-back DI services were not released",
    )
    .await;

    assert_eq!(ROOT_DISPOSE_CALLS.load(Ordering::SeqCst), 1);
    assert_eq!(EAGER_DISPOSE_CALLS.load(Ordering::SeqCst), 1);
    assert_eq!(ROOT_DROP_CALLS.load(Ordering::SeqCst), 1);
    assert_eq!(EAGER_DROP_CALLS.load(Ordering::SeqCst), 1);
    assert_eq!(
        events(),
        vec![
            LifecycleEvent::RootInitialize,
            LifecycleEvent::EagerInitialize,
            LifecycleEvent::BackplaneConstructorEntered,
            LifecycleEvent::BuildAbortRequested,
            LifecycleEvent::BackplaneConstructorDropped,
            LifecycleEvent::EagerDisposeEntered,
            LifecycleEvent::EagerDisposeCompleted,
            LifecycleEvent::RootDispose,
        ],
        "cancelled build must dispose app-owned DI in exact reverse order"
    );
}
