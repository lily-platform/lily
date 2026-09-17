use lily_error::injection::InjectionError;
use lily_injectable_derive::Injectable;
use lily_injection::{
    ApplicationContainer, ProcessContext, ServiceTrait, async_trait::async_trait,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

tokio::task_local! {
    static TEST_TASK_VALUE: &'static str;
}

const CONTROL: usize = 0;
const FIRST: usize = 1;
const SECOND: usize = 2;
const LEAF: usize = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PauseAt {
    Never,
    Control,
    Leaf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LifecycleEvent {
    ControlInit,
    FirstInit,
    SecondInit,
    LeafInit,
    LeafDispose,
    SecondDispose,
    FirstDispose,
    ControlDispose,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ContextObservation {
    process_id: Option<u64>,
    span: Option<String>,
    task_value: Option<&'static str>,
}

struct FixtureState {
    pause_at: PauseAt,
    fail_second_dispose: bool,
    expected_drops: usize,
    entered: CancellationToken,
    release: CancellationToken,
    rollback_tail_disposed: CancellationToken,
    all_services_dropped: CancellationToken,
    events: Mutex<Vec<LifecycleEvent>>,
    observations: Mutex<Vec<ContextObservation>>,
    initialized: [AtomicUsize; 4],
    disposed: [AtomicUsize; 4],
    dropped: [AtomicUsize; 4],
}

impl FixtureState {
    fn new(pause_at: PauseAt, expected_drops: usize) -> Arc<Self> {
        Self::new_with_dispose_failure(pause_at, expected_drops, false)
    }

    fn new_with_dispose_failure(
        pause_at: PauseAt,
        expected_drops: usize,
        fail_second_dispose: bool,
    ) -> Arc<Self> {
        Arc::new(Self {
            pause_at,
            fail_second_dispose,
            expected_drops,
            entered: CancellationToken::new(),
            release: CancellationToken::new(),
            rollback_tail_disposed: CancellationToken::new(),
            all_services_dropped: CancellationToken::new(),
            events: Mutex::new(Vec::new()),
            observations: Mutex::new(Vec::new()),
            initialized: std::array::from_fn(|_| AtomicUsize::new(0)),
            disposed: std::array::from_fn(|_| AtomicUsize::new(0)),
            dropped: std::array::from_fn(|_| AtomicUsize::new(0)),
        })
    }

    fn with_second_dispose_failure() -> Arc<Self> {
        Self::new_with_dispose_failure(PauseAt::Leaf, 4, true)
    }

    async fn initialize(&self, index: usize, event: LifecycleEvent, pause_at: PauseAt) {
        self.initialized[index].fetch_add(1, Ordering::SeqCst);
        self.events.lock().unwrap().push(event);
        self.observations.lock().unwrap().push(ContextObservation {
            process_id: ProcessContext::current().map(|context| context.process_id),
            span: tracing::Span::current()
                .metadata()
                .map(|metadata| metadata.name().to_string()),
            task_value: TEST_TASK_VALUE.try_with(|value| *value).ok(),
        });
        if pause_at != PauseAt::Never && self.pause_at == pause_at {
            self.entered.cancel();
            self.release.cancelled().await;
        }
    }

    fn dispose(&self, index: usize, event: LifecycleEvent) {
        self.disposed[index].fetch_add(1, Ordering::SeqCst);
        self.events.lock().unwrap().push(event);
        if index == CONTROL {
            self.rollback_tail_disposed.cancel();
        }
    }

    fn record_drop(&self, index: usize) {
        self.dropped[index].fetch_add(1, Ordering::SeqCst);
        let total = self
            .dropped
            .iter()
            .map(|count| count.load(Ordering::SeqCst))
            .sum::<usize>();
        if total == self.expected_drops {
            self.all_services_dropped.cancel();
        }
    }

    fn counts(counts: &[AtomicUsize; 4]) -> [usize; 4] {
        std::array::from_fn(|index| counts[index].load(Ordering::SeqCst))
    }

    fn assert_counts(&self, initialized: [usize; 4], disposed: [usize; 4], dropped: [usize; 4]) {
        assert_eq!(Self::counts(&self.initialized), initialized);
        assert_eq!(Self::counts(&self.disposed), disposed);
        assert_eq!(Self::counts(&self.dropped), dropped);
    }
}

#[derive(Injectable)]
#[service(lifetime = "Singleton")]
struct RegisteredControl {
    state: Arc<FixtureState>,
}

impl Default for RegisteredControl {
    fn default() -> Self {
        panic!("RegisteredControl must be supplied through seed_singleton")
    }
}

#[async_trait]
impl ServiceTrait for RegisteredControl {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        self.state
            .initialize(CONTROL, LifecycleEvent::ControlInit, PauseAt::Control)
            .await;
        Ok(())
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        self.state.dispose(CONTROL, LifecycleEvent::ControlDispose);
        Ok(())
    }
}

impl Drop for RegisteredControl {
    fn drop(&mut self) {
        self.state.record_drop(CONTROL);
    }
}

#[derive(Injectable)]
#[service(lifetime = "Singleton")]
struct FirstCompleted {
    #[inject]
    control: Arc<RegisteredControl>,
}

#[async_trait]
impl ServiceTrait for FirstCompleted {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        self.control
            .state
            .initialize(FIRST, LifecycleEvent::FirstInit, PauseAt::Never)
            .await;
        Ok(())
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        self.control
            .state
            .dispose(FIRST, LifecycleEvent::FirstDispose);
        Ok(())
    }
}

impl Drop for FirstCompleted {
    fn drop(&mut self) {
        self.control.state.record_drop(FIRST);
    }
}

#[derive(Injectable)]
#[service(lifetime = "Singleton")]
struct SecondCompleted {
    #[inject]
    first: Arc<FirstCompleted>,
}

#[async_trait]
impl ServiceTrait for SecondCompleted {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        self.first
            .control
            .state
            .initialize(SECOND, LifecycleEvent::SecondInit, PauseAt::Never)
            .await;
        Ok(())
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        let state = &self.first.control.state;
        state.dispose(SECOND, LifecycleEvent::SecondDispose);
        if state.fail_second_dispose {
            Err(InjectionError::DisposeError(
                "intentional cancellation rollback failure".to_string(),
            ))
        } else {
            Ok(())
        }
    }
}

impl Drop for SecondCompleted {
    fn drop(&mut self) {
        self.first.control.state.record_drop(SECOND);
    }
}

#[derive(Injectable)]
#[service(lifetime = "Singleton")]
struct PendingLeaf {
    #[inject]
    second: Arc<SecondCompleted>,
}

#[async_trait]
impl ServiceTrait for PendingLeaf {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        self.second
            .first
            .control
            .state
            .initialize(LEAF, LifecycleEvent::LeafInit, PauseAt::Leaf)
            .await;
        Ok(())
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        self.second
            .first
            .control
            .state
            .dispose(LEAF, LifecycleEvent::LeafDispose);
        Ok(())
    }
}

impl Drop for PendingLeaf {
    fn drop(&mut self) {
        self.second.first.control.state.record_drop(LEAF);
    }
}

fn builder(state: Arc<FixtureState>) -> lily_injection::ApplicationContainerBuilder {
    ApplicationContainer::builder().seed_singleton(RegisteredControl { state })
}

async fn wait_for(token: &CancellationToken) {
    tokio::time::timeout(Duration::from_secs(2), token.cancelled())
        .await
        .expect("lifecycle signal timed out");
}

fn reverse_lifecycle_events() -> Vec<LifecycleEvent> {
    vec![
        LifecycleEvent::ControlInit,
        LifecycleEvent::FirstInit,
        LifecycleEvent::SecondInit,
        LifecycleEvent::LeafInit,
        LifecycleEvent::LeafDispose,
        LifecycleEvent::SecondDispose,
        LifecycleEvent::FirstDispose,
        LifecycleEvent::ControlDispose,
    ]
}

#[test]
fn hidden_build_handle_remains_send_and_unpin() {
    fn assert_send_unpin<T: Send + Unpin>() {}
    assert_send_unpin::<lily_injection::__private::ApplicationContainerBuild>();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropping_pending_public_build_future_rolls_back_every_owned_service_once() {
    let state = FixtureState::new(PauseAt::Leaf, 4);
    let mut build = Box::pin(builder(Arc::clone(&state)).build());

    tokio::select! {
        biased;
        result = &mut build => panic!("build unexpectedly completed: {result:?}"),
        () = state.entered.cancelled() => {}
    }
    drop(build);

    wait_for(&state.all_services_dropped).await;
    assert_eq!(*state.events.lock().unwrap(), reverse_lifecycle_events());
    state.assert_counts([1; 4], [1; 4], [1; 4]);
}

#[test]
fn cancellation_rollback_does_not_require_a_tokio_time_driver() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    runtime.block_on(async {
        let state = FixtureState::new(PauseAt::Leaf, 4);
        let mut build = Box::pin(builder(Arc::clone(&state)).build());

        tokio::select! {
            biased;
            result = &mut build => panic!("build unexpectedly completed: {result:?}"),
            () = state.entered.cancelled() => {}
        }
        drop(build);

        for _ in 0..128 {
            if state.all_services_dropped.is_cancelled() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            state.all_services_dropped.is_cancelled(),
            "detached rollback did not complete without a Tokio time driver"
        );
        assert_eq!(*state.events.lock().unwrap(), reverse_lifecycle_events());
        state.assert_counts([1; 4], [1; 4], [1; 4]);
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explicit_build_cancellation_waits_for_complete_reverse_rollback() {
    let state = FixtureState::new(PauseAt::Leaf, 4);
    let mut build =
        lily_injection::__private::begin_application_container_build(builder(Arc::clone(&state)));

    tokio::select! {
        biased;
        outcome = build.wait() => panic!("build unexpectedly completed: {outcome:?}"),
        () = state.entered.cancelled() => {}
    }
    build.cancel();
    let outcome = build.wait().await;
    assert!(matches!(
        outcome,
        lily_injection::__private::ApplicationContainerBuildOutcome::Cancelled(Ok(()))
    ));

    assert!(state.rollback_tail_disposed.is_cancelled());
    wait_for(&state.all_services_dropped).await;
    assert_eq!(*state.events.lock().unwrap(), reverse_lifecycle_events());
    state.assert_counts([1; 4], [1; 4], [1; 4]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelling_pending_seeded_singleton_consumes_and_disposes_it_once() {
    let state = FixtureState::new(PauseAt::Control, 1);
    let mut build =
        lily_injection::__private::begin_application_container_build(builder(Arc::clone(&state)));

    tokio::select! {
        biased;
        outcome = build.wait() => panic!("build unexpectedly completed: {outcome:?}"),
        () = state.entered.cancelled() => {}
    }
    build.cancel();
    assert!(matches!(
        build.wait().await,
        lily_injection::__private::ApplicationContainerBuildOutcome::Cancelled(Ok(()))
    ));

    wait_for(&state.all_services_dropped).await;
    assert_eq!(
        *state.events.lock().unwrap(),
        vec![LifecycleEvent::ControlInit, LifecycleEvent::ControlDispose]
    );
    state.assert_counts([1, 0, 0, 0], [1, 0, 0, 0], [1, 0, 0, 0]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancellation_rollback_failure_does_not_skip_remaining_disposers() {
    let state = FixtureState::with_second_dispose_failure();
    let mut build =
        lily_injection::__private::begin_application_container_build(builder(Arc::clone(&state)));

    tokio::select! {
        biased;
        outcome = build.wait() => panic!("build unexpectedly completed: {outcome:?}"),
        () = state.entered.cancelled() => {}
    }
    build.cancel();
    let outcome = build.wait().await;
    let lily_injection::__private::ApplicationContainerBuildOutcome::Cancelled(Err(
        InjectionError::ShutdownFailed { errors, .. },
    )) = outcome
    else {
        panic!("expected a typed cancellation rollback failure: {outcome:?}")
    };
    assert!(
        errors
            .iter()
            .any(|error| error.contains("intentional cancellation rollback failure"))
    );

    wait_for(&state.all_services_dropped).await;
    assert_eq!(*state.events.lock().unwrap(), reverse_lifecycle_events());
    state.assert_counts([1; 4], [1; 4], [1; 4]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn build_completion_and_waiter_abort_never_leak_or_double_dispose() {
    for _ in 0..32 {
        let state = FixtureState::new(PauseAt::Leaf, 4);
        let build = tokio::spawn(builder(Arc::clone(&state)).build());
        wait_for(&state.entered).await;

        let barrier = Arc::new(Barrier::new(3));
        let release_barrier = Arc::clone(&barrier);
        let release = state.release.clone();
        let release_task = tokio::spawn(async move {
            release_barrier.wait();
            release.cancel();
        });
        let abort_barrier = Arc::clone(&barrier);
        let abort = build.abort_handle();
        let abort_task = tokio::spawn(async move {
            abort_barrier.wait();
            abort.abort();
        });
        barrier.wait();
        release_task.await.unwrap();
        abort_task.await.unwrap();

        match build.await {
            Ok(Ok(container)) => {
                container.close().await.unwrap();
                drop(container);
            }
            Err(error) if error.is_cancelled() => {}
            result => panic!("unexpected build race result: {result:?}"),
        }

        wait_for(&state.all_services_dropped).await;
        assert_eq!(*state.events.lock().unwrap(), reverse_lifecycle_events());
        state.assert_counts([1; 4], [1; 4], [1; 4]);
    }
}

#[test]
fn successful_build_preserves_caller_context_and_disarms_rollback() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let subscriber = tracing_subscriber::registry();
    tracing::subscriber::with_default(subscriber, || {
        runtime.block_on(async {
            let state = FixtureState::new(PauseAt::Never, 4);
            let span = tracing::info_span!("di_build_parent");
            let container = ProcessContext::scope(
                ProcessContext::with_process_id(4242),
                TEST_TASK_VALUE.scope(
                    "caller-task-local",
                    builder(Arc::clone(&state)).build().instrument(span),
                ),
            )
            .await
            .unwrap();

            state.assert_counts([1; 4], [0; 4], [0; 4]);
            let observations = state.observations.lock().unwrap().clone();
            assert_eq!(observations.len(), 4);
            assert!(observations.iter().all(|observation| {
                observation.process_id == Some(4242)
                    && observation.span.as_deref() == Some("di_build_parent")
                    && observation.task_value == Some("caller-task-local")
            }));

            container.close().await.unwrap();
            drop(container);
            wait_for(&state.all_services_dropped).await;
            assert_eq!(*state.events.lock().unwrap(), reverse_lifecycle_events());
            state.assert_counts([1; 4], [1; 4], [1; 4]);
        });
    });
}
