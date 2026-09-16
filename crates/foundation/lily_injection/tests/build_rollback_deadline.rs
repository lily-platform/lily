use lily_error::injection::{InjectionError, ShutdownOutcomeStatus};
use lily_injectable_derive::Injectable;
use lily_injection::{ApplicationContainer, ServiceTrait, async_trait::async_trait};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio_util::sync::CancellationToken;

const CONTROL: usize = 0;
const READY: usize = 1;
const PENDING: usize = 2;

struct State {
    initialize_entered: CancellationToken,
    disposer_entered: CancellationToken,
    disposer_dropped: CancellationToken,
    all_dropped: CancellationToken,
    dispose_calls: [AtomicUsize; 3],
    drop_calls: [AtomicUsize; 3],
}

impl State {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            initialize_entered: CancellationToken::new(),
            disposer_entered: CancellationToken::new(),
            disposer_dropped: CancellationToken::new(),
            all_dropped: CancellationToken::new(),
            dispose_calls: std::array::from_fn(|_| AtomicUsize::new(0)),
            drop_calls: std::array::from_fn(|_| AtomicUsize::new(0)),
        })
    }

    fn dispose_called(&self, index: usize) {
        self.dispose_calls[index].fetch_add(1, Ordering::SeqCst);
    }

    fn dropped(&self, index: usize) {
        self.drop_calls[index].fetch_add(1, Ordering::SeqCst);
        if self
            .drop_calls
            .iter()
            .map(|calls| calls.load(Ordering::SeqCst))
            .sum::<usize>()
            == 3
        {
            self.all_dropped.cancel();
        }
    }
}

struct DisposerDropGuard(CancellationToken);

impl Drop for DisposerDropGuard {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

#[derive(Injectable)]
#[service(lifetime = "Singleton")]
struct DeadlineControl {
    state: Arc<State>,
}

impl Default for DeadlineControl {
    fn default() -> Self {
        panic!("DeadlineControl must be supplied through seed_singleton")
    }
}

#[async_trait]
impl ServiceTrait for DeadlineControl {
    async fn dispose(&self) -> Result<(), InjectionError> {
        self.state.dispose_called(CONTROL);
        Ok(())
    }
}

impl Drop for DeadlineControl {
    fn drop(&mut self) {
        self.state.dropped(CONTROL);
    }
}

#[derive(Injectable)]
#[service(lifetime = "Singleton")]
struct DeadlineReady {
    #[inject]
    control: Arc<DeadlineControl>,
}

#[async_trait]
impl ServiceTrait for DeadlineReady {
    async fn dispose(&self) -> Result<(), InjectionError> {
        self.control.state.dispose_called(READY);
        Ok(())
    }
}

impl Drop for DeadlineReady {
    fn drop(&mut self) {
        self.control.state.dropped(READY);
    }
}

#[derive(Injectable)]
#[service(lifetime = "Singleton")]
struct DeadlinePending {
    #[inject]
    ready: Arc<DeadlineReady>,
}

#[async_trait]
impl ServiceTrait for DeadlinePending {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        self.ready.control.state.initialize_entered.cancel();
        std::future::pending().await
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        let state = &self.ready.control.state;
        state.dispose_called(PENDING);
        state.disposer_entered.cancel();
        let _drop_guard = DisposerDropGuard(state.disposer_dropped.clone());
        std::future::pending().await
    }
}

impl Drop for DeadlinePending {
    fn drop(&mut self) {
        self.ready.control.state.dropped(PENDING);
    }
}

#[tokio::test(start_paused = true)]
async fn adapter_rollback_reserve_uses_the_original_failure_timestamp() {
    let state = State::new();
    let mut build = lily_injection::__private::begin_application_container_build(
        ApplicationContainer::builder().seed_singleton(DeadlineControl {
            state: state.clone(),
        }),
    );
    build.reserve_rollback_tail(5);
    let total = build.rollback_timeout();
    tokio::select! {
        biased;
        result = build.wait() => panic!("unexpected result: {result:?}"),
        () = state.initialize_entered.cancelled() => {}
    }
    // Normal startup time cannot consume or start the rollback budget.
    tokio::time::advance(std::time::Duration::from_secs(3)).await;
    assert!(build.rollback_started_at().is_none());
    let started = tokio::time::Instant::now();
    build.cancel();
    assert_eq!(build.rollback_started_at(), Some(started));
    assert!(!build.rollback_quiescent());
    let outcome = build.wait().await;
    assert!(matches!(
        outcome,
        lily_injection::__private::ApplicationContainerBuildOutcome::Cancelled(Err(_))
    ));
    assert_eq!(
        tokio::time::Instant::now(),
        started + total - (total / 100) * 5
    );
    assert!(build.rollback_quiescent());
    build.cancel();
    assert_eq!(build.rollback_started_at(), Some(started));
}

#[tokio::test(start_paused = true)]
async fn build_rollback_uses_one_hard_deadline_and_cancels_unstarted_tail() {
    let state = State::new();
    let mut build = lily_injection::__private::begin_application_container_build(
        ApplicationContainer::builder().seed_singleton(DeadlineControl {
            state: Arc::clone(&state),
        }),
    );
    let rollback_timeout = build.rollback_timeout();

    tokio::select! {
        biased;
        outcome = build.wait() => panic!("build unexpectedly completed: {outcome:?}"),
        () = state.initialize_entered.cancelled() => {}
    }
    build.cancel();
    state.disposer_entered.cancelled().await;

    let mut terminal = Box::pin(build.wait());
    tokio::time::advance(rollback_timeout - std::time::Duration::from_millis(1)).await;
    tokio::select! {
        biased;
        outcome = &mut terminal => panic!("rollback finished before its deadline: {outcome:?}"),
        () = tokio::task::yield_now() => {}
    }

    tokio::time::advance(std::time::Duration::from_millis(1)).await;
    let outcome = terminal.await;
    let lily_injection::__private::ApplicationContainerBuildOutcome::Cancelled(Err(
        InjectionError::ShutdownFailed {
            outcomes,
            remaining,
            ..
        },
    )) = outcome
    else {
        panic!("expected a typed deadline failure: {outcome:?}")
    };

    assert!(outcomes.iter().any(|outcome| {
        outcome.component.contains("DeadlinePending")
            && outcome.status == ShutdownOutcomeStatus::TimedOut
    }));
    for type_name in ["DeadlineReady", "DeadlineControl"] {
        assert!(outcomes.iter().any(|outcome| {
            outcome.component.contains(type_name)
                && outcome.status == ShutdownOutcomeStatus::Cancelled
        }));
    }
    assert!(remaining.is_some());
    assert!(state.disposer_dropped.is_cancelled());
    assert_eq!(
        std::array::from_fn::<_, 3, _>(|index| {
            state.dispose_calls[index].load(Ordering::SeqCst)
        }),
        [0, 0, 1],
        "the expired aggregate budget must not start older disposers"
    );

    state.all_dropped.cancelled().await;
    assert_eq!(
        std::array::from_fn::<_, 3, _>(|index| { state.drop_calls[index].load(Ordering::SeqCst) }),
        [1, 1, 1]
    );
}

#[test]
fn pending_rollback_without_a_time_driver_is_contained_as_typed_evidence() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    runtime.block_on(async {
        let state = State::new();
        let mut build = lily_injection::__private::begin_application_container_build(
            ApplicationContainer::builder().seed_singleton(DeadlineControl {
                state: Arc::clone(&state),
            }),
        );

        tokio::select! {
            biased;
            outcome = build.wait() => panic!("build unexpectedly completed: {outcome:?}"),
            () = state.initialize_entered.cancelled() => {}
        }
        build.cancel();
        let outcome = build.wait().await;
        let lily_injection::__private::ApplicationContainerBuildOutcome::Cancelled(Err(
            InjectionError::ShutdownFailed { outcomes, .. },
        )) = outcome
        else {
            panic!("missing timer support must remain a typed rollback failure: {outcome:?}")
        };

        assert!(outcomes.iter().any(|outcome| {
            outcome.component.contains("DeadlinePending")
                && outcome.status == ShutdownOutcomeStatus::Panicked
                && outcome
                    .detail
                    .as_deref()
                    .is_some_and(|detail| detail.contains("deadline-controlled disposal panicked"))
        }));
        assert_eq!(
            std::array::from_fn::<_, 3, _>(|index| {
                state.dispose_calls[index].load(Ordering::SeqCst)
            }),
            [1, 1, 1],
            "synchronous tail cleanup must remain available without a time driver"
        );
        state.all_dropped.cancelled().await;
    });
}
