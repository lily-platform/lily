use lily_error::injection::InjectionError;
use lily_injectable_derive::Injectable;
use lily_injection::{
    ApplicationContainer, ServiceTrait, ShutdownOutcomeStatus, async_trait::async_trait,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Event {
    ControlInit,
    ReadyInit,
    FailureInit,
    FailureDispose,
    ReadyDispose,
    ControlDisposeEnter,
    ControlDisposeExit,
}

struct State {
    events: Mutex<Vec<Event>>,
    control_dispose_entered: CancellationToken,
    control_dispose_dropped: CancellationToken,
    release_control_dispose: CancellationToken,
    all_dropped: CancellationToken,
    drops: AtomicUsize,
}

impl State {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            events: Mutex::new(Vec::new()),
            control_dispose_entered: CancellationToken::new(),
            control_dispose_dropped: CancellationToken::new(),
            release_control_dispose: CancellationToken::new(),
            all_dropped: CancellationToken::new(),
            drops: AtomicUsize::new(0),
        })
    }

    fn push(&self, event: Event) {
        self.events.lock().unwrap().push(event);
    }

    fn dropped(&self) {
        if self.drops.fetch_add(1, Ordering::SeqCst) == 2 {
            self.all_dropped.cancel();
        }
    }
}

struct ControlDisposerDropGuard(CancellationToken);

impl Drop for ControlDisposerDropGuard {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

#[derive(Injectable)]
#[service(lifetime = "Singleton")]
struct RollbackControl {
    state: Arc<State>,
}

impl Default for RollbackControl {
    fn default() -> Self {
        panic!("RollbackControl must be supplied through seed_singleton")
    }
}

#[async_trait]
impl ServiceTrait for RollbackControl {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        self.state.push(Event::ControlInit);
        Ok(())
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        self.state.push(Event::ControlDisposeEnter);
        self.state.control_dispose_entered.cancel();
        let _drop_guard = ControlDisposerDropGuard(self.state.control_dispose_dropped.clone());
        self.state.release_control_dispose.cancelled().await;
        self.state.push(Event::ControlDisposeExit);
        Ok(())
    }
}

impl Drop for RollbackControl {
    fn drop(&mut self) {
        self.state.dropped();
    }
}

#[derive(Injectable)]
#[service(lifetime = "Singleton")]
struct RollbackReady {
    #[inject]
    control: Arc<RollbackControl>,
}

#[async_trait]
impl ServiceTrait for RollbackReady {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        self.control.state.push(Event::ReadyInit);
        Ok(())
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        self.control.state.push(Event::ReadyDispose);
        Ok(())
    }
}

impl Drop for RollbackReady {
    fn drop(&mut self) {
        self.control.state.dropped();
    }
}

#[derive(Injectable)]
#[service(lifetime = "Singleton")]
struct StartupFailure {
    #[inject]
    ready: Arc<RollbackReady>,
}

#[async_trait]
impl ServiceTrait for StartupFailure {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        self.ready.control.state.push(Event::FailureInit);
        Err(InjectionError::InitError(
            "intentional startup failure".to_string(),
        ))
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        self.ready.control.state.push(Event::FailureDispose);
        Ok(())
    }
}

impl Drop for StartupFailure {
    fn drop(&mut self) {
        // The failed instance is cleaned and dropped directly by its generated
        // factory before provider rollback begins.
        self.ready.control.state.dropped();
    }
}

async fn wait_for(token: &CancellationToken) {
    tokio::time::timeout(Duration::from_secs(2), token.cancelled())
        .await
        .expect("lifecycle signal timed out");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn aborting_startup_error_waiter_does_not_cancel_owned_rollback() {
    let state = State::new();
    let build = tokio::spawn(
        ApplicationContainer::builder()
            .seed_singleton(RollbackControl {
                state: Arc::clone(&state),
            })
            .build(),
    );

    wait_for(&state.control_dispose_entered).await;
    build.abort();
    assert!(build.await.unwrap_err().is_cancelled());

    state.release_control_dispose.cancel();
    wait_for(&state.all_dropped).await;
    assert_eq!(state.drops.load(Ordering::SeqCst), 3);
    assert_eq!(
        *state.events.lock().unwrap(),
        vec![
            Event::ControlInit,
            Event::ReadyInit,
            Event::FailureInit,
            Event::FailureDispose,
            Event::ReadyDispose,
            Event::ControlDisposeEnter,
            Event::ControlDisposeExit,
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn startup_failure_keeps_typed_primary_and_bounded_rollback_evidence() {
    let state = State::new();
    let rollback_timeout = lily_injection::__private::configured_build_rollback_timeout()
        .expect("valid build rollback timeout environment");
    let build = tokio::spawn(
        ApplicationContainer::builder()
            .seed_singleton(RollbackControl {
                state: Arc::clone(&state),
            })
            .build(),
    );

    state.control_dispose_entered.cancelled().await;
    tokio::time::advance(rollback_timeout).await;
    let error = build
        .await
        .expect("build task must not panic")
        .expect_err("startup must remain failed");
    let InjectionError::StartupRollbackFailed {
        startup,
        rollback_errors,
        rollback_outcomes,
        rollback_remaining,
    } = error
    else {
        panic!("expected typed startup and rollback evidence")
    };

    assert!(matches!(
        *startup,
        InjectionError::ServiceInitializationFailed { .. }
    ));
    assert!(
        rollback_errors
            .iter()
            .any(|detail| detail.contains("disposal timed out"))
    );
    assert!(rollback_outcomes.iter().any(|outcome| {
        outcome.component.contains("RollbackControl")
            && outcome.status == ShutdownOutcomeStatus::TimedOut
    }));
    assert!(rollback_remaining.is_some());
    assert!(state.control_dispose_dropped.is_cancelled());
    state.all_dropped.cancelled().await;
    assert!(
        !state
            .events
            .lock()
            .unwrap()
            .contains(&Event::ControlDisposeExit)
    );
}

#[tokio::test(start_paused = true)]
async fn aborting_the_waiter_does_not_detach_rollback_past_its_deadline() {
    let state = State::new();
    let rollback_timeout = lily_injection::__private::configured_build_rollback_timeout()
        .expect("valid build rollback timeout environment");
    let build = tokio::spawn(
        ApplicationContainer::builder()
            .seed_singleton(RollbackControl {
                state: Arc::clone(&state),
            })
            .build(),
    );

    state.control_dispose_entered.cancelled().await;
    build.abort();
    assert!(build.await.unwrap_err().is_cancelled());

    tokio::time::advance(rollback_timeout).await;
    state.control_dispose_dropped.cancelled().await;
    state.all_dropped.cancelled().await;
    assert_eq!(state.drops.load(Ordering::SeqCst), 3);
    assert!(
        !state
            .events
            .lock()
            .unwrap()
            .contains(&Event::ControlDisposeExit)
    );
}
