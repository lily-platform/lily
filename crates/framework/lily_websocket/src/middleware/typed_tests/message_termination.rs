use super::*;
use crate::lifecycle::LifecycleInvocationState;

#[derive(Clone, Copy)]
enum Behavior {
    Ready,
    Pending,
    Panic,
    DropPanic,
    Error,
}

#[derive(Default)]
struct State {
    events: StdMutex<Vec<(usize, &'static str)>>,
    views: StdMutex<
        Vec<(
            usize,
            CleanupCancellation,
            CleanupCancellation,
            WsMessageNormalExit,
        )>,
    >,
}

struct Probe {
    index: usize,
    before: Behavior,
    after: Behavior,
    termination: Behavior,
    state: Arc<State>,
}

struct DropObservation {
    state: Arc<State>,
    index: usize,
    phase: &'static str,
    signal: Option<CleanupCancellation>,
    panics: bool,
}
impl Drop for DropObservation {
    fn drop(&mut self) {
        if let Some(signal) = &self.signal {
            assert!(
                signal.is_cancelled(),
                "authority expires before future destruction"
            );
        }
        self.state
            .events
            .lock()
            .unwrap()
            .push((self.index, self.phase));
        assert!(!self.panics, "intentional destructor panic");
    }
}

async fn behavior(value: Behavior) -> Result<(), WsMiddlewareError> {
    match value {
        Behavior::Ready => Ok(()),
        Behavior::Pending | Behavior::DropPanic => pending().await,
        Behavior::Panic => panic!("intentional poll panic"),
        Behavior::Error => Err(WsMiddlewareError::timeout()),
    }
}

#[async_trait]
impl WsMessageMiddleware for Probe {
    async fn new(_: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
        unreachable!()
    }
    fn descriptor(&self) -> MiddlewareDescriptor {
        MiddlewareDescriptor::new(
            ["outer", "middle", "inner"][self.index],
            MiddlewareKind::WebSocketMessage,
        )
    }
    async fn before_message(
        &self,
        _: &mut WsMessageExchange,
        _: ExecutionCancellation,
    ) -> Result<WsMessageDecision, WsMiddlewareError> {
        behavior(self.before).await?;
        Ok(WsMessageDecision::Continue)
    }
    async fn after_message(
        &self,
        _: &mut WsMessageExchange,
        _: WsMessageOutcome,
        _: ExecutionCancellation,
    ) -> Result<WsMessageDecision, WsMiddlewareError> {
        self.state
            .events
            .lock()
            .unwrap()
            .push((self.index, "after"));
        let _drop = DropObservation {
            state: self.state.clone(),
            index: self.index,
            phase: "after.drop",
            signal: None,
            panics: matches!(self.after, Behavior::DropPanic),
        };
        behavior(self.after).await?;
        Ok(WsMessageDecision::Continue)
    }
    async fn on_message_termination(
        &self,
        mut context: WsMessageTerminationContext<'_>,
        signal: CleanupCancellation,
    ) -> Result<(), WsMiddlewareError> {
        assert!(!signal.is_cancelled());
        assert!(!context.cancellation().is_cancelled());
        assert!(context.deadline() > tokio::time::Instant::now());
        assert_eq!(context.outcome(), WsMessageOutcome::Handled);
        context.insert_message_local(self.index).unwrap();
        assert_eq!(context.remove_message_local::<usize>(), Some(self.index));
        self.state.views.lock().unwrap().push((
            self.index,
            signal.clone(),
            context.cancellation().clone(),
            context.normal_exit(),
        ));
        self.state
            .events
            .lock()
            .unwrap()
            .push((self.index, "termination"));
        // A retained clone observes invocation end; pending future destructors
        // additionally prove the signal is sent before timeout/drop.
        if matches!(self.termination, Behavior::Pending | Behavior::DropPanic) {
            let _drop = DropObservation {
                state: self.state.clone(),
                index: self.index,
                phase: "termination.drop",
                signal: Some(signal),
                panics: matches!(self.termination, Behavior::DropPanic),
            };
            behavior(self.termination).await
        } else {
            behavior(self.termination).await
        }
    }
}

fn chain(state: &Arc<State>, entries: &[(Behavior, Behavior)]) -> CompiledWsMessageChain {
    CompiledWsMessageChain::compile(
        entries
            .iter()
            .enumerate()
            .map(|(index, (after, termination))| {
                Arc::new(Probe {
                    index,
                    before: Behavior::Ready,
                    after: *after,
                    termination: *termination,
                    state: state.clone(),
                }) as Arc<dyn WsMessageMiddleware>
            })
            .collect(),
    )
    .unwrap()
}

const CAP: Duration = Duration::from_millis(100);

fn capped_exchange(source: CancellationToken) -> WsMessageExchange {
    let mut exchange = message_exchange(source);
    exchange.deadline = tokio::time::Instant::now() + CAP;
    exchange
}

#[tokio::test(start_paused = true)]
async fn cooperative_normal_completion_does_not_arm_termination() {
    let state = Arc::new(State::default());
    let chain = chain(&state, &[(Behavior::Ready, Behavior::Panic); 3]);
    let source = CancellationToken::new();
    let mut exchange = capped_exchange(source.clone());
    let (mut ledger, result) = chain.before(&mut exchange).await;
    result.unwrap();
    source.cancel();
    let report = chain
        .after(&mut exchange, &mut ledger, WsMessageOutcome::Handled)
        .await;
    assert_eq!(report.failed(), 0);
    chain
        .terminate(
            &mut exchange,
            &mut ledger,
            WsMessageOutcome::Handled,
            CAP,
            &CancellationToken::new(),
        )
        .await;
    assert!(state.views.lock().unwrap().is_empty());
    assert_eq!(
        *state.events.lock().unwrap(),
        [
            (2, "after"),
            (2, "after.drop"),
            (1, "after"),
            (1, "after.drop"),
            (0, "after"),
            (0, "after.drop")
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn interrupted_normal_exit_terminates_before_outer_and_is_not_retried() {
    let state = Arc::new(State::default());
    let chain = chain(
        &state,
        &[
            (Behavior::Ready, Behavior::Ready),
            (Behavior::Pending, Behavior::Ready),
            (Behavior::Ready, Behavior::Panic),
        ],
    );
    let mut exchange = capped_exchange(CancellationToken::new());
    let (mut ledger, _) = chain.before(&mut exchange).await;
    chain
        .after(&mut exchange, &mut ledger, WsMessageOutcome::Handled)
        .await;
    for _ in 0..2 {
        chain
            .terminate(
                &mut exchange,
                &mut ledger,
                WsMessageOutcome::Handled,
                CAP,
                &CancellationToken::new(),
            )
            .await;
    }
    assert_eq!(
        *state.events.lock().unwrap(),
        [
            (2, "after"),
            (2, "after.drop"),
            (1, "after"),
            (1, "after.drop"),
            (1, "termination"),
            (0, "termination")
        ]
    );
    let views = state.views.lock().unwrap();
    assert_eq!(
        views[0].3,
        WsMessageNormalExit::Interrupted {
            reason: WsMessageTerminationReason::TimedOut,
            started: true
        }
    );
    assert_eq!(views[1].3, WsMessageNormalExit::NotStarted);
    assert_eq!(
        ledger.report(WsMessageOutcome::Handled).failed(),
        1,
        "successful termination does not erase interrupted normal work"
    );
}

#[tokio::test(start_paused = true)]
async fn pending_termination_has_a_local_share_and_does_not_cancel_its_sibling() {
    let state = Arc::new(State::default());
    let chain = chain(
        &state,
        &[
            (Behavior::Ready, Behavior::Ready),
            (Behavior::Ready, Behavior::Pending),
        ],
    );
    let execution = CancellationToken::new();
    let mut exchange = capped_exchange(execution.clone());
    let (mut ledger, _) = chain.before(&mut exchange).await;
    execution.cancel();
    chain.execution_stopped(&mut ledger, WsMessageTerminationReason::Aborted);
    let authority = CancellationToken::new();
    let start = tokio::time::Instant::now();
    chain
        .terminate(
            &mut exchange,
            &mut ledger,
            WsMessageOutcome::Handled,
            CAP,
            &authority,
        )
        .await;
    assert!(start.elapsed() < CAP);
    assert!(!authority.is_cancelled());
    let views = state.views.lock().unwrap();
    assert_eq!(views.len(), 2);
    for (_, parameter, context, _) in views.iter() {
        assert!(parameter.is_cancelled() && context.is_cancelled());
    }
    assert_eq!(
        *state.events.lock().unwrap(),
        [
            (1, "termination"),
            (1, "termination.drop"),
            (0, "termination")
        ]
    );
    assert_eq!(ledger.report(WsMessageOutcome::Handled).failed(), 1);
    let counts = ledger.accounting();
    assert!(counts.reconciles());
    assert_eq!(counts.entered, 2);
    assert_eq!(counts.exits_completed, 1);
    assert_eq!(counts.termination.timed_out, 1);
    assert_eq!(counts.termination.started_incomplete, 1);
    assert_eq!(counts.termination.not_started, 0);
}

#[tokio::test(start_paused = true)]
async fn expired_root_records_not_started_without_polling_user_cleanup() {
    let state = Arc::new(State::default());
    let chain = chain(&state, &[(Behavior::Ready, Behavior::Panic); 3]);
    let mut exchange = capped_exchange(CancellationToken::new());
    let (mut ledger, _) = chain.before(&mut exchange).await;
    chain.execution_stopped(&mut ledger, WsMessageTerminationReason::Aborted);
    exchange
        .connection()
        .shutdown_budget()
        .force_before(tokio::time::Instant::now());
    chain
        .terminate(
            &mut exchange,
            &mut ledger,
            WsMessageOutcome::Handled,
            CAP,
            &CancellationToken::new(),
        )
        .await;
    assert!(state.events.lock().unwrap().is_empty());
    assert_eq!(ledger.report(WsMessageOutcome::Handled).failed(), 3);
    let counts = ledger.accounting();
    assert!(counts.reconciles());
    assert_eq!(counts.termination.timed_out, 3);
    assert_eq!(counts.termination.not_started, 3);
    assert_eq!(counts.termination.started_incomplete, 0);
    assert_eq!(counts.exits_not_started, 3);
    for entry in &ledger.lifecycle.entries {
        assert_eq!(
            entry.termination.state,
            LifecycleInvocationState::Terminal {
                outcome: LifecycleOutcome::Interrupted(LifecycleInterruption::TimedOut),
                started: false
            }
        );
        assert!(entry.termination.cancellation_requested);
    }
}

#[tokio::test(start_paused = true)]
async fn hook_panic_destructor_panic_and_returned_error_do_not_skip_outer_cleanup() {
    for inner in [Behavior::Panic, Behavior::DropPanic, Behavior::Error] {
        let state = Arc::new(State::default());
        let chain = chain(
            &state,
            &[(Behavior::Ready, Behavior::Ready), (Behavior::Ready, inner)],
        );
        let mut exchange = capped_exchange(CancellationToken::new());
        let (mut ledger, _) = chain.before(&mut exchange).await;
        chain.execution_stopped(&mut ledger, WsMessageTerminationReason::Aborted);
        chain
            .terminate(
                &mut exchange,
                &mut ledger,
                WsMessageOutcome::Handled,
                CAP,
                &CancellationToken::new(),
            )
            .await;
        assert!(state.events.lock().unwrap().contains(&(0, "termination")));
        let expected = if matches!(inner, Behavior::Error) {
            LifecycleOutcome::Failed
        } else {
            LifecycleOutcome::Panicked
        };
        assert_eq!(
            ledger.lifecycle.entries[1].termination.state,
            LifecycleInvocationState::Terminal {
                outcome: expected,
                started: true
            }
        );
        assert_eq!(ledger.report(WsMessageOutcome::Handled).failed(), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn shortened_root_stops_pending_hook_and_leaves_outer_not_started() {
    let state = Arc::new(State::default());
    let chain = chain(
        &state,
        &[
            (Behavior::Ready, Behavior::Ready),
            (Behavior::Ready, Behavior::Pending),
        ],
    );
    let mut exchange = capped_exchange(CancellationToken::new());
    let budget = exchange.connection().shutdown_budget().clone();
    let (mut ledger, _) = chain.before(&mut exchange).await;
    chain.execution_stopped(&mut ledger, WsMessageTerminationReason::Aborted);
    let authority = CancellationToken::new();
    {
        let unwind = chain.terminate(
            &mut exchange,
            &mut ledger,
            WsMessageOutcome::Handled,
            CAP,
            &authority,
        );
        tokio::pin!(unwind);
        assert!(futures_util::poll!(unwind.as_mut()).is_pending());
        budget.force_before(tokio::time::Instant::now());
        unwind.await;
    }
    assert_eq!(
        *state.events.lock().unwrap(),
        [(1, "termination"), (1, "termination.drop")]
    );
    assert_eq!(ledger.report(WsMessageOutcome::Handled).failed(), 2);
}

#[tokio::test(start_paused = true)]
async fn framework_before_timeout_only_terminates_the_successfully_entered_prefix() {
    let state = Arc::new(State::default());
    let chain = CompiledWsMessageChain::compile(
        (0..3)
            .map(|index| {
                Arc::new(Probe {
                    index,
                    before: if index == 1 {
                        Behavior::Pending
                    } else {
                        Behavior::Ready
                    },
                    after: Behavior::Panic,
                    termination: Behavior::Ready,
                    state: state.clone(),
                }) as Arc<dyn WsMessageMiddleware>
            })
            .collect(),
    )
    .unwrap();
    let mut exchange = capped_exchange(CancellationToken::new());
    let (mut ledger, result) = chain.before(&mut exchange).await;
    assert!(result.is_err());
    chain
        .after(&mut exchange, &mut ledger, WsMessageOutcome::Handled)
        .await;
    chain
        .terminate(
            &mut exchange,
            &mut ledger,
            WsMessageOutcome::Handled,
            CAP,
            &CancellationToken::new(),
        )
        .await;
    assert_eq!(*state.events.lock().unwrap(), [(0, "termination")]);
    assert_eq!(ledger.entered(), 1);
}
