use super::*;
use crate::CleanupCancellation;
use crate::extractor::{FromWebSocketMessageParts, MessageDeadline};
use crate::middleware::WsMessageTerminationContext;

static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
static DELAYS: StdMutex<[u64; 8]> = StdMutex::new([0; 8]);
static EVENTS: StdMutex<Vec<(&str, Instant)>> = StdMutex::new(Vec::new());
static TERMINATION_DELAY: AtomicUsize = AtomicUsize::new(0);
static COOPERATE_AT: AtomicUsize = AtomicUsize::new(usize::MAX);
static ACTION_ERROR: AtomicBool = AtomicBool::new(false);

const LABELS: [&str; 8] = [
    "outer.before",
    "inner.before",
    "first.guard",
    "second.guard",
    "extractor",
    "action",
    "inner.after",
    "outer.after",
];

async fn stage(index: usize, deadline: Instant, signal: &crate::ExecutionCancellation) {
    EVENTS.lock().unwrap().push((LABELS[index], deadline));
    if COOPERATE_AT.load(Ordering::SeqCst) == index {
        signal.cancelled().await;
    }
    let delay = DELAYS.lock().unwrap()[index];
    if delay > 0 {
        tokio::time::sleep(Duration::from_millis(delay)).await;
    }
}

struct Middleware<const I: usize>;

#[async_trait]
impl<const I: usize> WsMessageMiddleware for Middleware<I> {
    async fn new(_: Arc<Extensions>) -> Result<Self, crate::middleware::WsMiddlewareInitError> {
        Ok(Self)
    }

    fn descriptor(&self) -> MiddlewareDescriptor {
        MiddlewareDescriptor::new(LABELS[I], MiddlewareKind::WebSocketMessage)
    }

    async fn before_message(
        &self,
        exchange: &mut WsMessageExchange,
        signal: crate::ExecutionCancellation,
    ) -> Result<WsMessageDecision, crate::middleware::WsMiddlewareError> {
        stage(I, exchange.deadline(), &signal).await;
        Ok(WsMessageDecision::Continue)
    }

    async fn after_message(
        &self,
        exchange: &mut WsMessageExchange,
        _: WsMessageOutcome,
        signal: crate::ExecutionCancellation,
    ) -> Result<WsMessageDecision, crate::middleware::WsMiddlewareError> {
        stage(7 - I, exchange.deadline(), &signal).await;
        Ok(WsMessageDecision::Continue)
    }

    async fn on_message_termination(
        &self,
        context: WsMessageTerminationContext<'_>,
        signal: CleanupCancellation,
    ) -> Result<(), crate::middleware::WsMiddlewareError> {
        assert!(!signal.is_cancelled());
        assert!(context.deadline() > Instant::now());
        EVENTS.lock().unwrap().push((
            ["outer.termination", "inner.termination"][I],
            context.deadline(),
        ));
        let delay = TERMINATION_DELAY.load(Ordering::SeqCst);
        if delay > 0 {
            tokio::time::sleep(Duration::from_millis(delay as u64)).await;
        }
        Ok(())
    }
}

struct Guard<const I: usize>;

#[async_trait]
impl<const I: usize> WsGuard for Guard<I> {
    async fn new(_: Arc<Extensions>) -> Result<Self, GuardInitializationError> {
        Ok(Self)
    }

    async fn can_activate(
        &self,
        exchange: &mut WsMessageExchange,
        signal: crate::ExecutionCancellation,
    ) -> Result<(), WebSocketGuardRejection> {
        stage(2 + I, exchange.deadline(), &signal).await;
        Ok(())
    }
}

struct Action;

impl WebSocketMessageAction for Action {
    fn call(&self, mut invocation: WebSocketMessageInvocation) -> WebSocketActionFuture {
        Box::pin(async move {
            let deadline = MessageDeadline::from_message_parts(&mut invocation)
                .await
                .unwrap();
            assert_eq!(deadline.instant(), invocation.deadline());
            let signal = crate::ExecutionCancellation::from_message_parts(&mut invocation)
                .await
                .unwrap();
            stage(4, deadline.instant(), &signal).await;
            stage(5, deadline.instant(), &signal).await;
            if ACTION_ERROR.load(Ordering::SeqCst) {
                return Err(WebSocketActionError::rejected(
                    WebSocketErrorCode::new("USER_RESULT").unwrap(),
                    "The application result.",
                )
                .unwrap());
            }
            Ok(PendingWebSocketActionOutcome::NoReply)
        })
    }
}

fn bind(
    controller: ErasedWebSocketController,
) -> Result<BoundWebSocketOperation, WebSocketControllerBindingError> {
    let _ = downcast_websocket_controller::<AppTestController>(controller)?;
    Ok(BoundWebSocketOperation::Message(Arc::new(Action)))
}

struct ResultEvidence {
    started: Instant,
    elapsed: Duration,
    dispatch: ScopedWebSocketDispatch,
    accounting: ownership::MessageAccounting,
}

async fn execute(
    delays: [u64; 8],
    override_timeout: Option<Duration>,
    app_timeout: Duration,
) -> ResultEvidence {
    execute_case(delays, override_timeout, app_timeout, None, None, false).await
}

async fn execute_case(
    delays: [u64; 8],
    override_timeout: Option<Duration>,
    app_timeout: Duration,
    force_after: Option<Duration>,
    cooperate_at: Option<usize>,
    action_error: bool,
) -> ResultEvidence {
    COOPERATE_AT.store(cooperate_at.unwrap_or(usize::MAX), Ordering::SeqCst);
    ACTION_ERROR.store(action_error, Ordering::SeqCst);
    *DELAYS.lock().unwrap() = delays;
    EVENTS.lock().unwrap().clear();
    let container = Arc::new(ApplicationContainer::build().await.unwrap());
    let operation = PendingWebSocketOperation::new(
        WebSocketOperationKind::Message,
        Some("deadline"),
        "message_deadline::Action",
        WebSocketActionRegistration::of::<AppTestController>(bind),
        WebSocketOperationMetadata::new(
            vec![
                WebSocketMessageMiddlewareRegistration::of::<Middleware<0>>(),
                WebSocketMessageMiddlewareRegistration::of::<Middleware<1>>(),
            ],
            vec![
                WebSocketGuardRegistration::of::<Guard<0>>(),
                WebSocketGuardRegistration::of::<Guard<1>>(),
            ],
            None,
            override_timeout,
            WebSocketAsyncApiRegistration::unspecified(),
        ),
    );
    let table = test_action_table(vec![operation], container.services()).await;
    let manager = Arc::new(ConnectionManager::with_registered_namespaces(
        8,
        8,
        ["orders".into()],
    ));
    let connection_id = Uuid::new_v4();
    let budget = crate::shutdown::ShutdownBudget::default();
    let context = Arc::new(
        WebSocketContext::new(connection_id, manager.clone(), "orders".into())
            .with_shutdown_budget(budget.clone()),
    );
    let request = Arc::new(test_request(connection_id, "orders:deadline"));
    let decoded = decode_test_request(&request);
    let (action, codec) = WsApp::resolve_message_route(&table, &context, &request).unwrap();
    let source = CancellationToken::new();
    let runtime = MessageRuntime {
        message_timeout: app_timeout,
        cleanup_timeout: Duration::from_millis(200),
        cancellation: source.clone(),
        metrics: manager.metrics().clone(),
    };
    let registry = MessageDispatchRegistry::with_budget(budget.clone());
    let owner_container = container.clone();
    let started = Instant::now();
    let force_source = source.clone();
    let scope_budget = budget.clone();
    let output =
        registry.spawn_owner_with_cancellation(connection_id, source, move |slot| async move {
            ScopeCleanupRegistry::with_budget(scope_budget)
                .run_scoped(
                    connection_id,
                    &owner_container,
                    ProcessContext::new(),
                    WsApp::dispatch_message_owner(
                        &action,
                        codec,
                        owner_container.services(),
                        context,
                        request,
                        decoded,
                        runtime,
                        slot,
                    ),
                )
                .await
                .unwrap()
                .into_dispatch()
        });
    let mut output = output;
    let dispatch = if let Some(delay) = force_after {
        tokio::select! {
            biased;
            result = &mut output => result.unwrap(),
            () = tokio::time::sleep(delay) => {
                budget.force_before(Instant::now() + Duration::from_secs(1));
                force_source.cancel();
                output.await.unwrap()
            }
        }
    } else {
        output.await.unwrap()
    };
    assert_eq!(force_source.is_cancelled(), force_after.is_some());
    registry.wait_connection(connection_id).await;
    let elapsed = started.elapsed();
    let (accounting, tasks) = registry.accounting();
    assert_eq!(accounting.outstanding, 0);
    assert_eq!(accounting.joined, 1);
    assert_eq!(tasks.outstanding, 0);
    container.close().await.unwrap();
    ResultEvidence {
        started,
        elapsed,
        dispatch,
        accounting,
    }
}

#[tokio::test(start_paused = true)]
async fn every_normal_stage_and_deadline_extractor_observe_one_instant() {
    let _lock = TEST_LOCK.lock().await;
    TERMINATION_DELAY.store(0, Ordering::SeqCst);
    let result = execute([5; 8], None, Duration::from_millis(100)).await;
    assert_eq!(result.dispatch.outcome, WsMessageOutcome::Handled);
    assert_eq!(result.accounting.execution.completed, 1);
    assert_eq!(result.accounting.middleware.normal.completed, 2);
    assert_eq!(result.accounting.middleware.termination.total, 0);
    let events = EVENTS.lock().unwrap();
    assert_eq!(
        events.iter().map(|(label, _)| *label).collect::<Vec<_>>(),
        LABELS
    );
    assert!(
        events
            .iter()
            .all(|(_, deadline)| *deadline == result.started + Duration::from_millis(100))
    );
}

#[tokio::test(start_paused = true)]
async fn elapsed_forward_work_is_not_reset_at_any_later_stage() {
    let _lock = TEST_LOCK.lock().await;
    TERMINATION_DELAY.store(0, Ordering::SeqCst);
    for (delays, interrupted) in [
        ([60, 500, 0, 0, 0, 0, 0, 0], "inner.before"),
        ([10, 10, 10, 500, 0, 0, 0, 0], "second.guard"),
        ([10, 10, 10, 10, 500, 0, 0, 0], "extractor"),
        ([10, 10, 10, 10, 0, 500, 0, 0], "action"),
        ([10, 10, 10, 10, 0, 0, 500, 0], "inner.after"),
        ([10, 10, 10, 10, 0, 0, 30, 500], "outer.after"),
    ] {
        let result = execute(delays, None, Duration::from_millis(100)).await;
        assert!(
            result.elapsed >= Duration::from_millis(350)
                && result.elapsed <= Duration::from_millis(351),
            "{interrupted}: {:?}",
            result.elapsed
        );
        assert_eq!(result.accounting.execution.timed_out, 1, "{interrupted}");
        assert_eq!(result.accounting.execution.completed, 0);
        assert_eq!(result.accounting.execution.cancellation_requested, 1);
        assert_eq!(result.accounting.execution.abort_requested, 1);
        let events = EVENTS.lock().unwrap();
        let normal = events
            .iter()
            .filter(|(label, _)| !label.ends_with("termination"))
            .collect::<Vec<_>>();
        assert_eq!(normal.last().unwrap().0, interrupted);
        assert!(
            normal
                .iter()
                .all(|(_, deadline)| *deadline == result.started + Duration::from_millis(100))
        );
        if interrupted == "inner.before" {
            assert_eq!(result.accounting.middleware.entered, 1);
            assert_eq!(events.last().unwrap().0, "outer.termination");
        } else if interrupted == "outer.after" {
            assert_eq!(result.accounting.middleware.normal.completed, 1);
            assert_eq!(result.accounting.middleware.termination.completed, 1);
        } else {
            assert_eq!(events[events.len() - 2].0, "inner.termination");
            assert_eq!(events.last().unwrap().0, "outer.termination");
        }
    }
}

#[tokio::test(start_paused = true)]
async fn operation_override_bounds_the_first_before_and_each_message_gets_a_fresh_deadline() {
    let _lock = TEST_LOCK.lock().await;
    TERMINATION_DELAY.store(0, Ordering::SeqCst);
    let cap = Duration::from_millis(50);
    let first = execute(
        [500, 0, 0, 0, 0, 0, 0, 0],
        Some(cap),
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(EVENTS.lock().unwrap()[0].1, first.started + cap);
    assert_eq!(first.accounting.middleware.entered, 0);
    assert_eq!(first.accounting.execution.timed_out, 1);
    assert!(first.elapsed == Duration::from_millis(300));

    let second = execute(
        [5; 8],
        Some(Duration::from_millis(100)),
        Duration::from_millis(10),
    )
    .await;
    assert_eq!(second.dispatch.outcome, WsMessageOutcome::Handled);
    assert!(second.started >= first.started + first.elapsed);
    assert_eq!(
        EVENTS.lock().unwrap()[0].1,
        second.started + Duration::from_millis(100)
    );
}

#[tokio::test(start_paused = true)]
async fn message_expiry_leaves_an_independent_bounded_termination_budget() {
    let _lock = TEST_LOCK.lock().await;
    TERMINATION_DELAY.store(20, Ordering::SeqCst);
    let result = execute([0, 0, 0, 0, 0, 500, 0, 0], None, Duration::from_millis(100)).await;
    assert!(result.elapsed >= Duration::from_millis(390));
    assert!(result.elapsed < Duration::from_millis(410));
    assert_eq!(result.accounting.execution.timed_out, 1);
    assert_eq!(result.accounting.middleware.termination.completed, 2);
    let events = EVENTS.lock().unwrap();
    assert_eq!(events[events.len() - 2].0, "inner.termination");
    assert_eq!(events.last().unwrap().0, "outer.termination");
    assert!(
        events
            .iter()
            .filter(|(label, _)| label.ends_with("termination"))
            .all(|(_, deadline)| *deadline > result.started + Duration::from_millis(100))
    );
    TERMINATION_DELAY.store(0, Ordering::SeqCst);
}

#[tokio::test(start_paused = true)]
async fn every_stage_can_observe_timeout_or_force_and_complete_the_entire_normal_pipeline() {
    let _lock = TEST_LOCK.lock().await;
    TERMINATION_DELAY.store(0, Ordering::SeqCst);
    for force in [false, true] {
        for stage in 0..8 {
            let result = execute_case(
                [1; 8],
                None,
                Duration::from_millis(if force { 1000 } else { 20 }),
                force.then_some(Duration::from_millis(20)),
                Some(stage),
                false,
            )
            .await;
            assert_eq!(
                result.dispatch.outcome,
                WsMessageOutcome::Handled,
                "force={force}, stage={stage}"
            );
            assert_eq!(result.accounting.execution.completed, 1);
            assert_eq!(result.accounting.execution.cancellation_requested, 1);
            assert_eq!(result.accounting.execution.abort_requested, 0);
            assert_eq!(result.accounting.completed_after_cancellation, 1);
            assert_eq!(
                result.accounting.completed_after_deadline,
                usize::from(!force)
            );
            assert_eq!(result.accounting.middleware.normal.completed, 2);
            assert_eq!(result.accounting.middleware.termination.total, 0);
            assert_eq!(
                EVENTS
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|(name, _)| *name)
                    .collect::<Vec<_>>(),
                LABELS
            );
        }
    }
}

#[tokio::test(start_paused = true)]
async fn an_ordinary_application_error_returned_during_cooperation_is_not_overwritten() {
    let _lock = TEST_LOCK.lock().await;
    TERMINATION_DELAY.store(0, Ordering::SeqCst);
    for force in [false, true] {
        let result = execute_case(
            [1; 8],
            None,
            Duration::from_millis(if force { 1000 } else { 20 }),
            force.then_some(Duration::from_millis(20)),
            Some(5),
            true,
        )
        .await;
        assert_eq!(result.accounting.execution.completed, 1);
        assert_eq!(result.accounting.middleware.normal.completed, 2);
        assert_eq!(result.accounting.middleware.termination.total, 0);
        let Some(PreparedWebSocketTerminal::ApplicationFrame(Message::Text(text))) =
            result.dispatch.terminal
        else {
            panic!("actual application error must remain a keep-open reply");
        };
        let response: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(response["data"]["code"], "USER_RESULT");
    }
}

#[tokio::test(start_paused = true)]
async fn ignoring_the_signal_is_allowed_only_until_the_shared_cooperative_cutoff() {
    let _lock = TEST_LOCK.lock().await;
    TERMINATION_DELAY.store(0, Ordering::SeqCst);
    // The handler never observes cancellation, but its whole pipeline returns.
    let result = execute(
        [0, 0, 0, 0, 0, 100, 20, 20],
        None,
        Duration::from_millis(20),
    )
    .await;
    assert_eq!(result.dispatch.outcome, WsMessageOutcome::Handled);
    assert_eq!(result.accounting.completed_after_deadline, 1);
    assert_eq!(result.accounting.middleware.normal.completed, 2);

    // Handler completion alone cannot rescue a pending outer normal exit.
    let result = execute(
        [0, 0, 0, 0, 0, 100, 20, 500],
        None,
        Duration::from_millis(20),
    )
    .await;
    assert_eq!(result.accounting.execution.timed_out, 1);
    assert_eq!(result.accounting.completed_after_deadline, 0);
    assert_eq!(result.accounting.middleware.normal.completed, 1);
    assert_eq!(result.accounting.middleware.termination.completed, 1);
    assert_eq!(
        EVENTS.lock().unwrap().last().unwrap().0,
        "outer.termination"
    );
    assert!(matches!(result.dispatch.terminal,
        Some(PreparedWebSocketTerminal::ApplicationFrame(Message::Text(ref text)))
        if text.contains("MESSAGE_TIMEOUT")));
}
