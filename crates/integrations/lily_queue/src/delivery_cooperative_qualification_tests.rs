//! Qualification uses the real slot, owner, ledger, callback adapters and DI receipts.
use super::*;
use crate::{
    DeliveryCancellationReason as Reason, DeliveryTerminationReason,
    QueueDeliveryTerminationContext,
    cancellation::DeliveryCancellationSource,
    delivery_execution::run_cooperative,
    delivery_lifecycle::TerminationState,
    shutdown_budget::{DeliveryExecutionBudget, QueueShutdownBudget, QueueShutdownDeadlines},
};
use tokio::time::Instant;

fn owner_with_budget(
    container: &ApplicationContainer,
    pipeline: CompiledQueuePipeline,
    total: Duration,
) -> (
    DeliveryLifecycleOwner,
    DeliveryExecutionSlot,
    Arc<DeliveryScopeTracker>,
    DeliveryExecutionBudget,
    DeliveryCancellationSource,
) {
    let input = delivery_input(CancellationToken::new(), total);
    let budget = DeliveryExecutionBudget::before(input.deadline, Instant::now());
    let source = input.cancellation.clone();
    let scope = container
        .create_scope(lily_injection::ProcessContext::new())
        .unwrap();
    let tracker = Arc::new(DeliveryScopeTracker::default());
    let (owner, slot) = DeliveryLifecycleOwner::new(
        scope,
        DeliveryInvocation::new(container.services(), input),
        pipeline,
        tracker.clone(),
        budget.hard,
        source.clone(),
    );
    (owner, slot, tracker, budget, source)
}

#[derive(Clone, Copy)]
enum Handler {
    Ready,
    Cooperative(bool),
    Pending,
}

async fn drive(
    owner: &mut DeliveryLifecycleOwner,
    slot: DeliveryExecutionSlot,
    budget: DeliveryExecutionBudget,
    source: &DeliveryCancellationSource,
    root: &QueueShutdownBudget,
    handler: Handler,
) -> DeliveryExecutionExit<Result<(), QueueHandlerError>> {
    let context = owner.context();
    let resources = owner.resources();
    let invocation = resources.invocation.as_mut().unwrap();
    let pipeline = &resources.pipeline;
    let ledger = &mut resources.ledger;
    let result = run_cooperative(
        slot,
        lily_injection::ProcessContext::scope(context, async {
            let mut result = pipeline
                .enter_recorded(invocation, ledger, budget.pipeline)
                .await;
            if result.is_ok() {
                result = pipeline.evaluate_guards(invocation, budget.pipeline).await;
            }
            if result.is_ok() {
                match handler {
                    Handler::Ready => {}
                    Handler::Cooperative(fail) => {
                        invocation.cancellation().cancelled().await;
                        // Returning after another await proves continued polling, not poll-once.
                        tokio::time::sleep(Duration::from_millis(2)).await;
                        record("handler.cooperated");
                        if fail {
                            result = Err(QueueHandlerError::permanent("QUEUE_HANDLER_CANCELLED"));
                        }
                    }
                    Handler::Pending => {
                        let _drop = ExecutionDropEvent;
                        std::future::pending::<()>().await;
                    }
                }
            }
            pipeline
                .unwind_recorded(invocation, ledger, result, budget.pipeline)
                .await
        }),
        source,
        budget,
        root,
    )
    .await;
    if let DeliveryExecutionExit::Interrupted(reason) = &result {
        owner.record_interruption(DeliveryTerminationReason::ExecutionCancelled(*reason));
    }
    result
}

pub(super) async fn assert_short_pipeline(
    container: &ApplicationContainer,
    pipeline: CompiledQueuePipeline,
    entered: usize,
) {
    let (mut owner, slot, tracker, budget, source) =
        owner_with_budget(container, pipeline, Duration::from_millis(40));
    assert!(matches!(
        drive(
            &mut owner,
            slot,
            budget,
            &source,
            &tracker.shutdown_budget(),
            Handler::Ready
        )
        .await,
        DeliveryExecutionExit::Interrupted(Reason::DeliveryTimeout)
    ));
    assert_eq!(source.reason(), Some(Reason::DeliveryTimeout));
    assert_eq!(owner.resources().ledger.entered(), entered);
    let ledger = owner.resources().ledger.clone();
    owner.close().await.unwrap();
    assert_eq!(
        ledger.termination_states(),
        vec![TerminationState::Completed; entered]
    );
    assert!(tracker.reconciled());
}

pub(super) async fn assert_cancelled_completion(
    container: &ApplicationContainer,
    pipeline: CompiledQueuePipeline,
) {
    let (mut owner, slot, tracker, budget, source) =
        owner_with_budget(container, pipeline, Duration::from_secs(1));
    assert!(matches!(
        drive(
            &mut owner,
            slot,
            budget,
            &source,
            &tracker.shutdown_budget(),
            Handler::Ready
        )
        .await,
        DeliveryExecutionExit::Completed(Ok(()))
    ));
    assert!(source.is_cancelled());
    assert_eq!(
        owner.resources().ledger.states(),
        [MiddlewareExitState::Completed; 2]
    );
    owner.close().await.unwrap();
    assert!(tracker.reconciled());
}

#[derive(Clone, Copy)]
enum Behavior {
    Ready,
    Pending,
    Panic,
    AfterCancel,
}

struct ProbeMiddleware {
    name: &'static str,
    before: Behavior,
    after: Behavior,
    termination: Behavior,
}
impl ProbeMiddleware {
    fn ready(name: &'static str) -> Self {
        Self {
            name,
            before: Behavior::Ready,
            after: Behavior::Ready,
            termination: Behavior::Ready,
        }
    }
}

#[derive(Default, lily_injectable_derive::Injectable)]
#[service(lifetime = "Scoped")]
struct TerminationScopeProbe;
#[async_trait]
impl lily_injection::ServiceTrait for TerminationScopeProbe {
    async fn dispose(&self) -> Result<(), lily_injection::InjectionError> {
        record("scope.disposed");
        Ok(())
    }
}

#[async_trait]
impl QueueMiddleware for ProbeMiddleware {
    async fn new(
        _: Arc<lily_injection::Extensions>,
    ) -> Result<Self, QueuePipelineComponentInitError> {
        unreachable!()
    }
    async fn before_delivery(
        &self,
        exchange: &mut QueueDeliveryExchange<'_>,
    ) -> Result<(), QueueHandlerError> {
        exchange.service::<TerminationScopeProbe>().await.unwrap();
        exchange.insert_local(PipelineLocal("retained"))?;
        record(format!("before.{}", self.name));
        match self.before {
            Behavior::Pending => std::future::pending().await,
            Behavior::Panic => panic!("before panic"),
            Behavior::AfterCancel => exchange.cancellation().cancelled().await,
            Behavior::Ready => {}
        }
        Ok(())
    }
    async fn after_delivery(
        &self,
        exchange: &mut QueueDeliveryExchange<'_>,
        _: QueueDeliveryOutcome,
    ) -> Result<(), QueueHandlerError> {
        record(format!("after.{}", self.name));
        match self.after {
            Behavior::Pending => std::future::pending().await,
            Behavior::Panic => panic!("after panic"),
            Behavior::AfterCancel => {
                exchange.cancellation().cancelled().await;
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            Behavior::Ready => {}
        }
        Ok(())
    }
    async fn on_delivery_termination(
        &self,
        context: &mut QueueDeliveryTerminationContext<'_>,
    ) -> Result<(), QueueHandlerError> {
        assert!(
            !context.cancellation().is_cancelled(),
            "execution cancellation must not poison cleanup or its siblings"
        );
        assert_eq!(context.deadline(), context.cancellation().deadline());
        assert_eq!(
            context.local::<PipelineLocal>(),
            Some(PipelineLocal("retained"))
        );
        context.service::<TerminationScopeProbe>().await.unwrap();
        record(format!(
            "termination.{}.{:?}",
            self.name,
            context.normal_exit()
        ));
        match self.termination {
            Behavior::Pending => std::future::pending().await,
            Behavior::Panic => panic!("termination panic"),
            Behavior::AfterCancel => {
                context.cancellation().cancelled().await;
                std::future::pending::<()>().await;
            }
            Behavior::Ready => {}
        }
        Ok(())
    }
}

fn probe_pipeline(probes: Vec<ProbeMiddleware>) -> CompiledQueuePipeline {
    CompiledQueuePipeline::new(
        probes.into_iter().map(compiled_middleware).collect(),
        vec![],
    )
}

#[tokio::test(start_paused = true)]
async fn cancellation_during_before_handler_or_normal_after_preserves_completed_pipeline() {
    let _serial = QUALIFICATION_LOCK.lock().await;
    let container = test_container().await;
    for phase in 0..3 {
        for forced in [false, true] {
            for fail in [false, true] {
                reset_events();
                let mut probe = ProbeMiddleware::ready("outer");
                if phase == 0 {
                    probe.before = Behavior::AfterCancel;
                }
                if phase == 2 {
                    probe.after = Behavior::AfterCancel;
                }
                let (mut owner, slot, tracker, budget, source) = owner_with_budget(
                    &container,
                    probe_pipeline(vec![probe]),
                    Duration::from_secs(2),
                );
                if forced {
                    source.cancel(Reason::ForcedShutdown);
                }
                let handler = if phase == 1 {
                    Handler::Cooperative(fail)
                } else {
                    Handler::Ready
                };
                let result = drive(
                    &mut owner,
                    slot,
                    budget,
                    &source,
                    &tracker.shutdown_budget(),
                    handler,
                )
                .await;
                match result {
                    DeliveryExecutionExit::Completed(Ok(())) => assert!(!fail || phase != 1),
                    DeliveryExecutionExit::Completed(Err(error)) => {
                        assert!(fail && phase == 1);
                        assert_eq!(error.code(), "QUEUE_HANDLER_CANCELLED");
                        assert_eq!(error.class(), QueueHandlerFailureClass::Permanent);
                    }
                    _ => panic!("genuinely completed pipeline must keep its outcome"),
                }
                assert_eq!(
                    source.reason(),
                    Some(if forced {
                        Reason::ForcedShutdown
                    } else {
                        Reason::DeliveryTimeout
                    })
                );
                let ledger = owner.resources().ledger.clone();
                owner.close().await.unwrap();
                assert_eq!(ledger.states(), [MiddlewareExitState::Completed]);
                assert_eq!(ledger.termination_states(), [TerminationState::NotStarted]);
                assert!(tracker.reconciled());
                assert_eq!(event_snapshot().last().unwrap(), "scope.disposed");
                assert!(
                    !event_snapshot()
                        .iter()
                        .any(|e| e.starts_with("termination."))
                );
            }
        }
    }
    container.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn forced_execution_is_dropped_before_reverse_termination_and_exact_scope_disposal() {
    let _serial = QUALIFICATION_LOCK.lock().await;
    reset_events();
    let container = test_container().await;
    let pipeline = probe_pipeline(vec![
        ProbeMiddleware::ready("outer"),
        ProbeMiddleware::ready("inner"),
    ]);
    let (mut owner, slot, tracker, budget, source) =
        owner_with_budget(&container, pipeline, Duration::from_secs(30));
    let now = Instant::now();
    tracker.set_shutdown_deadlines(QueueShutdownDeadlines::before(
        now + Duration::from_millis(10),
        now + Duration::from_secs(1),
    ));
    assert!(matches!(
        drive(
            &mut owner,
            slot,
            budget,
            &source,
            &tracker.shutdown_budget(),
            Handler::Pending
        )
        .await,
        DeliveryExecutionExit::Interrupted(Reason::ShutdownDeadline)
    ));
    assert_eq!(source.requested_at(), Some(now + Duration::from_millis(10)));
    assert_eq!(container.active_scope_count(), 1);
    owner.close().await.unwrap();
    assert!(tracker.reconciled());
    assert_eq!(
        event_snapshot(),
        [
            "before.outer",
            "before.inner",
            "execution.dropped",
            "termination.inner.NotStarted",
            "termination.outer.NotStarted",
            "scope.disposed"
        ]
    );
    container.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn partial_normal_exit_never_replays_completed_inner_hook_and_terminates_remaining_prefix() {
    let _serial = QUALIFICATION_LOCK.lock().await;
    reset_events();
    let container = test_container().await;
    let mut middle = ProbeMiddleware::ready("middle");
    middle.after = Behavior::Pending;
    let pipeline = probe_pipeline(vec![
        ProbeMiddleware::ready("outer"),
        middle,
        ProbeMiddleware::ready("inner"),
    ]);
    let (mut owner, slot, tracker, budget, source) =
        owner_with_budget(&container, pipeline, Duration::from_secs(2));
    assert!(matches!(
        drive(
            &mut owner,
            slot,
            budget,
            &source,
            &tracker.shutdown_budget(),
            Handler::Ready
        )
        .await,
        DeliveryExecutionExit::Interrupted(Reason::DeliveryTimeout)
    ));
    let ledger = owner.resources().ledger.clone();
    owner.close().await.unwrap();
    assert_eq!(
        ledger.states(),
        [
            MiddlewareExitState::NotStarted,
            MiddlewareExitState::Interrupted,
            MiddlewareExitState::Completed
        ]
    );
    assert_eq!(
        ledger.termination_states(),
        [
            TerminationState::Completed,
            TerminationState::Completed,
            TerminationState::NotStarted
        ]
    );
    assert_eq!(
        event_snapshot(),
        [
            "before.outer",
            "before.middle",
            "before.inner",
            "after.inner",
            "after.middle",
            "termination.middle.Interrupted",
            "termination.outer.NotStarted",
            "scope.disposed"
        ]
    );
    container.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn pending_or_panicking_termination_does_not_cancel_outer_sibling_and_is_not_success() {
    let _serial = QUALIFICATION_LOCK.lock().await;
    let container = test_container().await;
    for behavior in [Behavior::Pending, Behavior::Panic] {
        reset_events();
        let mut inner = ProbeMiddleware::ready("inner");
        inner.termination = behavior;
        let pipeline = probe_pipeline(vec![ProbeMiddleware::ready("outer"), inner]);
        let (mut owner, slot, tracker, budget, source) =
            owner_with_budget(&container, pipeline, Duration::from_secs(2));
        assert!(matches!(
            drive(
                &mut owner,
                slot,
                budget,
                &source,
                &tracker.shutdown_budget(),
                Handler::Pending
            )
            .await,
            DeliveryExecutionExit::Interrupted(Reason::DeliveryTimeout)
        ));
        let ledger = owner.resources().ledger.clone();
        assert_eq!(
            owner.close().await.unwrap_err().code(),
            "QUEUE_DELIVERY_TERMINATION_CLEANUP_FAILED"
        );
        assert_eq!(
            ledger.termination_states(),
            [
                TerminationState::Completed,
                if matches!(behavior, Behavior::Pending) {
                    TerminationState::TimedOut
                } else {
                    TerminationState::Panicked
                }
            ]
        );
        assert!(
            tracker.reconciled(),
            "cleanup failure can be terminal, it is never successful"
        );
        tracker.drain().await.unwrap_err();
        assert_eq!(
            event_snapshot(),
            [
                "before.outer",
                "before.inner",
                "execution.dropped",
                "termination.inner.NotStarted",
                "termination.outer.NotStarted",
                "scope.disposed"
            ]
        );
    }
    container.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn unfinished_enter_does_not_arm_termination_and_panicked_exit_remains_eligible() {
    let _serial = QUALIFICATION_LOCK.lock().await;
    let container = test_container().await;
    for during_before in [true, false] {
        reset_events();
        let mut inner = ProbeMiddleware::ready("inner");
        if during_before {
            inner.before = Behavior::Pending;
        } else {
            inner.after = Behavior::Panic;
        }
        let pipeline = probe_pipeline(vec![ProbeMiddleware::ready("outer"), inner]);
        let (mut owner, slot, tracker, budget, source) =
            owner_with_budget(&container, pipeline, Duration::from_secs(2));
        let result = drive(
            &mut owner,
            slot,
            budget,
            &source,
            &tracker.shutdown_budget(),
            Handler::Ready,
        )
        .await;
        if during_before {
            assert!(matches!(
                result,
                DeliveryExecutionExit::Interrupted(Reason::DeliveryTimeout)
            ));
        } else {
            assert!(
                matches!(result, DeliveryExecutionExit::Completed(Err(error)) if error.code() == "QUEUE_MIDDLEWARE_PANICKED")
            );
        }
        let ledger = owner.resources().ledger.clone();
        owner.close().await.unwrap();
        if during_before {
            assert_eq!(ledger.entered(), 1);
            assert_eq!(
                event_snapshot(),
                [
                    "before.outer",
                    "before.inner",
                    "termination.outer.NotStarted",
                    "scope.disposed"
                ]
            );
        } else {
            assert_eq!(
                ledger.termination_states(),
                [TerminationState::Completed, TerminationState::Completed]
            );
            assert_eq!(
                event_snapshot(),
                [
                    "before.outer",
                    "before.inner",
                    "after.inner",
                    "termination.inner.Panicked",
                    "termination.outer.NotStarted",
                    "scope.disposed"
                ]
            );
        }
    }
    container.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn abandoned_termination_waiter_transfers_same_ledger_without_duplicate_invocation() {
    let _serial = QUALIFICATION_LOCK.lock().await;
    reset_events();
    let container = test_container().await;
    let mut inner = ProbeMiddleware::ready("inner");
    inner.termination = Behavior::Pending;
    let (mut owner, slot, tracker, budget, source) = owner_with_budget(
        &container,
        probe_pipeline(vec![ProbeMiddleware::ready("outer"), inner]),
        Duration::from_secs(2),
    );
    drive(
        &mut owner,
        slot,
        budget,
        &source,
        &tracker.shutdown_budget(),
        Handler::Pending,
    )
    .await;
    let ledger = owner.resources().ledger.clone();
    assert!(owner.close().now_or_never().is_none());
    assert_eq!(
        ledger.termination_states(),
        [TerminationState::NotStarted, TerminationState::Running]
    );
    drop(owner);
    tracker.drain().await.unwrap_err();
    assert!(tracker.reconciled());
    assert_eq!(
        ledger.termination_states(),
        [TerminationState::Completed, TerminationState::Interrupted]
    );
    assert_eq!(
        event_snapshot(),
        [
            "before.outer",
            "before.inner",
            "execution.dropped",
            "termination.inner.NotStarted",
            "termination.outer.NotStarted",
            "scope.disposed"
        ]
    );
    container.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn exhausted_root_marks_cleanup_not_started_and_never_invents_completion() {
    let _serial = QUALIFICATION_LOCK.lock().await;
    reset_events();
    let container = test_container().await;
    let (mut owner, slot, tracker, budget, source) = owner_with_budget(
        &container,
        probe_pipeline(vec![ProbeMiddleware::ready("outer")]),
        Duration::from_secs(2),
    );
    drive(
        &mut owner,
        slot,
        budget,
        &source,
        &tracker.shutdown_budget(),
        Handler::Pending,
    )
    .await;
    let ledger = owner.resources().ledger.clone();
    tracker.set_shutdown_deadlines(QueueShutdownDeadlines::before(
        Instant::now(),
        Instant::now(),
    ));
    owner.close().await.unwrap_err();
    assert_eq!(
        ledger.termination_states(),
        [TerminationState::BudgetExhausted]
    );
    assert!(
        !event_snapshot()
            .iter()
            .any(|event| event.starts_with("termination."))
    );
    tracker.drain().await.unwrap_err();
    assert!(tracker.reconciled());
    assert!(matches!(
        container.close().await,
        Err(lily_injection::InjectionError::ShutdownFailed { .. })
    ));
}

#[tokio::test(start_paused = true)]
async fn shutdown_during_local_timeout_cooperation_keeps_first_reason_and_shrinks_same_pipeline() {
    let _serial = QUALIFICATION_LOCK.lock().await;
    let container = test_container().await;
    for cooperative in [true, false] {
        reset_events();
        let (mut owner, slot, tracker, budget, source) = owner_with_budget(
            &container,
            probe_pipeline(vec![
                ProbeMiddleware::ready("outer"),
                ProbeMiddleware::ready("inner"),
            ]),
            Duration::from_secs(2),
        );
        let root = tracker.shutdown_budget();
        let started = Instant::now();
        let shutdown = async {
            source.cancelled().await;
            assert_eq!(source.reason(), Some(Reason::DeliveryTimeout));
            assert_eq!(Instant::now(), budget.pipeline);
            source.cancel(Reason::ForcedShutdown);
            root.install(QueueShutdownDeadlines::before(
                Instant::now(),
                Instant::now() + Duration::from_millis(100),
            ));
            // A second shutdown request cannot grant a new local or root budget.
            root.install(QueueShutdownDeadlines::starting_at(
                Instant::now(),
                Duration::from_secs(60),
            ));
        };
        let execution = drive(
            &mut owner,
            slot,
            budget,
            &source,
            &root,
            if cooperative {
                Handler::Cooperative(false)
            } else {
                Handler::Pending
            },
        );
        let (result, ()) = tokio::join!(execution, shutdown);
        assert_eq!(source.reason(), Some(Reason::DeliveryTimeout));
        assert_eq!(source.requested_at(), Some(budget.pipeline));
        assert_eq!(
            root.deadlines().unwrap().hard(),
            budget.pipeline + Duration::from_millis(100)
        );
        assert_eq!(container.active_scope_count(), 1);
        let ledger = owner.resources().ledger.clone();
        if cooperative {
            assert!(matches!(result, DeliveryExecutionExit::Completed(Ok(()))));
            assert_eq!(Instant::now() - started, Duration::from_millis(1_502));
            assert_eq!(ledger.states(), [MiddlewareExitState::Completed; 2]);
        } else {
            assert!(matches!(
                result,
                DeliveryExecutionExit::Interrupted(Reason::DeliveryTimeout)
            ));
            assert_eq!(Instant::now() - started, Duration::from_millis(1_525));
            assert_eq!(ledger.states(), [MiddlewareExitState::NotStarted; 2]);
        }
        owner.close().await.unwrap();
        assert!(tracker.reconciled());
        assert_eq!(container.active_scope_count(), 0);
        if cooperative {
            assert_eq!(
                ledger.termination_states(),
                [TerminationState::NotStarted; 2]
            );
            assert_eq!(
                event_snapshot(),
                [
                    "before.outer",
                    "before.inner",
                    "handler.cooperated",
                    "after.inner",
                    "after.outer",
                    "scope.disposed",
                ]
            );
        } else {
            assert_eq!(
                ledger.termination_states(),
                [TerminationState::Completed; 2]
            );
            assert_eq!(
                event_snapshot(),
                [
                    "before.outer",
                    "before.inner",
                    "execution.dropped",
                    "termination.inner.NotStarted",
                    "termination.outer.NotStarted",
                    "scope.disposed",
                ]
            );
        }
    }
    container.close().await.unwrap();
}
