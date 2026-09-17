use std::{
    any::{Any, TypeId},
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use lily_config::ConfigService;
use lily_error::application::{QueueHandlerError, QueueHandlerFailureClass};
use lily_injection::ApplicationContainer;
use lily_queue_registry::{
    QueueDeliveryOutcome, QueueHandlerInputContract, QueueHandlerMetadata, QueuePayloadKind,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::*;
use crate::{
    ContentKind, DeliveryContext, DeliveryHeaderValue, DeliveryHeaders, DeliveryProperties,
    EventId, Redelivered, RetryCount, SchemaVersion,
    delivery_context::DeliveryInput,
    delivery_lifecycle::{
        DeliveryExecutionExit, DeliveryExecutionSlot, DeliveryLifecycleOwner, DeliveryScopeTracker,
        MiddlewareExitState,
    },
    queue_service::QueueService,
};

fn lifecycle_owner(
    container: &ApplicationContainer,
    pipeline: CompiledQueuePipeline,
) -> (
    DeliveryLifecycleOwner,
    DeliveryExecutionSlot,
    Arc<DeliveryScopeTracker>,
) {
    let input = delivery_input(CancellationToken::new(), Duration::from_secs(2));
    let deadline = input.deadline;
    let cancellation = input.cancellation.clone();
    let scope = container
        .create_scope(lily_injection::ProcessContext::new())
        .unwrap();
    let tracker = Arc::new(DeliveryScopeTracker::default());
    let (owner, slot) = DeliveryLifecycleOwner::new(
        scope,
        DeliveryInvocation::new(container.services(), input),
        pipeline,
        tracker.clone(),
        deadline,
        cancellation,
    );
    (owner, slot, tracker)
}

struct LifecyclePendingMiddleware {
    entered: Arc<tokio::sync::Notify>,
    pending_before: bool,
}

#[tokio::test]
async fn scope_receipt_cannot_attach_to_a_reused_id_or_invent_the_original_cleanup_result() {
    let _serial = QUALIFICATION_LOCK.lock().await;
    let container = test_container().await;
    let context = lily_injection::ProcessContext::new();
    let mut first = container.create_scope(context.clone()).unwrap();
    first.close().await.unwrap();
    let mut second = container.create_scope(context).unwrap();
    let input = delivery_input(CancellationToken::new(), Duration::from_secs(2));
    let tracker = Arc::new(DeliveryScopeTracker::default());
    let cancellation = input.cancellation.clone();
    let deadline = input.deadline;
    let (mut owner, _slot) = DeliveryLifecycleOwner::new(
        first,
        DeliveryInvocation::new(container.services(), input),
        CompiledQueuePipeline::default(),
        tracker.clone(),
        deadline,
        cancellation,
    );
    let error = tokio::time::timeout(Duration::from_secs(1), owner.close())
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(error.code(), "QUEUE_DELIVERY_SCOPE_RESULT_UNOBSERVED");
    assert_eq!(
        container.active_scope_count(),
        1,
        "later scope generation must remain live"
    );
    assert!(
        tracker.reconciled(),
        "original generation is terminal even without its close result"
    );
    tracker.drain().await.unwrap_err();
    second.close().await.unwrap();
    container.close().await.unwrap();
}

#[async_trait]
impl QueueMiddleware for LifecyclePendingMiddleware {
    async fn new(
        _extensions: Arc<lily_injection::Extensions>,
    ) -> Result<Self, QueuePipelineComponentInitError> {
        unreachable!("qualification constructs the exact component instance")
    }

    async fn before_delivery(
        &self,
        _exchange: &mut QueueDeliveryExchange<'_>,
    ) -> Result<(), QueueHandlerError> {
        if self.pending_before {
            record("before.pending");
            self.entered.notify_one();
            std::future::pending::<()>().await;
        }
        Ok(())
    }

    async fn after_delivery(
        &self,
        _exchange: &mut QueueDeliveryExchange<'_>,
        _outcome: QueueDeliveryOutcome,
    ) -> Result<(), QueueHandlerError> {
        record("after.pending");
        self.entered.notify_one();
        std::future::pending::<()>().await;
        Ok(())
    }
}

struct ExecutionDropEvent;
impl Drop for ExecutionDropEvent {
    fn drop(&mut self) {
        record("execution.dropped");
    }
}

#[tokio::test]
async fn stopping_only_the_execution_slot_preserves_invocation_ledger_and_open_scope() {
    let _serial = QUALIFICATION_LOCK.lock().await;
    reset_events();
    let container = test_container().await;
    let pipeline = CompiledQueuePipeline::new(vec![compiled_middleware(GlobalMiddleware)], vec![]);
    let (mut owner, slot, tracker) = lifecycle_owner(&container, pipeline);
    let abort = slot.abort_handle();
    let context = owner.context();
    let resources = owner.resources();
    let invocation = resources.invocation.as_mut().unwrap();
    let ledger = &mut resources.ledger;
    let pipeline = &resources.pipeline;
    let mut execution = Box::pin(
        slot.run(lily_injection::ProcessContext::scope(context, async {
            let _drop = ExecutionDropEvent;
            invocation
                .insert_local(PipelineLocal("retained-after-execution-stop"))
                .unwrap();
            pipeline
                .enter_recorded(invocation, ledger, invocation.deadline().instant())
                .await
                .unwrap();
            std::future::pending::<()>().await;
        })),
    );
    assert!(execution.as_mut().now_or_never().is_none());
    abort.abort();
    assert_eq!(
        event_snapshot(),
        ["before.global"],
        "abort request has not dropped user execution yet"
    );
    assert!(matches!(execution.await, DeliveryExecutionExit::Cancelled));
    assert_eq!(event_snapshot(), ["before.global", "execution.dropped"]);
    assert_eq!(
        owner
            .resources()
            .invocation
            .as_ref()
            .unwrap()
            .local::<PipelineLocal>(),
        Some(PipelineLocal("retained-after-execution-stop"))
    );
    assert_eq!(
        owner.resources().ledger.states(),
        [MiddlewareExitState::NotStarted]
    );
    assert_eq!(container.active_scope_count(), 1);
    assert!(!tracker.reconciled());
    owner.close().await.unwrap();
    tracker.drain().await.unwrap();
    assert!(tracker.reconciled());
    assert_eq!(container.active_scope_count(), 0);
    container.close().await.unwrap();
}

#[tokio::test]
async fn outer_task_abort_retains_only_successfully_entered_prefix_and_joins_cleanup() {
    let _serial = QUALIFICATION_LOCK.lock().await;
    reset_events();
    let container = test_container().await;
    let entered = Arc::new(tokio::sync::Notify::new());
    let pipeline = CompiledQueuePipeline::new(
        vec![
            compiled_middleware(GlobalMiddleware),
            compiled_middleware(LifecyclePendingMiddleware {
                entered: entered.clone(),
                pending_before: true,
            }),
            compiled_middleware(HandlerMiddleware),
        ],
        vec![],
    );
    let (mut owner, slot, tracker) = lifecycle_owner(&container, pipeline);
    let ledger = owner.resources().ledger.clone();
    let task = tokio::spawn(async move {
        let context = owner.context();
        let resources = owner.resources();
        let invocation = resources.invocation.as_mut().unwrap();
        let deadline = invocation.deadline().instant();
        let execution = slot
            .run(lily_injection::ProcessContext::scope(context, async {
                let _drop = ExecutionDropEvent;
                resources
                    .pipeline
                    .enter_recorded(invocation, &mut resources.ledger, deadline)
                    .await
            }))
            .await;
        owner.close().await.unwrap();
        execution
    });
    entered.notified().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(1), tracker.drain())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ledger.states(), [MiddlewareExitState::NotStarted]);
    assert_eq!(
        event_snapshot(),
        ["before.global", "before.pending", "execution.dropped"]
    );
    assert!(
        tracker.reconciled(),
        "execution must drop before its owner transfers resources"
    );
    assert_eq!(container.active_scope_count(), 0);
    container.close().await.unwrap();
}

#[tokio::test]
async fn interrupted_normal_after_is_recorded_without_replaying_completed_or_pending_hooks() {
    let _serial = QUALIFICATION_LOCK.lock().await;
    reset_events();
    let container = test_container().await;
    let entered = Arc::new(tokio::sync::Notify::new());
    let pipeline = CompiledQueuePipeline::new(
        vec![
            compiled_middleware(GlobalMiddleware),
            compiled_middleware(LifecyclePendingMiddleware {
                entered: entered.clone(),
                pending_before: false,
            }),
            compiled_middleware(HandlerMiddleware),
        ],
        vec![],
    );
    let (mut owner, slot, tracker) = lifecycle_owner(&container, pipeline);
    let ledger = owner.resources().ledger.clone();
    let task = tokio::spawn(async move {
        let context = owner.context();
        let resources = owner.resources();
        let invocation = resources.invocation.as_mut().unwrap();
        let deadline = invocation.deadline().instant();
        slot.run(lily_injection::ProcessContext::scope(context, async {
            let _drop = ExecutionDropEvent;
            let result = resources
                .pipeline
                .enter_recorded(invocation, &mut resources.ledger, deadline)
                .await;
            resources
                .pipeline
                .unwind_recorded(invocation, &mut resources.ledger, result, deadline)
                .await
        }))
        .await;
        owner.close().await.unwrap();
    });
    entered.notified().await;
    assert_eq!(
        ledger.states(),
        [
            MiddlewareExitState::NotStarted,
            MiddlewareExitState::Running,
            MiddlewareExitState::Completed
        ]
    );
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(1), tracker.drain())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        ledger.states(),
        [
            MiddlewareExitState::NotStarted,
            MiddlewareExitState::Interrupted,
            MiddlewareExitState::Completed
        ]
    );
    assert_eq!(
        event_snapshot(),
        [
            "before.global",
            "before.handler",
            "after.handler.success",
            "after.pending",
            "execution.dropped"
        ]
    );
    assert!(tracker.reconciled());
    assert_eq!(container.active_scope_count(), 0);
    container.close().await.unwrap();
}

#[tokio::test]
async fn repeated_normal_unwind_does_not_invoke_completed_exits_twice() {
    let _serial = QUALIFICATION_LOCK.lock().await;
    reset_events();
    let container = test_container().await;
    let pipeline = CompiledQueuePipeline::new(
        vec![
            compiled_middleware(GlobalMiddleware),
            compiled_middleware(HandlerMiddleware),
        ],
        vec![],
    );
    let (mut owner, slot, tracker) = lifecycle_owner(&container, pipeline);
    let context = owner.context();
    let resources = owner.resources();
    let invocation = resources.invocation.as_mut().unwrap();
    let deadline = invocation.deadline().instant();
    let result = slot
        .run(lily_injection::ProcessContext::scope(context, async {
            resources
                .pipeline
                .enter_recorded(invocation, &mut resources.ledger, deadline)
                .await
                .unwrap();
            resources
                .pipeline
                .unwind_recorded(invocation, &mut resources.ledger, Ok(()), deadline)
                .await
                .unwrap();
            resources
                .pipeline
                .unwind_recorded(invocation, &mut resources.ledger, Ok(()), deadline)
                .await
                .unwrap();
        }))
        .await;
    assert!(matches!(result, DeliveryExecutionExit::Completed(())));
    assert_eq!(
        owner.resources().ledger.states(),
        [
            MiddlewareExitState::Completed,
            MiddlewareExitState::Completed
        ]
    );
    owner.close().await.unwrap();
    tracker.drain().await.unwrap();
    assert_eq!(
        event_snapshot(),
        [
            "before.global",
            "before.handler",
            "after.handler.success",
            "after.global.success"
        ]
    );
    container.close().await.unwrap();
}

static QUALIFICATION_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn events() -> &'static Mutex<Vec<String>> {
    static EVENTS: OnceLock<Mutex<Vec<String>>> = OnceLock::new();
    EVENTS.get_or_init(|| Mutex::new(Vec::new()))
}

fn record(event: impl Into<String>) {
    events()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .push(event.into());
}

fn reset_events() {
    events()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clear();
}

fn event_snapshot() -> Vec<String> {
    events()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}

fn outcome_label(outcome: QueueDeliveryOutcome) -> String {
    match outcome {
        QueueDeliveryOutcome::Succeeded => "success".to_owned(),
        QueueDeliveryOutcome::Failed { class, code } => {
            format!("failed:{class:?}:{code}")
        }
        QueueDeliveryOutcome::Panicked => "panicked".to_owned(),
        QueueDeliveryOutcome::TimedOut => "timed-out".to_owned(),
        QueueDeliveryOutcome::Cancelled => "cancelled".to_owned(),
        _ => "unknown".to_owned(),
    }
}

async fn test_container() -> Arc<ApplicationContainer> {
    test_container_with_probe().await.0
}

async fn test_container_with_probe() -> (
    Arc<ApplicationContainer>,
    crate::queue_service::QueueServiceTestProbe,
) {
    let config_path = format!("/tmp/lily-capq02-{}.toml", Uuid::new_v4());
    let config = ConfigService::development(&config_path);
    let (queue_service, probe) =
        QueueService::test_support_seed(Arc::new(ConfigService::development(config_path)));
    let container = Arc::new(
        ApplicationContainer::builder()
            .seed_singleton(config)
            .seed_singleton(queue_service)
            .build()
            .await
            .expect("transport-free CAP-Q-02 container must build"),
    );
    (container, probe)
}

fn delivery_input(cancellation: CancellationToken, timeout: Duration) -> DeliveryInput {
    let mut headers = BTreeMap::new();
    headers.insert(
        "x-cap-q-02".to_owned(),
        DeliveryHeaderValue::Text(Arc::from("qualified")),
    );
    DeliveryInput {
        body: Bytes::from_static(br#"{"order_id":42}"#),
        context: DeliveryContext {
            event_id: EventId(Uuid::new_v4()),
            schema_version: SchemaVersion::try_new(1).expect("schema version must be valid"),
            content_kind: ContentKind::try_new("json").expect("content kind must be valid"),
            retry_count: RetryCount(0),
            redelivered: Redelivered(false),
            queue: Arc::from("capq02.orders"),
            exchange: Arc::from("capq02.events"),
            routing_key: Arc::from("orders.created"),
        },
        headers: DeliveryHeaders::from_entries(headers),
        properties: DeliveryProperties {
            correlation_id: Some(Arc::from("capq02-correlation")),
            ..DeliveryProperties::default()
        },
        cancellation: cancellation.into(),
        deadline: tokio::time::Instant::now() + timeout,
    }
}

fn new_invocation(
    container: &ApplicationContainer,
    cancellation: CancellationToken,
    timeout: Duration,
) -> DeliveryInvocation {
    DeliveryInvocation::new(container.services(), delivery_input(cancellation, timeout))
}

fn compiled_middleware<M>(component: M) -> CompiledQueueMiddleware
where
    M: QueueMiddleware,
{
    CompiledQueueMiddleware {
        registration: queue_middleware_registration::<M>(),
        component: Arc::new(component),
    }
}

fn compiled_guard<G>(component: G) -> CompiledQueueGuard
where
    G: QueueGuard,
{
    CompiledQueueGuard {
        registration: queue_guard_registration::<G>(),
        component: Arc::new(component),
    }
}

macro_rules! recording_middleware {
    ($type_name:ident, $label:literal) => {
        struct $type_name;

        #[async_trait]
        impl QueueMiddleware for $type_name {
            async fn new(
                _extensions: Arc<lily_injection::Extensions>,
            ) -> Result<Self, QueuePipelineComponentInitError> {
                record(concat!("init.middleware.", $label));
                Ok(Self)
            }

            async fn before_delivery(
                &self,
                _exchange: &mut QueueDeliveryExchange<'_>,
            ) -> Result<(), QueueHandlerError> {
                record(concat!("before.", $label));
                Ok(())
            }

            async fn after_delivery(
                &self,
                _exchange: &mut QueueDeliveryExchange<'_>,
                outcome: QueueDeliveryOutcome,
            ) -> Result<(), QueueHandlerError> {
                record(format!("after.{}.{}", $label, outcome_label(outcome)));
                Ok(())
            }
        }
    };
}

macro_rules! recording_guard {
    ($type_name:ident, $label:literal) => {
        struct $type_name;

        #[async_trait]
        impl QueueGuard for $type_name {
            async fn new(
                _extensions: Arc<lily_injection::Extensions>,
            ) -> Result<Self, QueuePipelineComponentInitError> {
                record(concat!("init.guard.", $label));
                Ok(Self)
            }

            async fn can_activate(
                &self,
                _exchange: &mut QueueDeliveryExchange<'_>,
            ) -> Result<(), QueueHandlerError> {
                record(concat!("guard.", $label));
                Ok(())
            }
        }
    };
}

recording_middleware!(GlobalMiddleware, "global");
recording_middleware!(ServiceMiddleware, "service");
recording_middleware!(HandlerMiddleware, "handler");
recording_guard!(GlobalGuard, "global");
recording_guard!(ServiceGuard, "service");
recording_guard!(HandlerGuard, "handler");

struct RejectingGuard;

#[async_trait]
impl QueueGuard for RejectingGuard {
    async fn new(
        _extensions: Arc<lily_injection::Extensions>,
    ) -> Result<Self, QueuePipelineComponentInitError> {
        Ok(Self)
    }

    async fn can_activate(
        &self,
        _exchange: &mut QueueDeliveryExchange<'_>,
    ) -> Result<(), QueueHandlerError> {
        record("guard.reject");
        Err(QueueHandlerError::permanent("CAPQ02_GUARD_REJECTED"))
    }
}

struct PanickingGuard;

#[async_trait]
impl QueueGuard for PanickingGuard {
    async fn new(
        _extensions: Arc<lily_injection::Extensions>,
    ) -> Result<Self, QueuePipelineComponentInitError> {
        Ok(Self)
    }

    async fn can_activate(
        &self,
        _exchange: &mut QueueDeliveryExchange<'_>,
    ) -> Result<(), QueueHandlerError> {
        panic!("CAP-Q-02 contained guard panic")
    }
}

struct SlowGuard;

#[async_trait]
impl QueueGuard for SlowGuard {
    async fn new(
        _extensions: Arc<lily_injection::Extensions>,
    ) -> Result<Self, QueuePipelineComponentInitError> {
        Ok(Self)
    }

    async fn can_activate(
        &self,
        _exchange: &mut QueueDeliveryExchange<'_>,
    ) -> Result<(), QueueHandlerError> {
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(())
    }
}

struct CooperativeCancellationGuard {
    entered: Arc<tokio::sync::Notify>,
}

#[async_trait]
impl QueueGuard for CooperativeCancellationGuard {
    async fn new(
        _extensions: Arc<lily_injection::Extensions>,
    ) -> Result<Self, QueuePipelineComponentInitError> {
        Ok(Self {
            entered: Arc::new(tokio::sync::Notify::new()),
        })
    }

    async fn can_activate(
        &self,
        exchange: &mut QueueDeliveryExchange<'_>,
    ) -> Result<(), QueueHandlerError> {
        record("guard.cooperative.entered");
        self.entered.notify_one();
        exchange.cancellation().cancelled().await;
        Err(QueueHandlerError::permanent(
            "CAPQ02_GUARD_PRIMARY_AFTER_CANCELLATION",
        ))
    }
}

struct PanicMiddleware;

#[async_trait]
impl QueueMiddleware for PanicMiddleware {
    async fn new(
        _extensions: Arc<lily_injection::Extensions>,
    ) -> Result<Self, QueuePipelineComponentInitError> {
        Ok(Self)
    }

    async fn before_delivery(
        &self,
        _exchange: &mut QueueDeliveryExchange<'_>,
    ) -> Result<(), QueueHandlerError> {
        panic!("CAP-Q-02 contained middleware panic")
    }
}

struct SlowMiddleware;

#[async_trait]
impl QueueMiddleware for SlowMiddleware {
    async fn new(
        _extensions: Arc<lily_injection::Extensions>,
    ) -> Result<Self, QueuePipelineComponentInitError> {
        Ok(Self)
    }

    async fn before_delivery(
        &self,
        _exchange: &mut QueueDeliveryExchange<'_>,
    ) -> Result<(), QueueHandlerError> {
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(())
    }
}

struct CancellingMiddleware;

#[async_trait]
impl QueueMiddleware for CancellingMiddleware {
    async fn new(
        _extensions: Arc<lily_injection::Extensions>,
    ) -> Result<Self, QueuePipelineComponentInitError> {
        Ok(Self)
    }

    async fn before_delivery(
        &self,
        exchange: &mut QueueDeliveryExchange<'_>,
    ) -> Result<(), QueueHandlerError> {
        record("before.cancelling");
        exchange
            .cancellation()
            .0
            .cancel(crate::DeliveryCancellationReason::RuntimeCancellation);
        Ok(())
    }

    async fn after_delivery(
        &self,
        _exchange: &mut QueueDeliveryExchange<'_>,
        outcome: QueueDeliveryOutcome,
    ) -> Result<(), QueueHandlerError> {
        record(format!("after.cancelling.{}", outcome_label(outcome)));
        Ok(())
    }
}

struct NeverEnteredMiddleware;

#[async_trait]
impl QueueMiddleware for NeverEnteredMiddleware {
    async fn new(
        _extensions: Arc<lily_injection::Extensions>,
    ) -> Result<Self, QueuePipelineComponentInitError> {
        Ok(Self)
    }

    async fn before_delivery(
        &self,
        _exchange: &mut QueueDeliveryExchange<'_>,
    ) -> Result<(), QueueHandlerError> {
        record("before.must-not-run");
        Ok(())
    }
}

struct FailingAfterMiddleware;

#[async_trait]
impl QueueMiddleware for FailingAfterMiddleware {
    async fn new(
        _extensions: Arc<lily_injection::Extensions>,
    ) -> Result<Self, QueuePipelineComponentInitError> {
        Ok(Self)
    }

    async fn after_delivery(
        &self,
        _exchange: &mut QueueDeliveryExchange<'_>,
        outcome: QueueDeliveryOutcome,
    ) -> Result<(), QueueHandlerError> {
        record(format!("after.inner.{}", outcome_label(outcome)));
        Err(QueueHandlerError::retryable("CAPQ02_AFTER_FAILED"))
    }
}

struct PanickingAfterMiddleware;

#[async_trait]
impl QueueMiddleware for PanickingAfterMiddleware {
    async fn new(
        _extensions: Arc<lily_injection::Extensions>,
    ) -> Result<Self, QueuePipelineComponentInitError> {
        Ok(Self)
    }

    async fn after_delivery(
        &self,
        _exchange: &mut QueueDeliveryExchange<'_>,
        _outcome: QueueDeliveryOutcome,
    ) -> Result<(), QueueHandlerError> {
        record("after.panic.entered");
        panic!("CAP-Q-02 contained reverse middleware panic")
    }
}

struct SlowAfterMiddleware;

#[async_trait]
impl QueueMiddleware for SlowAfterMiddleware {
    async fn new(
        _extensions: Arc<lily_injection::Extensions>,
    ) -> Result<Self, QueuePipelineComponentInitError> {
        Ok(Self)
    }

    async fn after_delivery(
        &self,
        _exchange: &mut QueueDeliveryExchange<'_>,
        _outcome: QueueDeliveryOutcome,
    ) -> Result<(), QueueHandlerError> {
        record("after.slow.entered");
        tokio::time::sleep(Duration::from_millis(200)).await;
        Ok(())
    }
}

struct OuterOutcomeObserver;

#[async_trait]
impl QueueMiddleware for OuterOutcomeObserver {
    async fn new(
        _extensions: Arc<lily_injection::Extensions>,
    ) -> Result<Self, QueuePipelineComponentInitError> {
        Ok(Self)
    }

    async fn after_delivery(
        &self,
        _exchange: &mut QueueDeliveryExchange<'_>,
        outcome: QueueDeliveryOutcome,
    ) -> Result<(), QueueHandlerError> {
        record(format!("after.outer.{}", outcome_label(outcome)));
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PipelineLocal(&'static str);

struct LocalPublishingMiddleware;

#[async_trait]
impl QueueMiddleware for LocalPublishingMiddleware {
    async fn new(
        _extensions: Arc<lily_injection::Extensions>,
    ) -> Result<Self, QueuePipelineComponentInitError> {
        Ok(Self)
    }

    async fn before_delivery(
        &self,
        exchange: &mut QueueDeliveryExchange<'_>,
    ) -> Result<(), QueueHandlerError> {
        assert_eq!(exchange.body(), br#"{"order_id":42}"#);
        assert!(matches!(
            exchange.headers().get("x-cap-q-02"),
            Some(DeliveryHeaderValue::Text(value)) if value.as_ref() == "qualified"
        ));
        exchange.insert_local(PipelineLocal("middleware"))?;
        Ok(())
    }

    async fn after_delivery(
        &self,
        exchange: &mut QueueDeliveryExchange<'_>,
        _outcome: QueueDeliveryOutcome,
    ) -> Result<(), QueueHandlerError> {
        assert_eq!(
            exchange.local::<PipelineLocal>(),
            Some(PipelineLocal("handler"))
        );
        record("after.local.handoff");
        Ok(())
    }
}

struct LocalTransformingGuard;

#[async_trait]
impl QueueGuard for LocalTransformingGuard {
    async fn new(
        _extensions: Arc<lily_injection::Extensions>,
    ) -> Result<Self, QueuePipelineComponentInitError> {
        Ok(Self)
    }

    async fn can_activate(
        &self,
        exchange: &mut QueueDeliveryExchange<'_>,
    ) -> Result<(), QueueHandlerError> {
        assert_eq!(
            exchange.local::<PipelineLocal>(),
            Some(PipelineLocal("middleware"))
        );
        assert_eq!(
            exchange.insert_local(PipelineLocal("guard"))?,
            Some(PipelineLocal("middleware"))
        );
        Ok(())
    }
}

struct SharedMiddleware;

static SHARED_INITIALIZATIONS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

#[async_trait]
impl QueueMiddleware for SharedMiddleware {
    async fn new(
        _extensions: Arc<lily_injection::Extensions>,
    ) -> Result<Self, QueuePipelineComponentInitError> {
        SHARED_INITIALIZATIONS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        record("init.middleware.shared");
        Ok(Self)
    }
}

struct DualRole;

#[async_trait]
impl QueueMiddleware for DualRole {
    async fn new(
        _extensions: Arc<lily_injection::Extensions>,
    ) -> Result<Self, QueuePipelineComponentInitError> {
        Ok(Self)
    }
}

#[async_trait]
impl QueueGuard for DualRole {
    async fn new(
        _extensions: Arc<lily_injection::Extensions>,
    ) -> Result<Self, QueuePipelineComponentInitError> {
        Ok(Self)
    }

    async fn can_activate(
        &self,
        _exchange: &mut QueueDeliveryExchange<'_>,
    ) -> Result<(), QueueHandlerError> {
        Ok(())
    }
}

struct DummyService;

fn unused_handler<'a>(
    _service: Arc<dyn Any + Send + Sync>,
    _invocation: &'a mut (dyn Any + Send),
) -> Pin<Box<dyn Future<Output = Result<(), QueueHandlerError>> + Send + 'a>> {
    Box::pin(async { Ok(()) })
}

fn metadata(
    queue_name: &'static str,
    service_middlewares: Vec<QueueMiddlewareRegistration>,
    service_guards: Vec<QueueGuardRegistration>,
    handler_middlewares: Vec<QueueMiddlewareRegistration>,
    handler_guards: Vec<QueueGuardRegistration>,
) -> &'static QueueHandlerMetadata {
    Box::leak(Box::new(QueueHandlerMetadata {
        service_type_id: TypeId::of::<DummyService>(),
        service_type_name: "capq02::DummyService",
        component_kind: None,
        queue_name,
        method_name: "handle",
        handler_name: "capq02::DummyService::handle",
        schema_version: 1,
        content_kind: "json",
        delivery_guarantee: lily_queue_registry::DeliveryGuarantee::AtLeastOnce,
        input_contract: QueueHandlerInputContract::new(QueuePayloadKind::Json, Some("Payload")),
        #[cfg(feature = "asyncapi")]
        asyncapi: lily_queue_registry::QueueAsyncApiRegistration::unspecified(),
        service_middlewares,
        service_guards,
        handler_middlewares,
        handler_guards,
        handler_fn: unused_handler,
    }))
}

#[tokio::test]
async fn effective_pipeline_runs_exact_forward_guard_and_reverse_order() {
    let _serial = QUALIFICATION_LOCK.lock().await;
    reset_events();
    let container = test_container().await;
    let pipeline = CompiledQueuePipeline::new(
        vec![
            compiled_middleware(GlobalMiddleware),
            compiled_middleware(ServiceMiddleware),
            compiled_middleware(HandlerMiddleware),
        ],
        vec![
            compiled_guard(GlobalGuard),
            compiled_guard(ServiceGuard),
            compiled_guard(HandlerGuard),
        ],
    );
    let deadline = tokio::time::Instant::now() + Duration::from_millis(250);
    let mut invocation =
        new_invocation(&container, CancellationToken::new(), Duration::from_secs(1));

    let (entered, result) = pipeline.enter(&mut invocation, deadline).await;
    assert_eq!(entered, 3);
    result.expect("middleware enter must succeed");
    pipeline
        .evaluate_guards(&mut invocation, deadline)
        .await
        .expect("guard plan must succeed");
    record("handler");
    pipeline
        .unwind(&mut invocation, entered, Ok(()), deadline)
        .await
        .expect("reverse unwind must succeed");

    assert_eq!(
        event_snapshot(),
        [
            "before.global",
            "before.service",
            "before.handler",
            "guard.global",
            "guard.service",
            "guard.handler",
            "handler",
            "after.handler.success",
            "after.service.success",
            "after.global.success",
        ]
    );
    container
        .close()
        .await
        .expect("container close must succeed");
}

struct RollbackFirstMiddleware;

impl Drop for RollbackFirstMiddleware {
    fn drop(&mut self) {
        record("drop.rollback.first");
    }
}

#[async_trait]
impl QueueMiddleware for RollbackFirstMiddleware {
    async fn new(
        _extensions: Arc<lily_injection::Extensions>,
    ) -> Result<Self, QueuePipelineComponentInitError> {
        record("init.rollback.first");
        Ok(Self)
    }
}

struct RollbackSecondMiddleware;

impl Drop for RollbackSecondMiddleware {
    fn drop(&mut self) {
        record("drop.rollback.second");
    }
}

#[async_trait]
impl QueueMiddleware for RollbackSecondMiddleware {
    async fn new(
        _extensions: Arc<lily_injection::Extensions>,
    ) -> Result<Self, QueuePipelineComponentInitError> {
        record("init.rollback.second");
        Ok(Self)
    }
}

struct TypedFailInitializer;

#[async_trait]
impl QueueMiddleware for TypedFailInitializer {
    async fn new(
        _extensions: Arc<lily_injection::Extensions>,
    ) -> Result<Self, QueuePipelineComponentInitError> {
        record("init.rollback.typed-fail");
        Err(QueuePipelineComponentInitError::InvalidPolicy)
    }
}

struct PanickingInitializer;
struct PanickingInitializationDrop;

impl Drop for PanickingInitializationDrop {
    fn drop(&mut self) {
        record("drop.rollback.panic-future");
    }
}

#[async_trait]
impl QueueMiddleware for PanickingInitializer {
    async fn new(
        _extensions: Arc<lily_injection::Extensions>,
    ) -> Result<Self, QueuePipelineComponentInitError> {
        let _drop_evidence = PanickingInitializationDrop;
        record("init.rollback.panic");
        panic!("CAP-Q-02 contained initializer panic")
    }
}

struct PendingInitializer;
struct PendingInitializationDrop;

impl Drop for PendingInitializationDrop {
    fn drop(&mut self) {
        record("drop.rollback.pending-future");
    }
}

fn pending_initializer_entered() -> &'static tokio::sync::Notify {
    static ENTERED: OnceLock<tokio::sync::Notify> = OnceLock::new();
    ENTERED.get_or_init(tokio::sync::Notify::new)
}

#[async_trait]
impl QueueMiddleware for PendingInitializer {
    async fn new(
        _extensions: Arc<lily_injection::Extensions>,
    ) -> Result<Self, QueuePipelineComponentInitError> {
        let _drop_evidence = PendingInitializationDrop;
        record("init.rollback.pending-entered");
        pending_initializer_entered().notify_one();
        std::future::pending::<()>().await;
        Ok(Self)
    }
}

struct PendingGuardInitializer;
struct PendingGuardInitializationDrop;

impl Drop for PendingGuardInitializationDrop {
    fn drop(&mut self) {
        record("drop.rollback.pending-guard-future");
    }
}

fn pending_guard_initializer_entered() -> &'static tokio::sync::Notify {
    static ENTERED: OnceLock<tokio::sync::Notify> = OnceLock::new();
    ENTERED.get_or_init(tokio::sync::Notify::new)
}

#[async_trait]
impl QueueGuard for PendingGuardInitializer {
    async fn new(
        _extensions: Arc<lily_injection::Extensions>,
    ) -> Result<Self, QueuePipelineComponentInitError> {
        let _drop_evidence = PendingGuardInitializationDrop;
        record("init.rollback.pending-guard-entered");
        pending_guard_initializer_entered().notify_one();
        std::future::pending::<()>().await;
        Ok(Self)
    }

    async fn can_activate(
        &self,
        _exchange: &mut QueueDeliveryExchange<'_>,
    ) -> Result<(), QueueHandlerError> {
        unreachable!("a pending guard constructor cannot produce a runtime component")
    }
}

struct PreFuturePanicIdentity;

fn pre_future_panic_initializer(
    _extensions: Arc<lily_injection::Extensions>,
) -> QueuePipelineInitializationFuture {
    record("init.rollback.pre-future-panic");
    panic!("CAP-Q-02 contained pre-future initializer panic")
}

fn inert_before<'a>(
    _component: &'a (dyn Any + Send + Sync),
    _invocation: &'a mut (dyn Any + Send),
) -> QueuePipelineHookFuture<'a> {
    Box::pin(async { Ok(()) })
}

fn inert_after<'a>(
    _component: &'a (dyn Any + Send + Sync),
    _invocation: &'a mut (dyn Any + Send),
    _outcome: QueueDeliveryOutcome,
) -> QueuePipelineHookFuture<'a> {
    Box::pin(async { Ok(()) })
}

fn pre_future_panic_registration() -> QueueMiddlewareRegistration {
    QueueMiddlewareRegistration::new(
        TypeId::of::<PreFuturePanicIdentity>(),
        std::any::type_name::<PreFuturePanicIdentity>(),
        pre_future_panic_initializer,
        inert_before,
        inert_after,
    )
}

fn failure_metadata(
    queue_name: &'static str,
    failing: QueueMiddlewareRegistration,
) -> &'static QueueHandlerMetadata {
    metadata(
        queue_name,
        vec![
            queue_middleware_registration::<RollbackFirstMiddleware>(),
            queue_middleware_registration::<RollbackSecondMiddleware>(),
        ],
        Vec::new(),
        vec![failing],
        Vec::new(),
    )
}

async fn compile_expected_failure(
    container: &Arc<ApplicationContainer>,
    metadata: &'static QueueHandlerMetadata,
    timeout: Duration,
) -> lily_error::application::MessageBrokerError {
    let result = compile_pipeline_set(
        &[QueueHandlerCompilationInput::new(metadata, None)],
        &[],
        &[],
        container.services(),
        timeout,
    )
    .await;
    match result {
        Ok(_) => panic!("second initializer must fail the complete atomic build"),
        Err(error) => error,
    }
}

#[tokio::test]
async fn atomic_initialization_failure_matrix_rolls_back_without_queue_registration() {
    let _serial = QUALIFICATION_LOCK.lock().await;
    let (container, probe) = test_container_with_probe().await;

    reset_events();
    let error = compile_expected_failure(
        &container,
        failure_metadata(
            "capq02.init.typed-failure",
            queue_middleware_registration::<TypedFailInitializer>(),
        ),
        Duration::from_millis(250),
    )
    .await;
    assert_eq!(error.error_code(), "BROKER_CONFIGURATION");
    assert!(error.to_string().contains("QUEUE_PIPELINE_POLICY_INVALID"));
    assert_eq!(
        event_snapshot(),
        [
            "init.rollback.first",
            "init.rollback.second",
            "init.rollback.typed-fail",
            "drop.rollback.second",
            "drop.rollback.first",
        ]
    );
    assert_eq!(probe.registration_count(), 0);

    reset_events();
    let error = compile_expected_failure(
        &container,
        failure_metadata(
            "capq02.init.panic",
            queue_middleware_registration::<PanickingInitializer>(),
        ),
        Duration::from_millis(250),
    )
    .await;
    assert_eq!(error.error_code(), "BROKER_CONFIGURATION");
    assert!(error.to_string().contains("initialization panicked"));
    assert_eq!(
        event_snapshot(),
        [
            "init.rollback.first",
            "init.rollback.second",
            "init.rollback.panic",
            "drop.rollback.panic-future",
            "drop.rollback.second",
            "drop.rollback.first",
        ]
    );
    assert_eq!(probe.registration_count(), 0);

    reset_events();
    let error = compile_expected_failure(
        &container,
        failure_metadata(
            "capq02.init.pre-future-panic",
            pre_future_panic_registration(),
        ),
        Duration::from_millis(250),
    )
    .await;
    assert_eq!(error.error_code(), "BROKER_CONFIGURATION");
    assert!(error.to_string().contains("before returning a future"));
    assert_eq!(
        event_snapshot(),
        [
            "init.rollback.first",
            "init.rollback.second",
            "init.rollback.pre-future-panic",
            "drop.rollback.second",
            "drop.rollback.first",
        ]
    );
    assert_eq!(probe.registration_count(), 0);

    reset_events();
    let pending = failure_metadata(
        "capq02.init.timeout",
        queue_middleware_registration::<PendingInitializer>(),
    );
    let container_for_compile = Arc::clone(&container);
    let compile = tokio::spawn(async move {
        compile_expected_failure(&container_for_compile, pending, Duration::from_millis(50)).await
    });
    pending_initializer_entered().notified().await;
    let error = compile.await.expect("compiler task must remain joinable");
    assert_eq!(error.error_code(), "BROKER_CONFIGURATION");
    assert!(error.to_string().contains("initialization timed out"));
    assert_eq!(
        event_snapshot(),
        [
            "init.rollback.first",
            "init.rollback.second",
            "init.rollback.pending-entered",
            "drop.rollback.pending-future",
            "drop.rollback.second",
            "drop.rollback.first",
        ]
    );
    assert_eq!(probe.registration_count(), 0);

    container
        .close()
        .await
        .expect("container close must succeed");
}

#[tokio::test]
async fn dropping_compile_future_drops_pending_middleware_before_reverse_prefix() {
    let _serial = QUALIFICATION_LOCK.lock().await;
    let (container, probe) = test_container_with_probe().await;
    reset_events();

    let pending = failure_metadata(
        "capq06c.init.outer-drop",
        queue_middleware_registration::<PendingInitializer>(),
    );
    let inputs = [QueueHandlerCompilationInput::new(pending, None)];
    let mut compilation = Box::pin(compile_pipeline_set(
        &inputs,
        &[],
        &[],
        container.services(),
        Duration::from_secs(30),
    ));

    tokio::select! {
        () = pending_initializer_entered().notified() => {}
        _ = &mut compilation => panic!("pending constructor completed unexpectedly"),
    }
    drop(compilation);

    let expected = [
        "init.rollback.first",
        "init.rollback.second",
        "init.rollback.pending-entered",
        "drop.rollback.pending-future",
        "drop.rollback.second",
        "drop.rollback.first",
    ];
    assert_eq!(event_snapshot(), expected);
    tokio::task::yield_now().await;
    assert_eq!(
        event_snapshot(),
        expected,
        "dropping the compiler must not leave a detached constructor task"
    );
    assert_eq!(probe.registration_count(), 0);

    container
        .close()
        .await
        .expect("container close must succeed");
}

#[tokio::test]
async fn aborting_compile_task_reaps_pending_guard_before_reverse_prefix() {
    let _serial = QUALIFICATION_LOCK.lock().await;
    let (container, probe) = test_container_with_probe().await;
    reset_events();

    let pending = metadata(
        "capq06c.guard.outer-abort",
        vec![
            queue_middleware_registration::<RollbackFirstMiddleware>(),
            queue_middleware_registration::<RollbackSecondMiddleware>(),
        ],
        Vec::new(),
        Vec::new(),
        vec![queue_guard_registration::<PendingGuardInitializer>()],
    );
    let services = container.services();
    let compilation = tokio::spawn(async move {
        compile_pipeline_set(
            &[QueueHandlerCompilationInput::new(pending, None)],
            &[],
            &[],
            services,
            Duration::from_secs(30),
        )
        .await
    });
    pending_guard_initializer_entered().notified().await;
    compilation.abort();
    let join_error = match compilation.await {
        Ok(_) => panic!("outer pipeline compilation must be cancelled"),
        Err(error) => error,
    };
    assert!(join_error.is_cancelled());

    let expected = [
        "init.rollback.first",
        "init.rollback.second",
        "init.rollback.pending-guard-entered",
        "drop.rollback.pending-guard-future",
        "drop.rollback.second",
        "drop.rollback.first",
    ];
    assert_eq!(event_snapshot(), expected);
    tokio::task::yield_now().await;
    assert_eq!(
        event_snapshot(),
        expected,
        "joining the aborted compiler must prove that no constructor child remains"
    );
    assert_eq!(probe.registration_count(), 0);

    container
        .close()
        .await
        .expect("container close must succeed");
}

#[tokio::test]
async fn typed_guard_rejection_skips_remaining_guards_and_unwinds_all_entered_middleware() {
    let _serial = QUALIFICATION_LOCK.lock().await;
    reset_events();
    let container = test_container().await;
    let pipeline = CompiledQueuePipeline::new(
        vec![
            compiled_middleware(GlobalMiddleware),
            compiled_middleware(ServiceMiddleware),
            compiled_middleware(HandlerMiddleware),
        ],
        vec![
            compiled_guard(GlobalGuard),
            compiled_guard(RejectingGuard),
            compiled_guard(HandlerGuard),
        ],
    );
    let deadline = tokio::time::Instant::now() + Duration::from_millis(250);
    let mut invocation =
        new_invocation(&container, CancellationToken::new(), Duration::from_secs(1));

    let (entered, entered_result) = pipeline.enter(&mut invocation, deadline).await;
    entered_result.expect("middleware enter must succeed");
    let rejection = pipeline
        .evaluate_guards(&mut invocation, deadline)
        .await
        .expect_err("guard must reject");
    assert_eq!(rejection.class(), QueueHandlerFailureClass::Permanent);
    assert_eq!(rejection.code(), "CAPQ02_GUARD_REJECTED");
    let result = pipeline
        .unwind(&mut invocation, entered, Err(rejection), deadline)
        .await
        .expect_err("guard rejection must remain primary");
    assert_eq!(result.code(), "CAPQ02_GUARD_REJECTED");

    assert_eq!(
        event_snapshot(),
        [
            "before.global",
            "before.service",
            "before.handler",
            "guard.global",
            "guard.reject",
            "after.handler.failed:Permanent:CAPQ02_GUARD_REJECTED",
            "after.service.failed:Permanent:CAPQ02_GUARD_REJECTED",
            "after.global.failed:Permanent:CAPQ02_GUARD_REJECTED",
        ]
    );
    container
        .close()
        .await
        .expect("container close must succeed");
}

#[tokio::test]
async fn guard_panic_timeout_and_cooperative_cancellation_preserve_primary_outcome() {
    let _serial = QUALIFICATION_LOCK.lock().await;
    let container = test_container().await;

    reset_events();
    let panic_pipeline = CompiledQueuePipeline::new(
        vec![compiled_middleware(GlobalMiddleware)],
        vec![compiled_guard(PanickingGuard)],
    );
    let deadline = tokio::time::Instant::now() + Duration::from_millis(250);
    let mut invocation =
        new_invocation(&container, CancellationToken::new(), Duration::from_secs(1));
    let (entered, enter_result) = panic_pipeline.enter(&mut invocation, deadline).await;
    enter_result.expect("middleware must enter before the guard panic");
    let error = panic_pipeline
        .evaluate_guards(&mut invocation, deadline)
        .await
        .expect_err("guard panic must be contained");
    assert_eq!(error.code(), "QUEUE_GUARD_PANICKED");
    panic_pipeline
        .unwind(&mut invocation, entered, Err(error), deadline)
        .await
        .expect_err("guard panic must remain the primary outcome");
    assert_eq!(event_snapshot(), ["before.global", "after.global.panicked"]);

    reset_events();
    let timeout_pipeline = CompiledQueuePipeline::new(
        vec![compiled_middleware(GlobalMiddleware)],
        vec![compiled_guard(SlowGuard)],
    );
    cooperative::assert_short_pipeline(&container, timeout_pipeline, 1).await;
    assert_eq!(event_snapshot(), ["before.global"]);

    reset_events();
    let guard_entered = Arc::new(tokio::sync::Notify::new());
    let cancellation = CancellationToken::new();
    let cooperative_pipeline = CompiledQueuePipeline::new(
        vec![compiled_middleware(GlobalMiddleware)],
        vec![compiled_guard(CooperativeCancellationGuard {
            entered: Arc::clone(&guard_entered),
        })],
    );
    let deadline = tokio::time::Instant::now() + Duration::from_millis(250);
    let mut invocation = new_invocation(&container, cancellation.clone(), Duration::from_secs(1));
    let (entered, enter_result) = cooperative_pipeline.enter(&mut invocation, deadline).await;
    enter_result.expect("middleware must enter before cooperative cancellation");
    let evaluation = cooperative_pipeline.evaluate_guards(&mut invocation, deadline);
    let cancellation_driver = async {
        guard_entered.notified().await;
        cancellation.cancel();
    };
    let (guard_result, ()) = tokio::join!(evaluation, cancellation_driver);
    let error = guard_result.expect_err("guard must preserve its typed primary rejection");
    assert_eq!(error.class(), QueueHandlerFailureClass::Permanent);
    assert_eq!(error.code(), "CAPQ02_GUARD_PRIMARY_AFTER_CANCELLATION");
    let result = cooperative_pipeline
        .unwind(&mut invocation, entered, Err(error), deadline)
        .await
        .expect_err("later cancellation must not reclassify the primary rejection");
    assert_eq!(result.class(), QueueHandlerFailureClass::Permanent);
    assert_eq!(result.code(), "CAPQ02_GUARD_PRIMARY_AFTER_CANCELLATION");
    assert_eq!(
        event_snapshot(),
        [
            "before.global",
            "guard.cooperative.entered",
            "after.global.failed:Permanent:CAPQ02_GUARD_PRIMARY_AFTER_CANCELLATION",
        ]
    );

    container
        .close()
        .await
        .expect("container close must succeed");
}

#[tokio::test]
async fn handler_failure_modes_are_visible_to_reverse_unwind_without_reclassification() {
    let _serial = QUALIFICATION_LOCK.lock().await;
    let container = test_container().await;
    let pipeline =
        CompiledQueuePipeline::new(vec![compiled_middleware(GlobalMiddleware)], Vec::new());

    let cases = [
        (
            QueueHandlerError::permanent("CAPQ02_HANDLER_FAILED"),
            "after.global.failed:Permanent:CAPQ02_HANDLER_FAILED",
        ),
        (
            QueueHandlerError::retryable("QUEUE_HANDLER_PANICKED"),
            "after.global.panicked",
        ),
        (
            QueueHandlerError::retryable("QUEUE_DELIVERY_EXECUTION_TIMED_OUT"),
            "after.global.failed:Retryable:QUEUE_DELIVERY_EXECUTION_TIMED_OUT",
        ),
        (
            QueueHandlerError::retryable("QUEUE_HANDLER_CANCELLED"),
            "after.global.failed:Retryable:QUEUE_HANDLER_CANCELLED",
        ),
    ];

    for (primary, expected_after) in cases {
        reset_events();
        let deadline = tokio::time::Instant::now() + Duration::from_millis(250);
        let mut invocation =
            new_invocation(&container, CancellationToken::new(), Duration::from_secs(1));
        let (entered, enter_result) = pipeline.enter(&mut invocation, deadline).await;
        enter_result.expect("recording middleware must enter");
        let expected_code = primary.code();
        let result = pipeline
            .unwind(&mut invocation, entered, Err(primary), deadline)
            .await
            .expect_err("primary handler failure must survive unwind");
        assert_eq!(result.code(), expected_code);
        assert_eq!(event_snapshot(), ["before.global", expected_after]);
    }
    container
        .close()
        .await
        .expect("container close must succeed");
}

#[tokio::test]
async fn middleware_panic_timeout_and_cancellation_are_contained_and_unwind_only_entered_prefix() {
    let _serial = QUALIFICATION_LOCK.lock().await;
    let container = test_container().await;

    reset_events();
    let panic_pipeline = CompiledQueuePipeline::new(
        vec![
            compiled_middleware(GlobalMiddleware),
            compiled_middleware(PanicMiddleware),
            compiled_middleware(NeverEnteredMiddleware),
        ],
        Vec::new(),
    );
    let deadline = tokio::time::Instant::now() + Duration::from_millis(250);
    let mut panic_invocation =
        new_invocation(&container, CancellationToken::new(), Duration::from_secs(1));
    let (entered, result) = panic_pipeline.enter(&mut panic_invocation, deadline).await;
    assert_eq!(entered, 1);
    let error = result.expect_err("panic must be contained");
    assert_eq!(error.code(), "QUEUE_MIDDLEWARE_PANICKED");
    panic_pipeline
        .unwind(&mut panic_invocation, entered, Err(error), deadline)
        .await
        .expect_err("panic classification must remain primary");
    assert_eq!(event_snapshot(), ["before.global", "after.global.panicked"]);

    reset_events();
    let timeout_pipeline = CompiledQueuePipeline::new(
        vec![
            compiled_middleware(GlobalMiddleware),
            compiled_middleware(SlowMiddleware),
            compiled_middleware(NeverEnteredMiddleware),
        ],
        Vec::new(),
    );
    cooperative::assert_short_pipeline(&container, timeout_pipeline, 1).await;
    assert_eq!(event_snapshot(), ["before.global"]);

    reset_events();
    let cancellation_pipeline = CompiledQueuePipeline::new(
        vec![
            compiled_middleware(CancellingMiddleware),
            compiled_middleware(NeverEnteredMiddleware),
        ],
        Vec::new(),
    );
    cooperative::assert_cancelled_completion(&container, cancellation_pipeline).await;
    assert_eq!(
        event_snapshot(),
        [
            "before.cancelling",
            "before.must-not-run",
            "after.cancelling.success"
        ]
    );

    container
        .close()
        .await
        .expect("container close must succeed");
}

#[tokio::test]
async fn unwind_failure_cannot_replace_primary_failure_but_can_fail_prior_success() {
    let _serial = QUALIFICATION_LOCK.lock().await;
    let container = test_container().await;
    let pipeline = CompiledQueuePipeline::new(
        vec![
            compiled_middleware(OuterOutcomeObserver),
            compiled_middleware(FailingAfterMiddleware),
        ],
        Vec::new(),
    );

    reset_events();
    let deadline = tokio::time::Instant::now() + Duration::from_millis(250);
    let mut invocation =
        new_invocation(&container, CancellationToken::new(), Duration::from_secs(1));
    let (entered, enter_result) = pipeline.enter(&mut invocation, deadline).await;
    enter_result.expect("pipeline must enter");
    let result = pipeline
        .unwind(
            &mut invocation,
            entered,
            Err(QueueHandlerError::permanent("CAPQ02_PRIMARY")),
            deadline,
        )
        .await
        .expect_err("primary error must survive middleware failure");
    assert_eq!(result.class(), QueueHandlerFailureClass::Permanent);
    assert_eq!(result.code(), "CAPQ02_PRIMARY");
    assert_eq!(
        event_snapshot(),
        [
            "after.inner.failed:Permanent:CAPQ02_PRIMARY",
            "after.outer.failed:Permanent:CAPQ02_PRIMARY",
        ]
    );

    reset_events();
    let mut invocation =
        new_invocation(&container, CancellationToken::new(), Duration::from_secs(1));
    let (entered, enter_result) = pipeline.enter(&mut invocation, deadline).await;
    enter_result.expect("pipeline must enter");
    let result = pipeline
        .unwind(&mut invocation, entered, Ok(()), deadline)
        .await
        .expect_err("after hook may turn success into failure");
    assert_eq!(result.code(), "CAPQ02_AFTER_FAILED");
    assert_eq!(
        event_snapshot(),
        [
            "after.inner.success",
            "after.outer.failed:Retryable:CAPQ02_AFTER_FAILED",
        ]
    );
    container
        .close()
        .await
        .expect("container close must succeed");
}

#[tokio::test]
async fn reverse_middleware_panic_and_timeout_are_contained_and_visible_to_outer_unwind() {
    let _serial = QUALIFICATION_LOCK.lock().await;
    let container = test_container().await;

    reset_events();
    let panic_pipeline = CompiledQueuePipeline::new(
        vec![
            compiled_middleware(OuterOutcomeObserver),
            compiled_middleware(PanickingAfterMiddleware),
        ],
        Vec::new(),
    );
    let deadline = tokio::time::Instant::now() + Duration::from_millis(250);
    let mut invocation =
        new_invocation(&container, CancellationToken::new(), Duration::from_secs(1));
    let (entered, enter_result) = panic_pipeline.enter(&mut invocation, deadline).await;
    enter_result.expect("middleware must enter before reverse panic");
    let error = panic_pipeline
        .unwind(&mut invocation, entered, Ok(()), deadline)
        .await
        .expect_err("reverse panic must be contained");
    assert_eq!(error.code(), "QUEUE_MIDDLEWARE_PANICKED");
    assert_eq!(event_snapshot(), ["after.panic.entered"]);

    reset_events();
    let timeout_pipeline = CompiledQueuePipeline::new(
        vec![
            compiled_middleware(OuterOutcomeObserver),
            compiled_middleware(SlowAfterMiddleware),
        ],
        Vec::new(),
    );
    cooperative::assert_short_pipeline(&container, timeout_pipeline, 2).await;
    assert_eq!(event_snapshot(), ["after.slow.entered"]);

    container
        .close()
        .await
        .expect("container close must succeed");
}

#[tokio::test]
async fn delivery_local_state_flows_from_middleware_through_guard_handler_and_unwind() {
    let _serial = QUALIFICATION_LOCK.lock().await;
    reset_events();
    let container = test_container().await;
    let pipeline = CompiledQueuePipeline::new(
        vec![compiled_middleware(LocalPublishingMiddleware)],
        vec![compiled_guard(LocalTransformingGuard)],
    );
    let deadline = tokio::time::Instant::now() + Duration::from_millis(250);
    let mut invocation =
        new_invocation(&container, CancellationToken::new(), Duration::from_secs(1));

    let (entered, result) = pipeline.enter(&mut invocation, deadline).await;
    result.expect("middleware must publish local state");
    pipeline
        .evaluate_guards(&mut invocation, deadline)
        .await
        .expect("guard must transform local state");
    assert_eq!(
        invocation.local::<PipelineLocal>(),
        Some(PipelineLocal("guard"))
    );
    assert_eq!(
        invocation.insert_local(PipelineLocal("handler")).unwrap(),
        Some(PipelineLocal("guard"))
    );
    pipeline
        .unwind(&mut invocation, entered, Ok(()), deadline)
        .await
        .expect("middleware must observe handler local state");
    assert_eq!(event_snapshot(), ["after.local.handoff"]);
    container
        .close()
        .await
        .expect("container close must succeed");
}

#[test]
fn effective_plan_rejects_duplicate_component_and_cross_role_type_before_initialization() {
    let duplicate = metadata(
        "capq02.duplicate",
        vec![queue_middleware_registration::<GlobalMiddleware>()],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    let duplicate_inputs = [QueueHandlerCompilationInput::new(duplicate, None)];
    let error = validate_pipeline_inputs(
        &duplicate_inputs,
        &[queue_middleware_registration::<GlobalMiddleware>()],
        &[],
    )
    .expect_err("global plus service duplicate must fail closed");
    assert_eq!(error.error_code(), "BROKER_CONFIGURATION");
    assert!(error.to_string().contains("duplicate queue middleware"));

    let cross_role = metadata(
        "capq02.cross-role",
        vec![queue_middleware_registration::<DualRole>()],
        vec![queue_guard_registration::<DualRole>()],
        Vec::new(),
        Vec::new(),
    );
    let error = validate_pipeline_inputs(
        &[QueueHandlerCompilationInput::new(cross_role, None)],
        &[],
        &[],
    )
    .expect_err("same concrete type cannot own both pipeline roles");
    assert_eq!(error.error_code(), "BROKER_CONFIGURATION");
    assert!(error.to_string().contains("both middleware and guard"));
}

#[tokio::test]
async fn atomic_compiler_initializes_shared_type_once_and_materializes_exact_plan_order() {
    let _serial = QUALIFICATION_LOCK.lock().await;
    reset_events();
    SHARED_INITIALIZATIONS.store(0, std::sync::atomic::Ordering::SeqCst);
    let container = test_container().await;
    let first = metadata(
        "capq02.shared.first",
        vec![queue_middleware_registration::<ServiceMiddleware>()],
        vec![queue_guard_registration::<ServiceGuard>()],
        vec![queue_middleware_registration::<SharedMiddleware>()],
        vec![queue_guard_registration::<HandlerGuard>()],
    );
    let second = metadata(
        "capq02.shared.second",
        Vec::new(),
        Vec::new(),
        vec![queue_middleware_registration::<SharedMiddleware>()],
        Vec::new(),
    );
    let compiled = compile_pipeline_set(
        &[
            QueueHandlerCompilationInput::new(first, None),
            QueueHandlerCompilationInput::new(second, None),
        ],
        &[queue_middleware_registration::<GlobalMiddleware>()],
        &[queue_guard_registration::<GlobalGuard>()],
        container.services(),
        Duration::from_millis(500),
    )
    .await
    .expect("atomic compiler must initialize a valid pipeline set");

    assert_eq!(compiled.pipelines.len(), 2);
    assert_eq!(
        SHARED_INITIALIZATIONS.load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    assert!(Arc::ptr_eq(
        &compiled.pipelines[0].middlewares[2].component,
        &compiled.pipelines[1].middlewares[1].component,
    ));
    assert_eq!(
        compiled.pipelines[0]
            .middlewares
            .iter()
            .map(|entry| entry.registration.type_name())
            .collect::<Vec<_>>(),
        [
            std::any::type_name::<GlobalMiddleware>(),
            std::any::type_name::<ServiceMiddleware>(),
            std::any::type_name::<SharedMiddleware>(),
        ]
    );
    assert_eq!(
        compiled.pipelines[0]
            .guards
            .iter()
            .map(|entry| entry.registration.type_name())
            .collect::<Vec<_>>(),
        [
            std::any::type_name::<GlobalGuard>(),
            std::any::type_name::<ServiceGuard>(),
            std::any::type_name::<HandlerGuard>(),
        ]
    );
    assert_eq!(
        event_snapshot(),
        [
            "init.middleware.global",
            "init.middleware.service",
            "init.middleware.shared",
            "init.guard.global",
            "init.guard.service",
            "init.guard.handler",
        ]
    );
    container
        .close()
        .await
        .expect("container close must succeed");
}

#[path = "delivery_cooperative_qualification_tests.rs"]
mod cooperative;
