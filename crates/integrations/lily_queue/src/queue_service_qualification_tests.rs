use std::{
    any::{Any, TypeId},
    collections::{BTreeMap, HashSet},
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use lily_config::ConfigService;
use lily_error::{
    application::{MessageBrokerError, QueueHandlerError, QueueHandlerFailureClass},
    injection::InjectionError,
};
use lily_injectable_derive::Injectable;
use lily_injection::{ApplicationContainer, ProcessContext, ServiceTrait};
use lily_queue_registry::{QueueHandlerInputContract, QueueHandlerMetadata, QueuePayloadKind};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::*;
use crate::{
    BinaryPayload, ContentKind, DeliveryCancellation, DeliveryContext, DeliveryDeadline,
    DeliveryHeaderValue, DeliveryHeaders, DeliveryProperties, EventId, FromDelivery,
    FromDeliveryParts, Json, Local, RawDelivery, Redelivered, RetryCount, SchemaVersion, Service,
    TextPayload,
    delivery_context::DeliveryInput,
    queue_trait::Queue,
    retry_engine_trait::FailureClass,
    settlement::{
        ExecutionOutcome, HandoffDestination, HandoffPlan, HandoffReceipt, SettlementAuthority,
        SettlementObserver, SettlementObserverFailure, SettlementPort, SettlementTerminal,
        materialize_delivery,
    },
};

static QUALIFICATION_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
static NEXT_SCOPED_ID: AtomicUsize = AtomicUsize::new(0);
static NEXT_TRANSIENT_ID: AtomicUsize = AtomicUsize::new(0);
static NEXT_HANDLER_ID: AtomicUsize = AtomicUsize::new(0);
static JSON_CALLS: AtomicUsize = AtomicUsize::new(0);
static CODEC_CALLS: AtomicUsize = AtomicUsize::new(0);
static REJECTING_HANDLER_CALLS: AtomicUsize = AtomicUsize::new(0);
static SCOPED_PROBE_DISPOSES: AtomicUsize = AtomicUsize::new(0);
static TRANSIENT_PROBE_DISPOSES: AtomicUsize = AtomicUsize::new(0);
static SCOPED_HANDLER_DISPOSES: AtomicUsize = AtomicUsize::new(0);
static SINGLETON_HANDLER_STARTS: AtomicUsize = AtomicUsize::new(0);
static SINGLETON_HANDLER_DISPOSES: AtomicUsize = AtomicUsize::new(0);
static TRANSIENT_HANDLER_STARTS: AtomicUsize = AtomicUsize::new(0);
static TRANSIENT_HANDLER_DISPOSES: AtomicUsize = AtomicUsize::new(0);
static BLOCKING_PROBE_DISPOSES: AtomicUsize = AtomicUsize::new(0);
static VERSION_ONE_CALLS: AtomicUsize = AtomicUsize::new(0);
static VERSION_TWO_JSON_CALLS: AtomicUsize = AtomicUsize::new(0);
static VERSION_TWO_BINARY_CALLS: AtomicUsize = AtomicUsize::new(0);

struct CooperativePipelineMiddleware;
#[async_trait]
impl crate::QueueMiddleware for CooperativePipelineMiddleware {
    async fn new(
        _: Arc<lily_injection::Extensions>,
    ) -> Result<Self, lily_queue_registry::QueuePipelineComponentInitError> {
        Ok(Self)
    }
    async fn before_delivery(
        &self,
        exchange: &mut crate::QueueDeliveryExchange<'_>,
    ) -> Result<(), QueueHandlerError> {
        exchange.insert_local(exchange.deadline())?;
        record(PipelineEvent::Handler("cooperative.before"));
        tokio::time::sleep(Duration::from_millis(10)).await;
        Ok(())
    }
    async fn after_delivery(
        &self,
        exchange: &mut crate::QueueDeliveryExchange<'_>,
        outcome: crate::QueueDeliveryOutcome,
    ) -> Result<(), QueueHandlerError> {
        assert_eq!(
            exchange.local::<DeliveryDeadline>().unwrap().instant(),
            exchange.deadline().instant()
        );
        assert!(exchange.cancellation().is_cancelled());
        if !matches!(outcome, crate::QueueDeliveryOutcome::Succeeded) {
            assert_eq!(
                outcome,
                crate::QueueDeliveryOutcome::Failed {
                    class: QueueHandlerFailureClass::Permanent,
                    code: "QUEUE_HANDLER_CANCELLED",
                }
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        record(PipelineEvent::Handler(
            if matches!(outcome, crate::QueueDeliveryOutcome::Succeeded) {
                "cooperative.after.success"
            } else {
                "cooperative.after.error"
            },
        ));
        Ok(())
    }
    async fn on_delivery_termination(
        &self,
        _: &mut crate::QueueDeliveryTerminationContext<'_>,
    ) -> Result<(), QueueHandlerError> {
        record(PipelineEvent::Handler("cooperative.termination.unexpected"));
        Ok(())
    }
}

struct CooperativePipelineGuard;
#[async_trait]
impl crate::QueueGuard for CooperativePipelineGuard {
    async fn new(
        _: Arc<lily_injection::Extensions>,
    ) -> Result<Self, lily_queue_registry::QueuePipelineComponentInitError> {
        Ok(Self)
    }
    async fn can_activate(
        &self,
        exchange: &mut crate::QueueDeliveryExchange<'_>,
    ) -> Result<(), QueueHandlerError> {
        assert_eq!(
            exchange.local::<DeliveryDeadline>().unwrap().instant(),
            exchange.deadline().instant()
        );
        record(PipelineEvent::Handler("cooperative.guard"));
        tokio::time::sleep(Duration::from_millis(10)).await;
        Ok(())
    }
}

struct CooperativeParts;
impl FromDeliveryParts for CooperativeParts {
    type Rejection = QueueHandlerError;
    async fn from_delivery_parts(
        invocation: &mut crate::DeliveryInvocation,
    ) -> Result<Self, Self::Rejection> {
        assert_eq!(
            invocation.local::<DeliveryDeadline>().unwrap().instant(),
            invocation.deadline().instant()
        );
        invocation.cancellation().cancelled().await;
        tokio::time::sleep(Duration::from_millis(5)).await;
        record(PipelineEvent::Handler("cooperative.extractor"));
        Ok(Self)
    }
}

#[derive(Default, Injectable)]
#[service(lifetime = "Scoped")]
struct CooperativeHandlers;
#[async_trait]
impl ServiceTrait for CooperativeHandlers {
    async fn dispose(&self) -> Result<(), InjectionError> {
        record(PipelineEvent::Cleanup("cooperative.scope"));
        Ok(())
    }
}
#[crate::queue_service]
#[crate::middleware(CooperativePipelineMiddleware)]
#[crate::guard(CooperativePipelineGuard)]
impl CooperativeHandlers {
    #[crate::queue("lifecycle.cooperative-success", version = 1, content = "none")]
    async fn success(
        &self,
        _: CooperativeParts,
        deadline: DeliveryDeadline,
        Local(original): Local<DeliveryDeadline>,
    ) -> Result<(), QueueHandlerError> {
        assert_eq!(deadline.instant(), original.instant());
        record(PipelineEvent::Handler("cooperative.action"));
        tokio::time::sleep(Duration::from_millis(5)).await;
        Ok(())
    }
    #[crate::queue("lifecycle.cooperative-error", version = 1, content = "none")]
    async fn failure(&self, _: CooperativeParts) -> Result<(), QueueHandlerError> {
        record(PipelineEvent::Handler("cooperative.action"));
        tokio::time::sleep(Duration::from_millis(5)).await;
        Err(QueueHandlerError::permanent("QUEUE_HANDLER_CANCELLED"))
    }
}

#[tokio::test(start_paused = true)]
async fn generated_cooperative_pipeline_has_one_deadline_and_preserves_real_success_or_typed_failure()
 {
    use crate::{
        cancellation::DeliveryCancellationSource, shutdown_budget::QueueShutdownDeadlines,
    };
    let _serial = QUALIFICATION_LOCK.lock().await;
    reset_state();
    let container = build_container(Arc::new(FakeQueue::default())).await;
    for queue in [
        "lifecycle.cooperative-success",
        "lifecycle.cooperative-error",
    ] {
        for cancellation in [
            DeliveryCancellationReason::DeliveryTimeout,
            DeliveryCancellationReason::ShutdownDeadline,
            DeliveryCancellationReason::ForcedShutdown,
        ] {
            let mut handlers = compile_queue_handlers(
                container.clone(),
                vec![QueueHandlerCompilationInput::new(metadata(queue), None)],
                &[],
                &[],
                Duration::from_secs(1),
            )
            .await
            .unwrap()
            .into_vec();
            let handler = handlers.pop().unwrap().into_registration().1;
            let now = tokio::time::Instant::now();
            if cancellation != DeliveryCancellationReason::DeliveryTimeout {
                handler
                    .scope_tracker
                    .set_shutdown_deadlines(QueueShutdownDeadlines::before(
                        now + Duration::from_millis(50),
                        now + Duration::from_secs(1),
                    ));
            }
            let source = DeliveryCancellationSource::new();
            let mut input = none_input(queue);
            input.cancellation = source.child();
            if cancellation == DeliveryCancellationReason::ForcedShutdown {
                source.cancel(cancellation);
            }
            events().lock().unwrap().clear();
            let result = (handler.callback)(input).await;
            if queue.ends_with("success") {
                result.unwrap();
            } else {
                match result {
                    Err(QueueExecutionError::Handler(error)) => {
                        assert_eq!(error.code(), "QUEUE_HANDLER_CANCELLED");
                        assert_eq!(error.class(), QueueHandlerFailureClass::Permanent);
                    }
                    other => panic!("user result cannot become framework cancellation: {other:?}"),
                }
            }
            assert!(handler.scope_tracker.reconciled());
            assert_eq!(container.active_scope_count(), 0);
            assert_eq!(
                *events().lock().unwrap(),
                [
                    PipelineEvent::Handler("cooperative.before"),
                    PipelineEvent::Handler("cooperative.guard"),
                    PipelineEvent::Handler("cooperative.extractor"),
                    PipelineEvent::Handler("cooperative.action"),
                    PipelineEvent::Handler(if queue.ends_with("success") {
                        "cooperative.after.success"
                    } else {
                        "cooperative.after.error"
                    }),
                    PipelineEvent::Cleanup("cooperative.scope"),
                ]
            );
            assert!(
                tokio::time::Instant::now() < now + Duration::from_secs(2),
                "normal callbacks share the delivery's original cutoff"
            );
        }
    }
    container.close().await.unwrap();
}

fn timeout_handler_entered() -> &'static Notify {
    static ENTERED: OnceLock<Notify> = OnceLock::new();
    ENTERED.get_or_init(Notify::new)
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum PipelineEvent {
    Handler(&'static str),
    Cleanup(&'static str),
    ScopeReconciled,
    Settlement(&'static str),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CapQ02PipelineLocal(&'static str);

#[derive(Default, Injectable)]
#[service(lifetime = "Scoped")]
struct CapQ02PipelineProbe;

#[async_trait]
impl ServiceTrait for CapQ02PipelineProbe {
    async fn dispose(&self) -> Result<(), InjectionError> {
        record(PipelineEvent::Cleanup("capq02-pipeline-probe"));
        Ok(())
    }
}

struct CapQ02GlobalMiddleware;

#[async_trait]
impl crate::QueueMiddleware for CapQ02GlobalMiddleware {
    async fn new(
        _extensions: Arc<lily_injection::Extensions>,
    ) -> Result<Self, lily_queue_registry::QueuePipelineComponentInitError> {
        Ok(Self)
    }

    async fn before_delivery(
        &self,
        exchange: &mut crate::QueueDeliveryExchange<'_>,
    ) -> Result<(), QueueHandlerError> {
        let _probe = exchange
            .service::<CapQ02PipelineProbe>()
            .await
            .map_err(|error| {
                QueueHandlerError::retryable_with_source(
                    "CAPQ02_PIPELINE_PROBE_RESOLUTION_FAILED",
                    error,
                )
            })?;
        exchange.insert_local(CapQ02PipelineLocal("global"))?;
        record(PipelineEvent::Handler("capq02-global-before"));
        Ok(())
    }

    async fn after_delivery(
        &self,
        _exchange: &mut crate::QueueDeliveryExchange<'_>,
        outcome: lily_queue_registry::QueueDeliveryOutcome,
    ) -> Result<(), QueueHandlerError> {
        record(PipelineEvent::Handler(match outcome {
            lily_queue_registry::QueueDeliveryOutcome::Succeeded => "capq02-global-after-success",
            lily_queue_registry::QueueDeliveryOutcome::Cancelled => "capq02-global-after-cancelled",
            lily_queue_registry::QueueDeliveryOutcome::Failed { .. } => {
                "capq02-global-after-failed"
            }
            _ => "capq02-global-after-other",
        }));
        Ok(())
    }
}

struct CapQ02ServiceMiddleware;

#[async_trait]
impl crate::QueueMiddleware for CapQ02ServiceMiddleware {
    async fn new(
        _extensions: Arc<lily_injection::Extensions>,
    ) -> Result<Self, lily_queue_registry::QueuePipelineComponentInitError> {
        Ok(Self)
    }

    async fn before_delivery(
        &self,
        exchange: &mut crate::QueueDeliveryExchange<'_>,
    ) -> Result<(), QueueHandlerError> {
        assert_eq!(
            exchange.insert_local(CapQ02PipelineLocal("service"))?,
            Some(CapQ02PipelineLocal("global"))
        );
        record(PipelineEvent::Handler("capq02-service-before"));
        Ok(())
    }

    async fn after_delivery(
        &self,
        _exchange: &mut crate::QueueDeliveryExchange<'_>,
        outcome: lily_queue_registry::QueueDeliveryOutcome,
    ) -> Result<(), QueueHandlerError> {
        record(PipelineEvent::Handler(match outcome {
            lily_queue_registry::QueueDeliveryOutcome::Succeeded => "capq02-service-after-success",
            lily_queue_registry::QueueDeliveryOutcome::Cancelled => {
                "capq02-service-after-cancelled"
            }
            lily_queue_registry::QueueDeliveryOutcome::Failed { .. } => {
                "capq02-service-after-failed"
            }
            _ => "capq02-service-after-other",
        }));
        Ok(())
    }
}

struct CapQ02HandlerMiddleware;

#[async_trait]
impl crate::QueueMiddleware for CapQ02HandlerMiddleware {
    async fn new(
        _extensions: Arc<lily_injection::Extensions>,
    ) -> Result<Self, lily_queue_registry::QueuePipelineComponentInitError> {
        Ok(Self)
    }

    async fn before_delivery(
        &self,
        exchange: &mut crate::QueueDeliveryExchange<'_>,
    ) -> Result<(), QueueHandlerError> {
        assert_eq!(
            exchange.insert_local(CapQ02PipelineLocal("handler"))?,
            Some(CapQ02PipelineLocal("service"))
        );
        record(PipelineEvent::Handler("capq02-handler-before"));
        Ok(())
    }

    async fn after_delivery(
        &self,
        _exchange: &mut crate::QueueDeliveryExchange<'_>,
        outcome: lily_queue_registry::QueueDeliveryOutcome,
    ) -> Result<(), QueueHandlerError> {
        record(PipelineEvent::Handler(match outcome {
            lily_queue_registry::QueueDeliveryOutcome::Succeeded => "capq02-handler-after-success",
            lily_queue_registry::QueueDeliveryOutcome::Cancelled => {
                "capq02-handler-after-cancelled"
            }
            lily_queue_registry::QueueDeliveryOutcome::Failed { .. } => {
                "capq02-handler-after-failed"
            }
            _ => "capq02-handler-after-other",
        }));
        Ok(())
    }
}

macro_rules! capq02_allow_guard {
    ($guard:ident, $event:literal) => {
        struct $guard;

        #[async_trait]
        impl crate::QueueGuard for $guard {
            async fn new(
                _extensions: Arc<lily_injection::Extensions>,
            ) -> Result<Self, lily_queue_registry::QueuePipelineComponentInitError> {
                Ok(Self)
            }

            async fn can_activate(
                &self,
                exchange: &mut crate::QueueDeliveryExchange<'_>,
            ) -> Result<(), QueueHandlerError> {
                assert_eq!(
                    exchange.local::<CapQ02PipelineLocal>(),
                    Some(CapQ02PipelineLocal("handler"))
                );
                record(PipelineEvent::Handler($event));
                Ok(())
            }
        }
    };
}

capq02_allow_guard!(CapQ02GlobalGuard, "capq02-global-guard");
capq02_allow_guard!(CapQ02ServiceGuard, "capq02-service-guard");
capq02_allow_guard!(CapQ02HandlerGuard, "capq02-handler-guard");

struct CapQ02RejectGuard;

#[async_trait]
impl crate::QueueGuard for CapQ02RejectGuard {
    async fn new(
        _extensions: Arc<lily_injection::Extensions>,
    ) -> Result<Self, lily_queue_registry::QueuePipelineComponentInitError> {
        Ok(Self)
    }

    async fn can_activate(
        &self,
        _exchange: &mut crate::QueueDeliveryExchange<'_>,
    ) -> Result<(), QueueHandlerError> {
        record(PipelineEvent::Handler("capq02-reject-guard"));
        Err(QueueHandlerError::permanent("CAPQ02_E2E_GUARD_REJECTED"))
    }
}

#[derive(Debug, serde::Deserialize)]
struct CapQ02Message {
    order_id: u64,
}

fn capq02_cancel_entered() -> &'static Notify {
    static ENTERED: OnceLock<Notify> = OnceLock::new();
    ENTERED.get_or_init(Notify::new)
}

#[derive(Default, Injectable)]
#[service(lifetime = "Scoped")]
struct CapQ02PipelineHandlers;

#[async_trait]
impl ServiceTrait for CapQ02PipelineHandlers {
    async fn dispose(&self) -> Result<(), InjectionError> {
        record(PipelineEvent::Cleanup("capq02-handler-service"));
        Ok(())
    }
}

#[crate::queue_service]
#[crate::middleware(CapQ02ServiceMiddleware)]
#[crate::guard(CapQ02ServiceGuard)]
impl CapQ02PipelineHandlers {
    #[crate::queue("capq02.e2e.success", version = 1, content = "json")]
    #[crate::middleware(CapQ02HandlerMiddleware)]
    #[crate::guard(CapQ02HandlerGuard)]
    async fn success(
        &self,
        Local(local): Local<CapQ02PipelineLocal>,
        Json(message): Json<CapQ02Message>,
    ) -> Result<(), QueueHandlerError> {
        assert_eq!(local, CapQ02PipelineLocal("handler"));
        assert_eq!(message.order_id, 42);
        record(PipelineEvent::Handler("capq02-action-success"));
        Ok(())
    }

    #[crate::queue("capq02.e2e.reject", version = 1, content = "json")]
    #[crate::middleware(CapQ02HandlerMiddleware)]
    #[crate::guard(CapQ02RejectGuard)]
    async fn reject(&self, Json(_message): Json<CapQ02Message>) -> Result<(), QueueHandlerError> {
        record(PipelineEvent::Handler("capq02-action-must-not-run"));
        Ok(())
    }

    #[crate::queue("capq02.e2e.cancel", version = 1, content = "json")]
    #[crate::middleware(CapQ02HandlerMiddleware)]
    #[crate::guard(CapQ02HandlerGuard)]
    async fn cancel(
        &self,
        cancellation: DeliveryCancellation,
        Json(_message): Json<CapQ02Message>,
    ) -> Result<(), QueueHandlerError> {
        record(PipelineEvent::Handler("capq02-action-cancel-entered"));
        capq02_cancel_entered().notify_one();
        cancellation.cancelled().await;
        Err(QueueHandlerError::retryable("CAPQ02_E2E_CANCELLED"))
    }
}

fn capq02_metadata(queue: &str) -> &'static QueueHandlerMetadata {
    lily_queue_registry::get_all_queue_handlers()
        .into_iter()
        .find(|metadata| metadata.queue_name == queue)
        .unwrap_or_else(|| panic!("missing CAP-Q-02 generated metadata for {queue}"))
}

async fn capq02_compile_callback(
    container: &Arc<ApplicationContainer>,
    queue: &str,
) -> RegisteredQueueHandler {
    let handlers = compile_queue_handlers(
        Arc::clone(container),
        vec![crate::pipeline::QueueHandlerCompilationInput::new(
            capq02_metadata(queue),
            None,
        )],
        &[crate::pipeline::queue_middleware_registration::<
            CapQ02GlobalMiddleware,
        >()],
        &[crate::pipeline::queue_guard_registration::<CapQ02GlobalGuard>()],
        Duration::from_secs(1),
    )
    .await
    .expect("CAP-Q-02 generated pipeline must compile");
    let handler = handlers
        .into_vec()
        .pop()
        .expect("one compiled callback must be returned");
    handler.into_registration().1
}

fn capq02_input(queue: &str, cancellation: CancellationToken) -> DeliveryInput {
    delivery_input(
        queue,
        "json",
        br#"{"order_id":42}"#.as_slice(),
        cancellation,
        Duration::from_secs(2),
    )
}

#[tokio::test]
async fn generated_pipeline_callback_is_end_to_end_ordered_and_scope_safe() {
    let _serial = QUALIFICATION_LOCK.lock().await;
    reset_state();
    let container = build_container(Arc::new(FakeQueue::default())).await;

    let success = capq02_compile_callback(&container, "capq02.e2e.success").await;
    events()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clear();
    let result =
        (success.callback)(capq02_input("capq02.e2e.success", CancellationToken::new())).await;
    record(PipelineEvent::Settlement(if result.is_ok() {
        "capq02-boundary-success"
    } else {
        "capq02-boundary-error"
    }));
    result.expect("success callback must complete");
    assert!(success.scope_tracker.reconciled());
    assert_eq!(
        events()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_slice(),
        [
            PipelineEvent::Handler("capq02-global-before"),
            PipelineEvent::Handler("capq02-service-before"),
            PipelineEvent::Handler("capq02-handler-before"),
            PipelineEvent::Handler("capq02-global-guard"),
            PipelineEvent::Handler("capq02-service-guard"),
            PipelineEvent::Handler("capq02-handler-guard"),
            PipelineEvent::Handler("capq02-action-success"),
            PipelineEvent::Handler("capq02-handler-after-success"),
            PipelineEvent::Handler("capq02-service-after-success"),
            PipelineEvent::Handler("capq02-global-after-success"),
            PipelineEvent::Cleanup("capq02-handler-service"),
            PipelineEvent::Cleanup("capq02-pipeline-probe"),
            PipelineEvent::Settlement("capq02-boundary-success"),
        ]
    );

    let rejection = capq02_compile_callback(&container, "capq02.e2e.reject").await;
    events()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clear();
    let error = (rejection.callback)(capq02_input("capq02.e2e.reject", CancellationToken::new()))
        .await
        .expect_err("guard rejection must reach the settlement boundary");
    record(PipelineEvent::Settlement("capq02-boundary-error"));
    assert_eq!(error.code(), "CAPQ02_E2E_GUARD_REJECTED");
    assert!(rejection.scope_tracker.reconciled());
    assert_eq!(
        events()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_slice(),
        [
            PipelineEvent::Handler("capq02-global-before"),
            PipelineEvent::Handler("capq02-service-before"),
            PipelineEvent::Handler("capq02-handler-before"),
            PipelineEvent::Handler("capq02-global-guard"),
            PipelineEvent::Handler("capq02-service-guard"),
            PipelineEvent::Handler("capq02-reject-guard"),
            PipelineEvent::Handler("capq02-handler-after-failed"),
            PipelineEvent::Handler("capq02-service-after-failed"),
            PipelineEvent::Handler("capq02-global-after-failed"),
            PipelineEvent::Cleanup("capq02-pipeline-probe"),
            PipelineEvent::Settlement("capq02-boundary-error"),
        ]
    );

    let cancellation = CancellationToken::new();
    let cancelled = capq02_compile_callback(&container, "capq02.e2e.cancel").await;
    events()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clear();
    let callback = Arc::clone(&cancelled.callback);
    let callback_cancellation = cancellation.clone();
    let execution = tokio::spawn(async move {
        callback(capq02_input("capq02.e2e.cancel", callback_cancellation)).await
    });
    capq02_cancel_entered().notified().await;
    cancellation.cancel();
    let error = execution
        .await
        .expect("cancelled callback task must remain joinable")
        .expect_err("cooperative cancellation must remain typed");
    record(PipelineEvent::Settlement("capq02-boundary-error"));
    assert_eq!(error.code(), "CAPQ02_E2E_CANCELLED");
    assert!(cancelled.scope_tracker.reconciled());
    assert_eq!(
        events()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_slice(),
        [
            PipelineEvent::Handler("capq02-global-before"),
            PipelineEvent::Handler("capq02-service-before"),
            PipelineEvent::Handler("capq02-handler-before"),
            PipelineEvent::Handler("capq02-global-guard"),
            PipelineEvent::Handler("capq02-service-guard"),
            PipelineEvent::Handler("capq02-handler-guard"),
            PipelineEvent::Handler("capq02-action-cancel-entered"),
            // The handler deliberately returned its own typed retryable
            // failure after observing cancellation. That primary failure is
            // preserved; a later cancellation state cannot rewrite it.
            PipelineEvent::Handler("capq02-handler-after-failed"),
            PipelineEvent::Handler("capq02-service-after-failed"),
            PipelineEvent::Handler("capq02-global-after-failed"),
            PipelineEvent::Cleanup("capq02-handler-service"),
            PipelineEvent::Cleanup("capq02-pipeline-probe"),
            PipelineEvent::Settlement("capq02-boundary-error"),
        ]
    );

    container
        .close()
        .await
        .expect("container close must succeed");
}

fn events() -> &'static Mutex<Vec<PipelineEvent>> {
    static EVENTS: OnceLock<Mutex<Vec<PipelineEvent>>> = OnceLock::new();
    EVENTS.get_or_init(|| Mutex::new(Vec::new()))
}

fn record(event: PipelineEvent) {
    events()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .push(event);
}

#[derive(Clone, Debug)]
struct JsonObservation {
    process_id: String,
    handler_id: usize,
    scoped_id: usize,
    transient_id: usize,
    payload_id: u64,
}

fn json_observations() -> &'static Mutex<Vec<JsonObservation>> {
    static OBSERVATIONS: OnceLock<Mutex<Vec<JsonObservation>>> = OnceLock::new();
    OBSERVATIONS.get_or_init(|| Mutex::new(Vec::new()))
}

fn reset_state() {
    for counter in [
        &NEXT_SCOPED_ID,
        &NEXT_TRANSIENT_ID,
        &NEXT_HANDLER_ID,
        &JSON_CALLS,
        &CODEC_CALLS,
        &REJECTING_HANDLER_CALLS,
        &SCOPED_PROBE_DISPOSES,
        &TRANSIENT_PROBE_DISPOSES,
        &SCOPED_HANDLER_DISPOSES,
        &SINGLETON_HANDLER_STARTS,
        &SINGLETON_HANDLER_DISPOSES,
        &TRANSIENT_HANDLER_STARTS,
        &TRANSIENT_HANDLER_DISPOSES,
        &BLOCKING_PROBE_DISPOSES,
    ] {
        counter.store(0, Ordering::SeqCst);
    }
    events()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clear();
    json_observations()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clear();
}

trait ScopedIdentity: Send + Sync {
    fn id(&self) -> usize;
}

#[derive(Default, Injectable)]
#[service(lifetime = "Scoped", interface = dyn ScopedIdentity)]
struct ScopedProbe {
    id: usize,
}

impl ScopedIdentity for ScopedProbe {
    fn id(&self) -> usize {
        self.id
    }
}

#[async_trait]
impl ServiceTrait for ScopedProbe {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        self.id = NEXT_SCOPED_ID.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(())
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        SCOPED_PROBE_DISPOSES.fetch_add(1, Ordering::SeqCst);
        record(PipelineEvent::Cleanup("scoped-probe"));
        Ok(())
    }
}

#[derive(Default, Injectable)]
#[service(lifetime = "Transient")]
struct TransientProbe {
    id: usize,
}

#[async_trait]
impl ServiceTrait for TransientProbe {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        self.id = NEXT_TRANSIENT_ID.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(())
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        TRANSIENT_PROBE_DISPOSES.fetch_add(1, Ordering::SeqCst);
        record(PipelineEvent::Cleanup("transient-probe"));
        Ok(())
    }
}

struct PublishScopedLocal;

impl FromDeliveryParts for PublishScopedLocal {
    type Rejection = QueueHandlerError;

    async fn from_delivery_parts(
        invocation: &mut crate::DeliveryInvocation,
    ) -> Result<Self, Self::Rejection> {
        let scoped = invocation
            .extensions()
            .get_service::<ScopedProbe>(None)
            .await
            .map_err(|error| {
                QueueHandlerError::retryable_with_source(
                    "QUEUE_QUALIFICATION_SCOPED_RESOLUTION_FAILED",
                    error,
                )
            })?;
        invocation.insert_local(Arc::clone(&scoped))?;
        Ok(Self)
    }
}

struct RejectParts;

impl FromDeliveryParts for RejectParts {
    type Rejection = QueueHandlerError;

    fn from_delivery_parts(
        _invocation: &mut crate::DeliveryInvocation,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        std::future::ready(Err(QueueHandlerError::permanent(
            "QUEUE_QUALIFICATION_PARTS_REJECTED",
        )))
    }
}

struct MissingDependency;

#[derive(Debug, serde::Deserialize)]
struct JsonMessage {
    id: u64,
}

struct CustomPayload(Vec<u8>);

impl FromDelivery for CustomPayload {
    const PAYLOAD_KIND: QueuePayloadKind = QueuePayloadKind::Custom;
    type Rejection = QueueHandlerError;

    fn from_delivery(
        mut input: crate::DeliveryPayloadInput<'_>,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        let result = input.take_body().and_then(|body| {
            if body.first() == Some(&0xCA) {
                Ok(Self(body.to_vec()))
            } else {
                Err(QueueHandlerError::permanent(
                    "QUEUE_QUALIFICATION_CUSTOM_INVALID",
                ))
            }
        });
        std::future::ready(result)
    }
}

#[derive(Default, Injectable)]
#[service(lifetime = "Scoped")]
struct TypedHandlers {
    instance_id: usize,
}

#[async_trait]
impl ServiceTrait for TypedHandlers {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        self.instance_id = NEXT_HANDLER_ID.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(())
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        SCOPED_HANDLER_DISPOSES.fetch_add(1, Ordering::SeqCst);
        record(PipelineEvent::Cleanup("scoped-handler"));
        Ok(())
    }
}

#[crate::queue_service]
impl TypedHandlers {
    #[allow(clippy::too_many_arguments)]
    #[crate::queue("capq01d.json", version = 1, content = "json")]
    async fn json(
        &self,
        context: DeliveryContext,
        headers: DeliveryHeaders,
        properties: DeliveryProperties,
        event_id: EventId,
        schema: SchemaVersion,
        content: ContentKind,
        retry: RetryCount,
        redelivered: Redelivered,
        cancellation: DeliveryCancellation,
        deadline: DeliveryDeadline,
        _published: PublishScopedLocal,
        Local(local): Local<Arc<ScopedProbe>>,
        Service(concrete): Service<ScopedProbe>,
        Service(interface): Service<dyn ScopedIdentity>,
        Service(transient): Service<TransientProbe>,
        Json(payload): Json<JsonMessage>,
    ) -> Result<(), QueueHandlerError> {
        assert_eq!(context.queue(), "capq01d.json");
        assert_eq!(event_id, context.event_id());
        assert_eq!(schema.into_inner(), 1);
        assert_eq!(content.as_str(), "json");
        assert_eq!(retry.into_inner(), 0);
        assert!(!redelivered.into_inner());
        assert!(!cancellation.is_cancelled());
        assert!(deadline.remaining() > Duration::ZERO);
        assert_eq!(headers.len(), 1);
        assert_eq!(properties.correlation_id(), Some("capq01d-correlation"));
        assert!(Arc::ptr_eq(&local, &concrete));
        assert_eq!(concrete.id, interface.id());

        JSON_CALLS.fetch_add(1, Ordering::SeqCst);
        let process_id = ProcessContext::current()
            .expect("handler must run inside the delivery scope")
            .process_id_string();
        json_observations()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(JsonObservation {
                process_id,
                handler_id: self.instance_id,
                scoped_id: concrete.id,
                transient_id: transient.id,
                payload_id: payload.id,
            });
        record(PipelineEvent::Handler("json"));
        Ok(())
    }

    #[crate::queue("capq01d.text", version = 1, content = "text")]
    async fn text(&self, TextPayload(payload): TextPayload) -> Result<(), QueueHandlerError> {
        assert_eq!(payload, "hello");
        CODEC_CALLS.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    #[crate::queue("capq01d.binary", version = 1, content = "binary")]
    async fn binary(&self, payload: BinaryPayload) -> Result<(), QueueHandlerError> {
        assert_eq!(payload.as_bytes(), [0, 1, 2, 3]);
        CODEC_CALLS.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    #[crate::queue("capq01d.raw", version = 1, content = "raw")]
    async fn raw(&self, payload: RawDelivery) -> Result<(), QueueHandlerError> {
        assert_eq!(payload.body(), b"raw-body");
        assert_eq!(payload.context().queue(), "capq01d.raw");
        CODEC_CALLS.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    #[crate::queue("capq01d.custom", version = 1, content = "custom")]
    async fn custom(&self, payload: CustomPayload) -> Result<(), QueueHandlerError> {
        assert_eq!(payload.0, [0xCA, 0xFE]);
        CODEC_CALLS.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    #[crate::queue("capq01d.parts-reject", version = 1, content = "none")]
    async fn parts_reject(&self, _reject: RejectParts) -> Result<(), QueueHandlerError> {
        REJECTING_HANDLER_CALLS.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    #[crate::queue("capq01d.di-reject", version = 1, content = "none")]
    async fn di_reject(
        &self,
        _missing: Service<MissingDependency>,
    ) -> Result<(), QueueHandlerError> {
        REJECTING_HANDLER_CALLS.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    #[crate::queue("capq01d.success", version = 1, content = "none")]
    async fn success(&self) -> Result<(), QueueHandlerError> {
        record(PipelineEvent::Handler("success"));
        Ok(())
    }

    #[crate::queue("capq01d.typed-error", version = 1, content = "none")]
    async fn typed_error(&self) -> Result<(), QueueHandlerError> {
        record(PipelineEvent::Handler("typed-error"));
        Err(QueueHandlerError::permanent(
            "QUEUE_QUALIFICATION_TYPED_ERROR",
        ))
    }

    #[crate::queue("capq01d.panic", version = 1, content = "none")]
    async fn panic(&self) -> Result<(), QueueHandlerError> {
        record(PipelineEvent::Handler("panic"));
        panic!("qualification panic payload must not escape")
    }

    #[crate::queue("capq01d.timeout", version = 1, content = "none")]
    async fn timeout(&self) -> Result<(), QueueHandlerError> {
        record(PipelineEvent::Handler("timeout"));
        timeout_handler_entered().notify_waiters();
        std::future::pending::<()>().await;
        Ok(())
    }

    #[crate::queue("capq01d.cancel", version = 1, content = "none")]
    async fn cancel(&self, cancellation: DeliveryCancellation) -> Result<(), QueueHandlerError> {
        record(PipelineEvent::Handler("cancel"));
        cancellation.cancelled().await;
        Err(QueueHandlerError::retryable(
            "QUEUE_QUALIFICATION_CANCELLED",
        ))
    }

    #[crate::queue("capq04.versioned", version = 1, content = "json")]
    async fn version_one(&self, Json(payload): Json<JsonMessage>) -> Result<(), QueueHandlerError> {
        assert_eq!(payload.id, 1);
        VERSION_ONE_CALLS.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    #[crate::queue("capq04.versioned", version = 2, content = "json")]
    async fn version_two_json(
        &self,
        Json(payload): Json<JsonMessage>,
    ) -> Result<(), QueueHandlerError> {
        assert_eq!(payload.id, 2);
        VERSION_TWO_JSON_CALLS.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    #[crate::queue("capq04.versioned", version = 2, content = "binary")]
    async fn version_two_binary(&self, payload: BinaryPayload) -> Result<(), QueueHandlerError> {
        assert_eq!(payload.as_bytes(), [2, 0, 2, 6]);
        VERSION_TWO_BINARY_CALLS.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
struct SingletonHandler;

#[async_trait]
impl ServiceTrait for SingletonHandler {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        SINGLETON_HANDLER_STARTS.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        SINGLETON_HANDLER_DISPOSES.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[crate::queue_service]
impl SingletonHandler {
    #[crate::queue("capq01d.singleton-owner", version = 1, content = "none")]
    async fn run(&self) -> Result<(), QueueHandlerError> {
        Ok(())
    }
}

#[derive(Default, Injectable)]
#[service(lifetime = "Transient")]
struct TransientHandler;

#[async_trait]
impl ServiceTrait for TransientHandler {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        TRANSIENT_HANDLER_STARTS.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        TRANSIENT_HANDLER_DISPOSES.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[crate::queue_service]
impl TransientHandler {
    #[crate::queue("capq01d.transient-owner", version = 1, content = "none")]
    async fn run(&self) -> Result<(), QueueHandlerError> {
        Ok(())
    }
}

#[derive(Clone)]
struct BlockingSignals {
    handler_entered: Arc<Notify>,
    cleanup_entered: Arc<Notify>,
    cleanup_release: Arc<Notify>,
    cleanup_fails: bool,
}

fn blocking_signals() -> &'static Mutex<Option<BlockingSignals>> {
    static SIGNALS: OnceLock<Mutex<Option<BlockingSignals>>> = OnceLock::new();
    SIGNALS.get_or_init(|| Mutex::new(None))
}

fn current_blocking_signals() -> BlockingSignals {
    blocking_signals()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
        .expect("blocking signals must be installed")
}

#[derive(Default, Injectable)]
#[service(lifetime = "Scoped")]
struct BlockingProbe;

#[async_trait]
impl ServiceTrait for BlockingProbe {
    async fn dispose(&self) -> Result<(), InjectionError> {
        let signals = current_blocking_signals();
        BLOCKING_PROBE_DISPOSES.fetch_add(1, Ordering::SeqCst);
        signals.cleanup_entered.notify_one();
        signals.cleanup_release.notified().await;
        if signals.cleanup_fails {
            return Err(InjectionError::General("retained disposer failure".into()));
        }
        Ok(())
    }
}

#[derive(Default, Injectable)]
#[service(lifetime = "Scoped")]
struct BlockingHandler;

impl ServiceTrait for BlockingHandler {}

#[crate::queue_service]
impl BlockingHandler {
    #[crate::queue("capq01d.blocking", version = 1, content = "none")]
    async fn run(&self, _probe: Service<BlockingProbe>) -> Result<(), QueueHandlerError> {
        current_blocking_signals().handler_entered.notify_one();
        std::future::pending::<()>().await;
        Ok(())
    }

    #[crate::queue("lifecycle.scope-close", version = 1, content = "none")]
    async fn close_scope(&self, _probe: Service<BlockingProbe>) -> Result<(), QueueHandlerError> {
        Ok(())
    }
}

struct StoredRegistration {
    exchange_name: String,
    queue_name: String,
    handler: RegisteredQueueHandler,
}

#[derive(Default)]
struct FakeQueue {
    registrations: Mutex<Vec<StoredRegistration>>,
}

impl FakeQueue {
    fn take(&self, queue_name: &str) -> StoredRegistration {
        let mut registrations = self
            .registrations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let index = registrations
            .iter()
            .position(|entry| entry.queue_name == queue_name)
            .expect("compiled handler must reach the fake provider");
        registrations.remove(index)
    }
}

#[async_trait]
impl Queue for FakeQueue {
    async fn create_queue(
        &self,
        exchange_name: &str,
        queue: &str,
        handler: RegisteredQueueHandler,
    ) -> Result<(), MessageBrokerError> {
        self.registrations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(StoredRegistration {
                exchange_name: exchange_name.to_string(),
                queue_name: queue.to_string(),
                handler,
            });
        Ok(())
    }

    async fn start_async(&self, _ct: CancellationToken) -> Result<(), MessageBrokerError> {
        Ok(())
    }

    async fn stop_async(&self) -> Result<(), MessageBrokerError> {
        Ok(())
    }

    async fn stop_admission_async(&self) -> Result<(), MessageBrokerError> {
        Ok(())
    }

    async fn drain_async(&self) -> Result<(), MessageBrokerError> {
        Ok(())
    }

    async fn force_drain_async(&self) -> Result<(), MessageBrokerError> {
        Ok(())
    }

    async fn close_async(&self) -> Result<(), MessageBrokerError> {
        Ok(())
    }

    async fn wait_for_shutdown(&self) -> Result<(), MessageBrokerError> {
        Ok(())
    }

    fn drain_reconciled(&self) -> bool {
        self.registrations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .all(|entry| entry.handler.scope_tracker.reconciled())
    }

    fn delivery_terminal_snapshot(&self) -> crate::DeliveryTerminalSnapshot {
        crate::DeliveryTerminalSnapshot::default()
    }

    fn delivery_terminal_observations(&self) -> crate::DeliveryTerminalObservationsSnapshot {
        crate::DeliveryTerminalObservationsSnapshot::default()
    }
}

async fn build_container(fake: Arc<FakeQueue>) -> Arc<ApplicationContainer> {
    let config_path = format!("/tmp/lily-capq01d-{}.toml", Uuid::new_v4());
    let config = ConfigService::development(&config_path);
    let queue_service = QueueService {
        provider: Some(fake),
        config_service: Arc::new(ConfigService::development(config_path)),
    };
    Arc::new(
        ApplicationContainer::builder()
            .seed_singleton(config)
            .seed_singleton(queue_service)
            .build()
            .await
            .expect("qualification container must build without transport I/O"),
    )
}

fn metadata(queue_name: &str) -> &'static QueueHandlerMetadata {
    lily_queue_registry::get_all_queue_handlers()
        .into_iter()
        .find(|metadata| metadata.queue_name == queue_name)
        .unwrap_or_else(|| panic!("missing generated metadata for {queue_name}"))
}

fn metadata_contract(
    queue_name: &str,
    schema_version: u16,
    content_kind: &str,
) -> &'static QueueHandlerMetadata {
    lily_queue_registry::get_all_queue_handlers()
        .into_iter()
        .find(|metadata| {
            metadata.queue_name == queue_name
                && metadata.schema_version == schema_version
                && metadata.content_kind == content_kind
        })
        .unwrap_or_else(|| {
            panic!("missing generated metadata for {queue_name} v{schema_version}/{content_kind}")
        })
}

fn compile(container: &Arc<ApplicationContainer>, queue_name: &str) -> CompiledQueueHandler {
    CompiledQueueHandler::try_new(
        Arc::clone(container),
        metadata(queue_name),
        CompiledQueuePipeline::default(),
        None,
    )
    .expect("generated handler must compile")
}

fn delivery_input(
    queue_name: &str,
    content_kind: &str,
    body: impl Into<Bytes>,
    cancellation: CancellationToken,
    timeout: Duration,
) -> DeliveryInput {
    delivery_input_version(queue_name, 1, content_kind, body, cancellation, timeout)
}

fn delivery_input_version(
    queue_name: &str,
    schema_version: u16,
    content_kind: &str,
    body: impl Into<Bytes>,
    cancellation: CancellationToken,
    timeout: Duration,
) -> DeliveryInput {
    let mut header_entries = BTreeMap::new();
    header_entries.insert(
        "x-capq01d".to_string(),
        DeliveryHeaderValue::Text(Arc::from("present")),
    );
    DeliveryInput {
        body: body.into(),
        context: DeliveryContext {
            event_id: EventId(Uuid::new_v4()),
            schema_version: SchemaVersion::try_new(schema_version).unwrap(),
            content_kind: ContentKind::try_new(content_kind).unwrap(),
            retry_count: RetryCount(0),
            redelivered: Redelivered(false),
            queue: Arc::from(queue_name),
            exchange: Arc::from("capq01d.worker"),
            routing_key: Arc::from(queue_name),
        },
        headers: DeliveryHeaders::from_entries(header_entries),
        properties: DeliveryProperties {
            correlation_id: Some(Arc::from("capq01d-correlation")),
            ..DeliveryProperties::default()
        },
        cancellation: cancellation.into(),
        deadline: tokio::time::Instant::now() + timeout,
    }
}

fn none_input(queue_name: &str) -> DeliveryInput {
    delivery_input(
        queue_name,
        "none",
        Bytes::new(),
        CancellationToken::new(),
        Duration::from_secs(2),
    )
}

struct RecordingPort {
    fail_ack: bool,
    calls: usize,
}

#[async_trait]
impl SettlementPort for RecordingPort {
    async fn handoff(&mut self, plan: HandoffPlan) -> Result<HandoffReceipt, MessageBrokerError> {
        self.calls += 1;
        record(PipelineEvent::Settlement("handoff"));
        Ok(match plan.destination() {
            HandoffDestination::Retry => HandoffReceipt::RetryConfirmed,
            HandoffDestination::DeadLetter => HandoffReceipt::DeadLetterConfirmed,
        })
    }

    async fn ack(&mut self) -> Result<bool, MessageBrokerError> {
        self.calls += 1;
        record(PipelineEvent::Settlement("ack"));
        if self.fail_ack {
            Err(configuration_error("qualification ACK failure"))
        } else {
            Ok(true)
        }
    }

    async fn nack_requeue(&mut self) -> Result<bool, MessageBrokerError> {
        self.calls += 1;
        record(PipelineEvent::Settlement("nack-requeue"));
        Ok(true)
    }

    async fn nack_dead_letter(&mut self) -> Result<bool, MessageBrokerError> {
        self.calls += 1;
        record(PipelineEvent::Settlement("nack-dead-letter"));
        Ok(true)
    }
}

struct RecordingObserver;

impl SettlementObserver for RecordingObserver {
    fn terminal(&mut self, _terminal: SettlementTerminal) -> Result<(), SettlementObserverFailure> {
        Ok(())
    }
}

fn execution_outcome(result: &Result<(), QueueHandlerError>) -> ExecutionOutcome {
    match result {
        Ok(()) => ExecutionOutcome::Success,
        Err(error) => ExecutionOutcome::Failure {
            class: match error.class() {
                QueueHandlerFailureClass::Retryable => FailureClass::Retryable,
                QueueHandlerFailureClass::Permanent => FailureClass::Permanent,
            },
            code: error.code(),
        },
    }
}

async fn settle(
    result: &Result<(), QueueHandlerError>,
    fail_ack: bool,
) -> (SettlementTerminal, usize, bool) {
    let mut port = RecordingPort { fail_ack, calls: 0 };
    let mut observer = RecordingObserver;
    let report = materialize_delivery(
        &mut port,
        &SettlementAuthority::new(),
        &mut observer,
        execution_outcome(result),
        0,
        0,
        &CancellationToken::new(),
        Duration::from_secs(1),
        Duration::from_secs(1),
        &crate::shutdown_budget::QueueShutdownBudget::default(),
    )
    .await;
    (report.terminal(), port.calls, report.failure().is_some())
}

#[tokio::test]
async fn real_registry_registration_and_all_parts_preserve_delivery_scope_identity() {
    let _serial = QUALIFICATION_LOCK.lock().await;
    reset_state();
    let fake = Arc::new(FakeQueue::default());
    let container = build_container(Arc::clone(&fake)).await;
    assert_eq!(SINGLETON_HANDLER_STARTS.load(Ordering::SeqCst), 1);

    let compiled = compile(&container, "capq01d.json");
    assert_eq!(metadata("capq01d.json").service_type_name, "TypedHandlers");
    assert_eq!(metadata("capq01d.json").method_name, "json");
    let queue_service = container
        .services()
        .get_service::<QueueService>(None)
        .await
        .expect("seeded queue service must resolve");
    queue_service
        .register_compiled_handler("capq01d.worker", compiled)
        .await
        .expect("compiled handler must register");
    let registration = fake.take("capq01d.json");
    assert_eq!(registration.exchange_name, "capq01d.worker");
    assert_eq!(registration.queue_name, metadata("capq01d.json").queue_name);

    let callback = Arc::clone(&registration.handler.callback);
    let first = callback(delivery_input(
        "capq01d.json",
        "json",
        br#"{"id":1}"#.as_slice(),
        CancellationToken::new(),
        Duration::from_secs(2),
    ));
    first.await.expect("first typed delivery must succeed");

    let left = callback(delivery_input(
        "capq01d.json",
        "json",
        br#"{"id":2}"#.as_slice(),
        CancellationToken::new(),
        Duration::from_secs(2),
    ));
    let right = callback(delivery_input(
        "capq01d.json",
        "json",
        br#"{"id":3}"#.as_slice(),
        CancellationToken::new(),
        Duration::from_secs(2),
    ));
    let (left, right) = tokio::join!(left, right);
    left.expect("left concurrent delivery must succeed");
    right.expect("right concurrent delivery must succeed");

    let observations = json_observations()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    assert_eq!(observations.len(), 3);
    assert_eq!(JSON_CALLS.load(Ordering::SeqCst), 3);
    assert_eq!(SCOPED_PROBE_DISPOSES.load(Ordering::SeqCst), 3);
    assert_eq!(TRANSIENT_PROBE_DISPOSES.load(Ordering::SeqCst), 3);
    assert_eq!(SCOPED_HANDLER_DISPOSES.load(Ordering::SeqCst), 3);
    assert_eq!(
        observations
            .iter()
            .map(|entry| entry.process_id.as_str())
            .collect::<HashSet<_>>()
            .len(),
        3
    );
    assert_eq!(
        observations
            .iter()
            .map(|entry| entry.handler_id)
            .collect::<HashSet<_>>()
            .len(),
        3
    );
    assert_eq!(
        observations
            .iter()
            .map(|entry| entry.scoped_id)
            .collect::<HashSet<_>>()
            .len(),
        3
    );
    assert_eq!(
        observations
            .iter()
            .map(|entry| entry.transient_id)
            .collect::<HashSet<_>>()
            .len(),
        3
    );
    assert_eq!(
        observations
            .iter()
            .map(|entry| entry.payload_id)
            .collect::<HashSet<_>>(),
        HashSet::from([1, 2, 3])
    );
    assert_eq!(SINGLETON_HANDLER_DISPOSES.load(Ordering::SeqCst), 0);

    container
        .close()
        .await
        .expect("container close must succeed");
    assert_eq!(SINGLETON_HANDLER_DISPOSES.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn versioned_dispatch_uses_one_registration_and_exact_local_handler_selection() {
    let _serial = QUALIFICATION_LOCK.lock().await;
    VERSION_ONE_CALLS.store(0, Ordering::SeqCst);
    VERSION_TWO_JSON_CALLS.store(0, Ordering::SeqCst);
    VERSION_TWO_BINARY_CALLS.store(0, Ordering::SeqCst);

    let fake = Arc::new(FakeQueue::default());
    let container = build_container(Arc::clone(&fake)).await;
    let inputs = [(1, "json"), (2, "json"), (2, "binary")]
        .into_iter()
        .map(|(version, content)| {
            QueueHandlerCompilationInput::new(
                metadata_contract("capq04.versioned", version, content),
                None,
            )
        })
        .collect();
    let handlers = compile_queue_handlers(
        Arc::clone(&container),
        inputs,
        &[],
        &[],
        Duration::from_secs(1),
    )
    .await
    .expect("all versioned handlers must compile atomically");
    let dispatch = CompiledQueueDispatch::try_new(handlers.into_vec())
        .expect("distinct version/content keys must form one dispatch table");
    assert_eq!(dispatch.handler_count(), 3);

    let queue_service = container
        .services()
        .get_service::<QueueService>(None)
        .await
        .expect("seeded queue service must resolve");
    queue_service
        .register_compiled_dispatch("capq04.worker", dispatch)
        .await
        .expect("one versioned dispatch must register");
    let registration = fake.take("capq04.versioned");
    assert_eq!(registration.exchange_name, "capq04.worker");
    assert_eq!(registration.queue_name, "capq04.versioned");
    let callback = Arc::clone(&registration.handler.callback);

    callback(delivery_input_version(
        "capq04.versioned",
        1,
        "json",
        br#"{"id":1}"#.as_slice(),
        CancellationToken::new(),
        Duration::from_secs(2),
    ))
    .await
    .expect("V1 JSON must select only the V1 handler");
    callback(delivery_input_version(
        "capq04.versioned",
        2,
        "json",
        br#"{"id":2}"#.as_slice(),
        CancellationToken::new(),
        Duration::from_secs(2),
    ))
    .await
    .expect("V2 JSON must select only the V2 JSON handler");
    callback(delivery_input_version(
        "capq04.versioned",
        2,
        "binary",
        [2_u8, 0, 2, 6].as_slice(),
        CancellationToken::new(),
        Duration::from_secs(2),
    ))
    .await
    .expect("V2 binary must select only the V2 binary handler");

    assert_eq!(VERSION_ONE_CALLS.load(Ordering::SeqCst), 1);
    assert_eq!(VERSION_TWO_JSON_CALLS.load(Ordering::SeqCst), 1);
    assert_eq!(VERSION_TWO_BINARY_CALLS.load(Ordering::SeqCst), 1);

    let unsupported_version = callback(delivery_input_version(
        "capq04.versioned",
        3,
        "json",
        br#"{"id":3}"#.as_slice(),
        CancellationToken::new(),
        Duration::from_secs(2),
    ))
    .await
    .expect_err("unknown version must fail before application execution");
    assert_eq!(
        unsupported_version.code(),
        "QUEUE_SCHEMA_VERSION_UNSUPPORTED"
    );

    let unsupported_content = callback(delivery_input_version(
        "capq04.versioned",
        2,
        "protobuf",
        [0_u8].as_slice(),
        CancellationToken::new(),
        Duration::from_secs(2),
    ))
    .await
    .expect_err("unknown content kind must fail before application execution");
    assert_eq!(unsupported_content.code(), "QUEUE_CONTENT_KIND_UNSUPPORTED");
    assert_eq!(VERSION_ONE_CALLS.load(Ordering::SeqCst), 1);
    assert_eq!(VERSION_TWO_JSON_CALLS.load(Ordering::SeqCst), 1);
    assert_eq!(VERSION_TWO_BINARY_CALLS.load(Ordering::SeqCst), 1);
    assert!(registration.handler.scope_tracker.reconciled());

    container
        .close()
        .await
        .expect("container close must succeed");
}

#[tokio::test]
async fn dispatch_table_rejects_duplicate_contracts_and_mixed_queues() {
    let _serial = QUALIFICATION_LOCK.lock().await;
    let container = build_container(Arc::new(FakeQueue::default())).await;

    let duplicate = vec![
        CompiledQueueHandler::try_new(
            Arc::clone(&container),
            metadata_contract("capq04.versioned", 1, "json"),
            CompiledQueuePipeline::default(),
            None,
        )
        .unwrap(),
        CompiledQueueHandler::try_new(
            Arc::clone(&container),
            metadata_contract("capq04.versioned", 1, "json"),
            CompiledQueuePipeline::default(),
            None,
        )
        .unwrap(),
    ];
    assert_eq!(
        CompiledQueueDispatch::try_new(duplicate)
            .err()
            .expect("duplicate dispatch key must fail")
            .error_code(),
        "BROKER_CONFIGURATION"
    );

    let mixed = vec![
        CompiledQueueHandler::try_new(
            Arc::clone(&container),
            metadata_contract("capq04.versioned", 1, "json"),
            CompiledQueuePipeline::default(),
            None,
        )
        .unwrap(),
        compile(&container, "capq01d.text"),
    ];
    assert_eq!(
        CompiledQueueDispatch::try_new(mixed)
            .err()
            .expect("mixed physical queues must fail")
            .error_code(),
        "BROKER_CONFIGURATION"
    );

    container
        .close()
        .await
        .expect("container close must succeed");
}

#[tokio::test]
async fn codec_and_pre_handler_failures_are_typed_and_never_call_the_method() {
    let _serial = QUALIFICATION_LOCK.lock().await;
    reset_state();
    let container = build_container(Arc::new(FakeQueue::default())).await;

    for (queue, kind, body) in [
        ("capq01d.text", "text", b"hello".as_slice()),
        ("capq01d.binary", "binary", [0, 1, 2, 3].as_slice()),
        ("capq01d.raw", "raw", b"raw-body".as_slice()),
        ("capq01d.custom", "custom", [0xCA, 0xFE].as_slice()),
    ] {
        compile(&container, queue)
            .execute(delivery_input(
                queue,
                kind,
                body,
                CancellationToken::new(),
                Duration::from_secs(2),
            ))
            .await
            .unwrap_or_else(|error| panic!("{queue} failed: {error}"));
    }
    assert_eq!(CODEC_CALLS.load(Ordering::SeqCst), 4);

    let failures = [
        (
            "capq01d.json",
            "json",
            b"{".as_slice(),
            "QUEUE_JSON_INVALID",
        ),
        (
            "capq01d.text",
            "text",
            [0xFF].as_slice(),
            "QUEUE_TEXT_INVALID",
        ),
        (
            "capq01d.custom",
            "custom",
            [0x00].as_slice(),
            "QUEUE_QUALIFICATION_CUSTOM_INVALID",
        ),
        (
            "capq01d.parts-reject",
            "none",
            b"".as_slice(),
            "QUEUE_QUALIFICATION_PARTS_REJECTED",
        ),
        (
            "capq01d.di-reject",
            "none",
            b"".as_slice(),
            "QUEUE_SERVICE_RESOLUTION_FAILED",
        ),
    ];
    for (queue, kind, body, expected) in failures {
        let error = compile(&container, queue)
            .execute(delivery_input(
                queue,
                kind,
                body,
                CancellationToken::new(),
                Duration::from_secs(2),
            ))
            .await
            .expect_err("pre-handler failure must be typed");
        assert_eq!(error.code(), expected, "{queue}");
    }
    assert_eq!(CODEC_CALLS.load(Ordering::SeqCst), 4);
    assert_eq!(JSON_CALLS.load(Ordering::SeqCst), 0);
    assert_eq!(REJECTING_HANDLER_CALLS.load(Ordering::SeqCst), 0);

    let wrong_content = compile(&container, "capq01d.json")
        .execute(delivery_input(
            "capq01d.json",
            "text",
            br#"{"id":9}"#.as_slice(),
            CancellationToken::new(),
            Duration::from_secs(2),
        ))
        .await
        .expect_err("runtime content mismatch must fail before scope creation");
    assert_eq!(wrong_content.code(), "QUEUE_CONTENT_KIND_MISMATCH");

    let real = metadata("capq01d.json");
    let mut mismatch = real.clone();
    mismatch.content_kind = "text";
    let mismatch = Box::leak(Box::new(mismatch));
    let Err(error) = CompiledQueueHandler::try_new(
        Arc::clone(&container),
        mismatch,
        CompiledQueuePipeline::default(),
        None,
    ) else {
        panic!("semantic payload mismatch must fail at startup");
    };
    assert_eq!(error.error_code(), "BROKER_CONFIGURATION");

    fn unused_handler<'a>(
        _service: Arc<dyn Any + Send + Sync>,
        _invocation: &'a mut (dyn Any + Send),
    ) -> Pin<Box<dyn Future<Output = Result<(), QueueHandlerError>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
    let unregistered = Box::leak(Box::new(QueueHandlerMetadata {
        service_type_id: TypeId::of::<MissingDependency>(),
        service_type_name: "MissingDependency",
        component_kind: None,
        queue_name: "capq01d.unregistered",
        method_name: "run",
        handler_name: "qualification::MissingDependency::run",
        schema_version: 1,
        content_kind: "none",
        delivery_guarantee: lily_queue_registry::DeliveryGuarantee::AtLeastOnce,
        input_contract: QueueHandlerInputContract::new(QueuePayloadKind::None, None),
        #[cfg(feature = "asyncapi")]
        asyncapi: lily_queue_registry::QueueAsyncApiRegistration::unspecified(),
        service_middlewares: Vec::new(),
        service_guards: Vec::new(),
        handler_middlewares: Vec::new(),
        handler_guards: Vec::new(),
        handler_fn: unused_handler,
    }));
    let Err(error) = CompiledQueueHandler::try_new(
        Arc::clone(&container),
        unregistered,
        CompiledQueuePipeline::default(),
        None,
    ) else {
        panic!("missing handler service must fail before broker registration");
    };
    assert_eq!(error.error_code(), "BROKER_CONFIGURATION");

    let mut invocation =
        crate::DeliveryInvocation::new(container.services(), none_input("capq01d.typed-error"));
    let error =
        (metadata("capq01d.typed-error").handler_fn)(Arc::new(MissingDependency), &mut invocation)
            .await
            .expect_err("wrong generated service wrapper must fail closed");
    assert_eq!(error.code(), "QUEUE_HANDLER_SERVICE_TYPE_MISMATCH");
    let mut wrong_invocation = 42usize;
    let error = (metadata("capq01d.typed-error").handler_fn)(
        Arc::new(TypedHandlers::default()),
        &mut wrong_invocation,
    )
    .await
    .expect_err("wrong generated invocation wrapper must fail closed");
    assert_eq!(error.code(), "QUEUE_HANDLER_INVOCATION_TYPE_MISMATCH");

    container
        .close()
        .await
        .expect("container close must succeed");
}

#[tokio::test]
async fn every_execution_exit_cleans_scope_before_exact_settlement_materialization() {
    let _serial = QUALIFICATION_LOCK.lock().await;
    reset_state();
    let container = build_container(Arc::new(FakeQueue::default())).await;

    let cases = [
        ("capq01d.success", None, SettlementTerminal::AckedSuccess),
        (
            "capq01d.typed-error",
            Some("QUEUE_QUALIFICATION_TYPED_ERROR"),
            SettlementTerminal::AckedDeadLetter,
        ),
        (
            "capq01d.panic",
            Some("QUEUE_HANDLER_PANICKED"),
            SettlementTerminal::AckedDeadLetter,
        ),
        (
            "capq01d.timeout",
            Some("QUEUE_DELIVERY_EXECUTION_TIMED_OUT"),
            SettlementTerminal::AckedDeadLetter,
        ),
    ];

    for (queue, expected_error, expected_terminal) in cases {
        events()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
        let timeout_case = queue == "capq01d.timeout";
        let timeout = if timeout_case {
            Duration::from_secs(1)
        } else {
            Duration::from_secs(2)
        };
        let compiled = compile(&container, queue);
        let tracker = Arc::clone(&compiled.0.scope_tracker);
        let execution = compiled.execute(delivery_input(
            queue,
            "none",
            Bytes::new(),
            CancellationToken::new(),
            timeout,
        ));
        tokio::pin!(execution);
        if timeout_case {
            let entered = timeout_handler_entered().notified();
            tokio::pin!(entered);
            entered.as_mut().enable();
            tokio::select! {
                biased;
                result = &mut execution => {
                    panic!(
                        "timeout execution ended before the scoped handler entered: {:?}",
                        result.err().map(|error| error.code())
                    );
                }
                () = &mut entered => {}
            }
        }
        let result = execution.await;
        assert_eq!(
            result.as_ref().err().map(QueueHandlerError::code),
            expected_error
        );
        assert!(
            tracker.reconciled(),
            "{queue} scope must reconcile before outcome"
        );
        record(PipelineEvent::ScopeReconciled);
        let (terminal, calls, failure) = settle(&result, false).await;
        assert_eq!(terminal, expected_terminal);
        assert!(!failure);
        assert_eq!(calls, if result.is_ok() { 1 } else { 2 });

        let observed = events()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let cleanup = observed
            .iter()
            .rposition(|event| matches!(event, PipelineEvent::ScopeReconciled))
            .unwrap_or_else(|| panic!("{queue} delivery scope did not clean up: {observed:?}"));
        let settlement = observed
            .iter()
            .position(|event| matches!(event, PipelineEvent::Settlement(_)))
            .expect("delivery must reach settlement");
        assert!(cleanup < settlement, "{queue}: {observed:?}");
    }
    assert_eq!(
        SCOPED_HANDLER_DISPOSES.load(Ordering::SeqCst),
        4,
        "success, typed error, panic and timeout must each dispose their delivery-scoped owner"
    );

    events()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clear();
    let cancellation = CancellationToken::new();
    let callback = compile(&container, "capq01d.cancel");
    let cancellation_tracker = Arc::clone(&callback.0.scope_tracker);
    let task_cancellation = cancellation.clone();
    let task = tokio::spawn(async move {
        callback
            .execute(delivery_input(
                "capq01d.cancel",
                "none",
                Bytes::new(),
                task_cancellation,
                Duration::from_secs(2),
            ))
            .await
    });
    tokio::task::yield_now().await;
    cancellation.cancel();
    let result = task.await.unwrap();
    assert_eq!(
        result.as_ref().unwrap_err().code(),
        "QUEUE_QUALIFICATION_CANCELLED"
    );
    assert!(cancellation_tracker.reconciled());
    record(PipelineEvent::ScopeReconciled);
    let (terminal, calls, failure) = settle(&result, false).await;
    assert_eq!(terminal, SettlementTerminal::AckedDeadLetter);
    assert_eq!(calls, 2);
    assert!(!failure);

    events()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clear();
    let compiled = compile(&container, "capq01d.success");
    let settlement_failure_tracker = Arc::clone(&compiled.0.scope_tracker);
    let result = compiled.execute(none_input("capq01d.success")).await;
    assert!(settlement_failure_tracker.reconciled());
    record(PipelineEvent::ScopeReconciled);
    let (terminal, calls, failure) = settle(&result, true).await;
    assert_eq!(terminal, SettlementTerminal::Unresolved);
    assert_eq!(calls, 1);
    assert!(failure);
    let observed = events()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    let cleanup = observed
        .iter()
        .rposition(|event| matches!(event, PipelineEvent::ScopeReconciled))
        .unwrap();
    let settlement = observed
        .iter()
        .position(|event| matches!(event, PipelineEvent::Settlement(_)))
        .unwrap();
    assert!(cleanup < settlement);

    container
        .close()
        .await
        .expect("all delivery scopes were reconciled before settlement");
    assert_eq!(container.active_scope_count(), 0);
}

#[tokio::test]
async fn queue_owner_lifetimes_and_external_container_ownership_are_preserved() {
    let _serial = QUALIFICATION_LOCK.lock().await;
    reset_state();
    let container = build_container(Arc::new(FakeQueue::default())).await;

    for _ in 0..2 {
        compile(&container, "capq01d.singleton-owner")
            .execute(none_input("capq01d.singleton-owner"))
            .await
            .expect("singleton handler delivery must succeed");
        compile(&container, "capq01d.transient-owner")
            .execute(none_input("capq01d.transient-owner"))
            .await
            .expect("transient handler delivery must succeed");
    }

    assert_eq!(SINGLETON_HANDLER_STARTS.load(Ordering::SeqCst), 1);
    assert_eq!(SINGLETON_HANDLER_DISPOSES.load(Ordering::SeqCst), 0);
    assert_eq!(TRANSIENT_HANDLER_STARTS.load(Ordering::SeqCst), 2);
    assert_eq!(TRANSIENT_HANDLER_DISPOSES.load(Ordering::SeqCst), 2);
    assert_eq!(container.active_scope_count(), 0);

    container
        .close()
        .await
        .expect("external application container remains its own shutdown authority");
    assert_eq!(SINGLETON_HANDLER_DISPOSES.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancellation_during_scope_close_retains_the_original_disposer_failure() {
    let _serial = QUALIFICATION_LOCK.lock().await;
    reset_state();
    let signals = BlockingSignals {
        handler_entered: Arc::new(Notify::new()),
        cleanup_entered: Arc::new(Notify::new()),
        cleanup_release: Arc::new(Notify::new()),
        cleanup_fails: true,
    };
    *blocking_signals().lock().unwrap() = Some(signals.clone());
    let container = build_container(Arc::new(FakeQueue::default())).await;
    let compiled = compile(&container, "lifecycle.scope-close");
    let tracker = compiled.0.scope_tracker.clone();
    let task = tokio::spawn(compiled.execute(none_input("lifecycle.scope-close")));
    signals.cleanup_entered.notified().await;
    assert!(
        !task.is_finished(),
        "the original close result is still pending"
    );
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(!tracker.reconciled());
    signals.cleanup_release.notify_one();
    tokio::time::timeout(Duration::from_secs(1), tracker.drain())
        .await
        .unwrap()
        .expect_err("a terminal observation must not hide the original disposer error");
    assert!(
        tracker.reconciled(),
        "failed disposal was nevertheless joined"
    );
    assert_eq!(
        BLOCKING_PROBE_DISPOSES.load(Ordering::SeqCst),
        1,
        "cancelled close cannot restart disposal"
    );
    assert_eq!(container.active_scope_count(), 0);
    let error = container
        .close()
        .await
        .expect_err("DI retains the same failed disposal");
    assert!(error.to_string().contains("retained disposer failure"));
    *blocking_signals().lock().unwrap() = None;
}

#[tokio::test]
async fn a_later_expired_root_stops_already_transferred_scope_cleanup_and_confirms_termination() {
    let _serial = QUALIFICATION_LOCK.lock().await;
    reset_state();
    let signals = BlockingSignals {
        handler_entered: Arc::new(Notify::new()),
        cleanup_entered: Arc::new(Notify::new()),
        cleanup_release: Arc::new(Notify::new()),
        cleanup_fails: false,
    };
    *blocking_signals().lock().unwrap() = Some(signals.clone());
    let container = build_container(Arc::new(FakeQueue::default())).await;
    let compiled = compile(&container, "lifecycle.scope-close");
    let tracker = compiled.0.scope_tracker.clone();
    let mut input = none_input("lifecycle.scope-close");
    input.deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let task = tokio::spawn(compiled.execute(input));
    signals.cleanup_entered.notified().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    // Start observing the retained cleanup, then publish a root which cannot
    // permit a new local budget. The disposer is never cooperatively released.
    assert!(tracker.drain().now_or_never().is_none());
    let expired = tokio::time::Instant::now() - Duration::from_secs(1);
    tracker.set_shutdown_deadlines(crate::shutdown_budget::QueueShutdownDeadlines::before(
        expired, expired,
    ));
    tokio::time::timeout(Duration::from_secs(1), tracker.drain())
        .await
        .expect("new root must shorten the original sixty-second close wait")
        .expect_err("forced scope disposal must not be reported as successful");
    assert!(
        tracker.reconciled(),
        "receipt and transferred task both prove termination"
    );
    assert_eq!(container.active_scope_count(), 0);
    assert_eq!(BLOCKING_PROBE_DISPOSES.load(Ordering::SeqCst), 1);
    container
        .close()
        .await
        .expect_err("DI must retain the forced cleanup failure");
    *blocking_signals().lock().unwrap() = None;
}

#[tokio::test]
async fn aborted_delivery_waits_for_container_owned_scope_cleanup_before_reconciliation() {
    let _serial = QUALIFICATION_LOCK.lock().await;
    reset_state();
    let signals = BlockingSignals {
        handler_entered: Arc::new(Notify::new()),
        cleanup_entered: Arc::new(Notify::new()),
        cleanup_release: Arc::new(Notify::new()),
        cleanup_fails: false,
    };
    *blocking_signals()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(signals.clone());

    let container = build_container(Arc::new(FakeQueue::default())).await;
    let compiled = compile(&container, "capq01d.blocking");
    let tracker = Arc::clone(&compiled.0.scope_tracker);
    let task = tokio::spawn(async move { compiled.execute(none_input("capq01d.blocking")).await });
    signals.handler_entered.notified().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    signals.cleanup_entered.notified().await;
    assert!(!tracker.reconciled());

    assert!(
        tokio::time::timeout(Duration::from_millis(10), tracker.drain())
            .await
            .is_err(),
        "scope reconciliation must wait for the owned disposer"
    );
    signals.cleanup_release.notify_one();
    tokio::time::timeout(Duration::from_secs(1), tracker.drain())
        .await
        .expect("tracker drain must be bounded")
        .expect("scope cleanup must reconcile");
    assert!(tracker.reconciled());
    assert_eq!(BLOCKING_PROBE_DISPOSES.load(Ordering::SeqCst), 1);
    assert_eq!(container.active_scope_count(), 0);

    container
        .close()
        .await
        .expect("container close must succeed");
    *blocking_signals()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
}

#[tokio::test]
async fn timed_out_action_waits_for_async_scope_disposal_before_settlement() {
    let _serial = QUALIFICATION_LOCK.lock().await;
    reset_state();
    let signals = BlockingSignals {
        handler_entered: Arc::new(Notify::new()),
        cleanup_entered: Arc::new(Notify::new()),
        cleanup_release: Arc::new(Notify::new()),
        cleanup_fails: false,
    };
    *blocking_signals()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(signals.clone());

    let container = build_container(Arc::new(FakeQueue::default())).await;
    let compiled = compile(&container, "capq01d.blocking");
    let tracker = Arc::clone(&compiled.0.scope_tracker);
    let task = tokio::spawn(async move {
        compiled
            .execute(delivery_input(
                "capq01d.blocking",
                "none",
                Bytes::new(),
                CancellationToken::new(),
                Duration::from_millis(500),
            ))
            .await
    });

    signals.handler_entered.notified().await;
    signals.cleanup_entered.notified().await;
    assert!(
        !task.is_finished(),
        "delivery outcome must remain owned while async scope disposal is pending"
    );
    assert!(!tracker.reconciled());
    assert!(
        !events()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .any(|event| matches!(event, PipelineEvent::Settlement(_))),
        "broker settlement must not race async scope disposal"
    );

    signals.cleanup_release.notify_one();
    let result = tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("delivery must finish after disposer release")
        .expect("delivery task must not panic");
    assert_eq!(
        result.as_ref().unwrap_err().code(),
        "QUEUE_DELIVERY_EXECUTION_TIMED_OUT"
    );
    assert!(tracker.reconciled());
    record(PipelineEvent::ScopeReconciled);

    let (terminal, calls, failure) = settle(&result, false).await;
    assert_eq!(terminal, SettlementTerminal::AckedDeadLetter);
    assert_eq!(calls, 2);
    assert!(!failure);
    let observed = events()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    let cleanup = observed
        .iter()
        .rposition(|event| matches!(event, PipelineEvent::ScopeReconciled))
        .expect("scope cleanup must be observed");
    let settlement = observed
        .iter()
        .position(|event| matches!(event, PipelineEvent::Settlement(_)))
        .expect("delivery must settle");
    assert!(cleanup < settlement, "{observed:?}");

    container
        .close()
        .await
        .expect("container close must succeed after reconciled scope disposal");
    *blocking_signals()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
}

#[tokio::test]
async fn force_cancelled_delivery_runs_fast_scope_cleanup_exactly_once() {
    let _serial = QUALIFICATION_LOCK.lock().await;
    reset_state();
    let container = build_container(Arc::new(FakeQueue::default())).await;
    let compiled = compile(&container, "capq01d.timeout");
    let tracker = Arc::clone(&compiled.0.scope_tracker);
    let cancellation = CancellationToken::new();
    let task_cancellation = cancellation.clone();
    let task = tokio::spawn(async move {
        compiled
            .execute(delivery_input(
                "capq01d.timeout",
                "none",
                Bytes::new(),
                task_cancellation,
                Duration::from_secs(30),
            ))
            .await
    });

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if events()
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .contains(&PipelineEvent::Handler("timeout"))
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("timeout handler must enter before force cancellation");

    cancellation.cancel();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());

    tokio::time::timeout(Duration::from_secs(1), tracker.drain())
        .await
        .expect("force-cancelled cleanup reconciliation must be bounded")
        .expect("a fast disposer must complete inside the force cleanup reserve");
    assert!(tracker.reconciled());
    assert_eq!(SCOPED_HANDLER_DISPOSES.load(Ordering::SeqCst), 1);
    assert_eq!(container.active_scope_count(), 0);
    container
        .close()
        .await
        .expect("reconciled force cleanup must not poison container shutdown");
}

#[tokio::test]
async fn force_cancelled_delivery_bounds_stuck_scope_cleanup_and_releases_ownership() {
    let _serial = QUALIFICATION_LOCK.lock().await;
    reset_state();
    let signals = BlockingSignals {
        handler_entered: Arc::new(Notify::new()),
        cleanup_entered: Arc::new(Notify::new()),
        cleanup_release: Arc::new(Notify::new()),
        cleanup_fails: false,
    };
    *blocking_signals()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(signals.clone());

    let container = build_container(Arc::new(FakeQueue::default())).await;
    let compiled = compile(&container, "capq01d.blocking");
    let tracker = Arc::clone(&compiled.0.scope_tracker);
    let cancellation = CancellationToken::new();
    let task_cancellation = cancellation.clone();
    let task = tokio::spawn(async move {
        compiled
            .execute(delivery_input(
                "capq01d.blocking",
                "none",
                Bytes::new(),
                task_cancellation,
                Duration::from_secs(30),
            ))
            .await
    });
    signals.handler_entered.notified().await;

    // Force-drain cancels the delivery token before aborting its task. The
    // abandoned scope observer must use only its bounded force-cleanup window
    // rather than retaining the 30-second delivery budget.
    cancellation.cancel();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());

    let error = tokio::time::timeout(Duration::from_secs(1), tracker.drain())
        .await
        .expect("force-cancelled cleanup reconciliation must be bounded")
        .expect_err("aborted stuck cleanup must remain visible to shutdown");
    assert!(error.to_string().contains("delivery scope cleanup failed"));
    assert!(
        tracker.reconciled(),
        "failed cleanup must still release resource ownership"
    );
    assert_eq!(container.active_scope_count(), 0);

    // Remain safe after the bounded cleanup deadline aborted and joined the
    // stuck disposer.
    signals.cleanup_release.notify_waiters();
    let _ = container.close().await;
    *blocking_signals()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
}
