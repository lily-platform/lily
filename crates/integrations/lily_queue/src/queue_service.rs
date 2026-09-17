use std::{any::TypeId, future::Future, panic::AssertUnwindSafe, pin::Pin, sync::Arc};

#[cfg(feature = "test-support")]
use std::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use async_trait::async_trait;
use futures::FutureExt;
use lily_config::{ConfigService, RabbitMqConsumerConfig};
use lily_error::{
    application::{MessageBrokerError, QueueHandlerError, message_broker::RabbitMQError},
    injection::InjectionError,
};
use lily_injectable_derive::Injectable;
use lily_injection::{ApplicationContainer, ProcessContext, ServiceTrait};
use lily_queue_client::RabbitMqOptions;
use lily_trace::prelude::*;
#[cfg(any(test, feature = "test-support"))]
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::{
    DeliveryCancellationReason, DeliveryInvocation,
    delivery_execution::{FrameworkStopReceipt, run_cooperative},
    delivery_lifecycle::{DeliveryExecutionExit, DeliveryLifecycleOwner},
    pipeline::{CompiledQueuePipeline, QueueHandlerCompilationInput, compile_pipeline_set},
    providers::rabbitmq::{
        RabbitMQConsumer, channel_manager::RabbitMQChannelManager,
        connection_manager::RabbitMQConnectionManager, queue_engine::RabbitMQQueueEngine,
        retry_engine::RabbitMQRetryEngine,
    },
    queue_engine_trait::{QueueDeliveryHandler, QueueExecutionError},
    queue_trait::Queue,
    shutdown_budget::DeliveryExecutionBudget,
};
use lily_queue_registry::{
    HandlerFunction, QueueGuardRegistration, QueueHandlerMetadata, QueueMiddlewareRegistration,
    QueuePayloadKind,
};

#[cfg(any(
    feature = "transactional-inbox-postgresql",
    feature = "transactional-inbox-postgresql-factory",
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-mongodb-factory"
))]
use crate::outbox_relay::TransactionalOutboxRelaySnapshot;

#[cfg(any(
    feature = "transactional-inbox-postgresql",
    feature = "transactional-inbox-postgresql-factory"
))]
use crate::transactional_postgresql::PostgresTransactionalRuntime;

#[cfg(any(
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-mongodb-factory"
))]
use crate::transactional_mongodb::MongoTransactionalRuntime;

pub(crate) use crate::delivery_lifecycle::DeliveryScopeTracker;

#[cfg(test)]
use crate::delivery_lifecycle::DeliveryScopeCleanupObserver;

#[cfg(any(
    feature = "transactional-inbox-postgresql",
    feature = "transactional-inbox-postgresql-factory",
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-mongodb-factory"
))]
enum TransactionalDeliveryContext {
    #[cfg(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory"
    ))]
    PostgreSql(crate::transactional_postgresql::PostgresTransaction),
    #[cfg(any(
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-mongodb-factory"
    ))]
    MongoDb(crate::transactional_mongodb::MongoTransaction),
}

struct CompiledQueueHandlerInner {
    container: Arc<ApplicationContainer>,
    service_type_id: TypeId,
    queue_name: &'static str,
    handler_name: &'static str,
    handler_fn: HandlerFunction,
    expected_schema_version: u16,
    expected_content_kind: &'static str,
    pipeline: CompiledQueuePipeline,
    component: Option<Arc<lily_trace::ComponentIdentity>>,
    #[cfg(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory",
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-mongodb-factory"
    ))]
    transactional_runtime: Option<crate::transactional::PreparedTransactionalRuntime>,
    #[cfg(test)]
    scope_tracker: Arc<DeliveryScopeTracker>,
}

/// Immutable generated-handler adapter consumed by `lily_consumer`.
///
/// This remains Rust-public only as a hidden cross-crate ABI. Applications
/// register `#[queue_service]` and `#[queue]` metadata instead.
#[doc(hidden)]
pub struct CompiledQueueHandler(Arc<CompiledQueueHandlerInner>);

/// Atomically compile every selected handler and its immutable pipeline.
///
/// No broker registration starts until all metadata, effective plans and
/// component constructors have succeeded. Shared concrete middleware/guard
/// types are initialized once for the Consumer and reused by every unrelated
/// handler that names them.
#[doc(hidden)]
pub async fn compile_queue_handlers(
    container: Arc<ApplicationContainer>,
    inputs: Vec<QueueHandlerCompilationInput>,
    global_middlewares: &[QueueMiddlewareRegistration],
    global_guards: &[QueueGuardRegistration],
    initialization_timeout: std::time::Duration,
) -> Result<Box<[CompiledQueueHandler]>, MessageBrokerError> {
    for input in &inputs {
        validate_generated_handler_metadata(input.metadata)?;
        if lily_injection::__private::service_registration_lifetime(
            container.services().as_ref(),
            input.metadata.service_type_id,
        )
        .is_none()
        {
            return Err(configuration_error(
                "generated queue handler service is not registered in the application container",
            ));
        }
    }

    let compiled = compile_pipeline_set(
        &inputs,
        global_middlewares,
        global_guards,
        container.services(),
        initialization_timeout,
    )
    .await?;
    if compiled.pipelines.len() != inputs.len() {
        return Err(configuration_error(
            "queue pipeline compiler returned a divergent handler count",
        ));
    }

    let handlers = inputs
        .into_iter()
        .zip(compiled.pipelines)
        .map(|(input, pipeline)| {
            CompiledQueueHandler::try_new_with_runtime(
                Arc::clone(&container),
                input.metadata,
                pipeline,
                input.component,
                #[cfg(any(
                    feature = "transactional-inbox-postgresql",
                    feature = "transactional-inbox-postgresql-factory",
                    feature = "transactional-inbox-mongodb",
                    feature = "transactional-inbox-mongodb-factory"
                ))]
                input.transactional_runtime,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(handlers.into_boxed_slice())
}

impl CompiledQueueHandler {
    #[cfg(test)]
    pub(crate) fn try_new(
        container: Arc<ApplicationContainer>,
        metadata: &'static QueueHandlerMetadata,
        pipeline: CompiledQueuePipeline,
        component: Option<Arc<lily_trace::ComponentIdentity>>,
    ) -> Result<Self, MessageBrokerError> {
        Self::try_new_with_runtime(
            container,
            metadata,
            pipeline,
            component,
            #[cfg(any(
                feature = "transactional-inbox-postgresql",
                feature = "transactional-inbox-postgresql-factory",
                feature = "transactional-inbox-mongodb",
                feature = "transactional-inbox-mongodb-factory"
            ))]
            None,
        )
    }

    fn try_new_with_runtime(
        container: Arc<ApplicationContainer>,
        metadata: &'static QueueHandlerMetadata,
        pipeline: CompiledQueuePipeline,
        component: Option<Arc<lily_trace::ComponentIdentity>>,
        #[cfg(any(
            feature = "transactional-inbox-postgresql",
            feature = "transactional-inbox-postgresql-factory",
            feature = "transactional-inbox-mongodb",
            feature = "transactional-inbox-mongodb-factory"
        ))]
        transactional_runtime: Option<crate::transactional::PreparedTransactionalRuntime>,
    ) -> Result<Self, MessageBrokerError> {
        validate_generated_handler_metadata(metadata)?;
        if lily_injection::__private::service_registration_lifetime(
            container.services().as_ref(),
            metadata.service_type_id,
        )
        .is_none()
        {
            return Err(configuration_error(
                "generated queue handler service is not registered in the application container",
            ));
        }

        #[cfg(any(
            feature = "transactional-inbox-postgresql",
            feature = "transactional-inbox-postgresql-factory",
            feature = "transactional-inbox-mongodb",
            feature = "transactional-inbox-mongodb-factory"
        ))]
        match (metadata.delivery_guarantee, transactional_runtime.is_some()) {
            (lily_queue_registry::DeliveryGuarantee::AtLeastOnce, false)
            | (lily_queue_registry::DeliveryGuarantee::TransactionalInbox, true) => {}
            (lily_queue_registry::DeliveryGuarantee::AtLeastOnce, true) => {
                return Err(configuration_error(
                    "at-least-once queue handler received a transactional runtime",
                ));
            }
            (lily_queue_registry::DeliveryGuarantee::TransactionalInbox, false) => {
                return Err(configuration_error(
                    "transactional inbox queue handler has no prepared storage runtime",
                ));
            }
        }

        Ok(Self(Arc::new(CompiledQueueHandlerInner {
            container,
            service_type_id: metadata.service_type_id,
            queue_name: metadata.queue_name,
            handler_name: metadata.handler_name,
            handler_fn: metadata.handler_fn,
            expected_schema_version: metadata.schema_version,
            expected_content_kind: metadata.content_kind,
            pipeline,
            component,
            #[cfg(any(
                feature = "transactional-inbox-postgresql",
                feature = "transactional-inbox-postgresql-factory",
                feature = "transactional-inbox-mongodb",
                feature = "transactional-inbox-mongodb-factory"
            ))]
            transactional_runtime,
            #[cfg(test)]
            scope_tracker: Arc::new(DeliveryScopeTracker::default()),
        })))
    }

    #[cfg(test)]
    fn into_registration(self) -> (&'static str, RegisteredQueueHandler) {
        let inner = self.0;
        let queue_name = inner.queue_name;
        let tracker = Arc::clone(&inner.scope_tracker);
        let callback_tracker = Arc::clone(&tracker);
        let callback = Arc::new(move |input| {
            Self::execute_inner(Arc::clone(&inner), Arc::clone(&callback_tracker), input)
        });
        (
            queue_name,
            RegisteredQueueHandler {
                callback,
                scope_tracker: tracker,
            },
        )
    }

    fn execute_inner(
        inner: Arc<CompiledQueueHandlerInner>,
        scope_tracker: Arc<DeliveryScopeTracker>,
        input: crate::DeliveryInput,
    ) -> Pin<Box<dyn Future<Output = Result<(), QueueExecutionError>> + Send>> {
        let budget = DeliveryExecutionBudget::before(input.deadline, tokio::time::Instant::now());
        let stop_receipt = FrameworkStopReceipt::default();
        let message_size = input.body.len();
        let span = tracing::info_span!(
            "queue.consume_message",
            queue_name = %inner.queue_name,
            handler_name = inner.handler_name,
            schema_version = u64::from(inner.expected_schema_version),
            content_kind = inner.expected_content_kind,
            message_size_bytes = message_size,
            processing_duration_ms = tracing::field::Empty,
            handler_result = tracing::field::Empty,
            lily.component.id = tracing::field::Empty,
            lily.component.kind = tracing::field::Empty,
            lily.component.name = tracing::field::Empty,
            lily.component.service.name = tracing::field::Empty,
            lily.worker.type = tracing::field::Empty
        );
        if let Some(identity) = inner.component.as_deref() {
            lily_trace::record_component_identity(&span, identity);
        }

        Box::pin(lily_trace::scope_component(
            inner.component.clone(),
            async move {
                if input.context.schema_version().into_inner() != inner.expected_schema_version {
                    return Err(
                        QueueHandlerError::permanent("QUEUE_SCHEMA_VERSION_UNSUPPORTED").into(),
                    );
                }
                if input.context.content_kind().as_str() != inner.expected_content_kind {
                    return Err(QueueHandlerError::permanent("QUEUE_CONTENT_KIND_MISMATCH").into());
                }

                #[cfg(any(
                    feature = "transactional-inbox-postgresql",
                    feature = "transactional-inbox-postgresql-factory",
                    feature = "transactional-inbox-mongodb",
                    feature = "transactional-inbox-mongodb-factory"
                ))]
                if let Some(runtime) = inner.transactional_runtime.clone() {
                    let logical_handler = inner.handler_name;
                    let event_id = input.context.event_id().into_inner();
                    return match runtime {
                        #[cfg(any(
                            feature = "transactional-inbox-postgresql",
                            feature = "transactional-inbox-postgresql-factory"
                        ))]
                        crate::transactional::PreparedTransactionalRuntime::PostgreSql(runtime) => {
                            let body_receipt = stop_receipt.clone();
                            let result = runtime
                                .execute_delivery(
                                    logical_handler,
                                    event_id,
                                    input.deadline,
                                    move |transaction| {
                                        Self::execute_scoped_inner(
                                            inner,
                                            scope_tracker,
                                            input,
                                            budget,
                                            body_receipt,
                                            Some(TransactionalDeliveryContext::PostgreSql(
                                                transaction,
                                            )),
                                        )
                                    },
                                )
                                .await
                                .map_err(|error| stop_receipt.classify_error(error))?;
                            transactional_execution_result(
                                result,
                                "QUEUE_POSTGRES_INBOX_IN_PROGRESS",
                            )
                            .map_err(|error| stop_receipt.classify_error(error))
                        }
                        #[cfg(any(
                            feature = "transactional-inbox-mongodb",
                            feature = "transactional-inbox-mongodb-factory"
                        ))]
                        crate::transactional::PreparedTransactionalRuntime::MongoDb(runtime) => {
                            // MongoDB's TransientTransactionError contract reruns
                            // the complete transaction body. Clone only immutable
                            // delivery input and create a fresh DI delivery scope
                            // inside every replayed operation.
                            let delivery_deadline = input.deadline;
                            // Execution notification must not cancel the MongoDB
                            // driver before the pipeline's cooperative window ends.
                            // The transaction owner separately bounds finalization.
                            let delivery_cancellation = CancellationToken::new();
                            let attempt_cancellation_root = input.cancellation.raw_child_token();
                            let attempt_inner = Arc::clone(&inner);
                            let attempt_tracker = Arc::clone(&scope_tracker);
                            let attempt_input = input.clone();
                            let attempt_receipt = stop_receipt.clone();
                            let execution = runtime
                                .execute_delivery(
                                    logical_handler,
                                    event_id,
                                    delivery_deadline,
                                    delivery_cancellation,
                                    move |transaction, body_deadline| {
                                        let mut attempt_input = attempt_input.clone();
                                        attempt_input.deadline =
                                            attempt_input.deadline.min(body_deadline);
                                        // Handler timeouts or application-triggered
                                        // cancellation remain attempt-local. Only the
                                        // original delivery owner can cancel the
                                        // transaction runtime itself.
                                        attempt_input.cancellation =
                                            attempt_input.cancellation.child_with_token(
                                                attempt_cancellation_root.child_token(),
                                            );
                                        Self::execute_scoped_inner(
                                            Arc::clone(&attempt_inner),
                                            Arc::clone(&attempt_tracker),
                                            attempt_input,
                                            budget,
                                            attempt_receipt.clone(),
                                            Some(TransactionalDeliveryContext::MongoDb(
                                                transaction,
                                            )),
                                        )
                                    },
                                )
                                .await
                                .map_err(|error| stop_receipt.classify_error(error))?;
                            mongodb_delivery_execution_result(execution)
                        }
                    };
                }

                Self::execute_scoped_inner(
                    inner,
                    scope_tracker,
                    input,
                    budget,
                    stop_receipt.clone(),
                    #[cfg(any(
                        feature = "transactional-inbox-postgresql",
                        feature = "transactional-inbox-postgresql-factory",
                        feature = "transactional-inbox-mongodb",
                        feature = "transactional-inbox-mongodb-factory"
                    ))]
                    None,
                )
                .await
                .map_err(|error| stop_receipt.classify_error(error))
            }
            .instrument(span),
        ))
    }

    async fn execute_scoped_inner(
        inner: Arc<CompiledQueueHandlerInner>,
        scope_tracker: Arc<DeliveryScopeTracker>,
        mut input: crate::DeliveryInput,
        budget: DeliveryExecutionBudget,
        stop_receipt: FrameworkStopReceipt,
        #[cfg(any(
            feature = "transactional-inbox-postgresql",
            feature = "transactional-inbox-postgresql-factory",
            feature = "transactional-inbox-mongodb",
            feature = "transactional-inbox-mongodb-factory"
        ))]
        transaction: Option<TransactionalDeliveryContext>,
    ) -> Result<(), QueueHandlerError> {
        stop_receipt.begin_attempt();
        let event_id = input.context.event_id().into_inner().to_string();
        let budget = budget.cap(input.deadline);
        let aggregate_deadline = budget.hard;
        let pipeline_deadline = budget.pipeline;
        input.deadline = pipeline_deadline;
        let cancellation = input.cancellation.clone();
        let context = ProcessContext::new()
            .with_metadata("transport".to_string(), "queue".to_string())
            .with_metadata("queue".to_string(), inner.queue_name.to_string())
            .with_metadata("handler".to_string(), inner.handler_name.to_string())
            .with_metadata("event_id".to_string(), event_id);
        let scope = inner.container.create_scope(context).map_err(|error| {
            QueueHandlerError::retryable_with_source("QUEUE_DELIVERY_SCOPE_CREATE_FAILED", error)
        })?;
        let invocation = DeliveryInvocation::new(inner.container.services(), input);
        let (mut owner, slot) = DeliveryLifecycleOwner::new(
            scope,
            invocation,
            inner.pipeline.clone(),
            scope_tracker.clone(),
            aggregate_deadline,
            cancellation.clone(),
        );
        let context = owner.context();
        // The replaceable execution borrows the owner's invocation and ledger.
        // Dropping a broker/transaction task releases this slot before the
        // owner transfers the same resources to tracked scope cleanup.
        let resources = owner.resources();
        let invocation = resources
            .invocation
            .as_mut()
            .expect("open delivery invocation");
        let ledger = &mut resources.ledger;
        let pipeline = &resources.pipeline;
        let root_budget = scope_tracker.shutdown_budget();
        let execution = run_cooperative(
            slot,
            ProcessContext::scope(context, async {
                #[cfg(any(
                    feature = "transactional-inbox-postgresql",
                    feature = "transactional-inbox-postgresql-factory",
                    feature = "transactional-inbox-mongodb",
                    feature = "transactional-inbox-mongodb-factory"
                ))]
                if let Some(transaction) = transaction {
                    match transaction {
                        #[cfg(any(
                            feature = "transactional-inbox-postgresql",
                            feature = "transactional-inbox-postgresql-factory"
                        ))]
                        TransactionalDeliveryContext::PostgreSql(transaction) => {
                            invocation.insert_local(transaction)?;
                        }
                        #[cfg(any(
                            feature = "transactional-inbox-mongodb",
                            feature = "transactional-inbox-mongodb-factory"
                        ))]
                        TransactionalDeliveryContext::MongoDb(transaction) => {
                            invocation.insert_local(transaction)?;
                        }
                    }
                }
                let mut result = pipeline
                    .enter_recorded(invocation, ledger, pipeline_deadline)
                    .await;
                if result.is_ok() {
                    result = pipeline
                        .evaluate_guards(invocation, pipeline_deadline)
                        .await;
                }
                if result.is_ok() {
                    result = match resolve_handler_service(
                        Arc::clone(&inner.container),
                        inner.service_type_id,
                    )
                    .await
                    {
                        Ok(service) => {
                            run_handler(inner.handler_fn, service, invocation, inner.queue_name)
                                .await
                        }
                        Err(error) => Err(error),
                    };
                }
                pipeline
                    .unwind_recorded(invocation, ledger, result, pipeline_deadline)
                    .await
            }),
            &cancellation,
            budget,
            &root_budget,
        )
        .await;
        let pipeline_completed = matches!(&execution, DeliveryExecutionExit::Completed(_));
        let execution = match execution {
            DeliveryExecutionExit::Completed(result) => result,
            DeliveryExecutionExit::Interrupted(reason) => {
                owner.record_interruption(crate::DeliveryTerminationReason::ExecutionCancelled(
                    reason,
                ));
                stop_receipt.interrupted(reason);
                Err(QueueHandlerError::retryable(
                    if reason == DeliveryCancellationReason::DeliveryTimeout {
                        "QUEUE_DELIVERY_EXECUTION_TIMED_OUT"
                    } else {
                        "QUEUE_HANDLER_CANCELLED"
                    },
                ))
            }
            DeliveryExecutionExit::Cancelled => {
                owner.record_interruption(crate::DeliveryTerminationReason::OwnerDropped);
                stop_receipt.interrupted(DeliveryCancellationReason::RuntimeCancellation);
                Err(QueueHandlerError::retryable("QUEUE_HANDLER_CANCELLED"))
            }
            DeliveryExecutionExit::Panicked => {
                owner.record_interruption(crate::DeliveryTerminationReason::Panicked);
                Err(QueueHandlerError::retryable(
                    "QUEUE_DELIVERY_SCOPE_RUN_PANICKED",
                ))
            }
        };
        if cancellation.is_cancelled() {
            tracing::debug!(pipeline_completed, cancellation_reason = ?cancellation.reason(), "Delivery pipeline terminal result observed after cancellation");
        }
        if let Err(cleanup_error) = owner.close().await {
            if execution.is_ok() {
                return Err(cleanup_error);
            }
            tracing::warn!(
                error_code = cleanup_error.code(),
                "Delivery cleanup failure did not replace its primary execution failure"
            );
        }
        execution
    }

    #[cfg(test)]
    fn execute(
        &self,
        input: crate::DeliveryInput,
    ) -> Pin<Box<dyn Future<Output = Result<(), QueueHandlerError>> + Send>> {
        let execution = Self::execute_inner(
            Arc::clone(&self.0),
            Arc::clone(&self.0.scope_tracker),
            input,
        );
        Box::pin(async move {
            match execution.await {
                Ok(()) => Ok(()),
                Err(QueueExecutionError::Handler(error)) => Err(error),
                Err(QueueExecutionError::RequeueDeferred { code })
                | Err(QueueExecutionError::FrameworkCancelled { code }) => {
                    Err(QueueHandlerError::retryable(code))
                }
            }
        })
    }
}

#[cfg(any(
    feature = "transactional-inbox-postgresql",
    feature = "transactional-inbox-postgresql-factory",
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-mongodb-factory"
))]
fn transactional_execution_result(
    result: crate::transactional::TransactionalExecution<()>,
    in_progress_code: &'static str,
) -> Result<(), QueueHandlerError> {
    match result {
        crate::transactional::TransactionalExecution::Applied(())
        | crate::transactional::TransactionalExecution::AlreadyCompleted => Ok(()),
        crate::transactional::TransactionalExecution::InProgress => {
            Err(QueueHandlerError::retryable(in_progress_code))
        }
    }
}

#[cfg(any(
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-mongodb-factory"
))]
fn mongodb_delivery_execution_result(
    result: crate::transactional_mongodb::MongoDeliveryExecution<()>,
) -> Result<(), QueueExecutionError> {
    match result {
        crate::transactional_mongodb::MongoDeliveryExecution::Completed(result) => {
            transactional_execution_result(result, "QUEUE_MONGODB_INBOX_IN_PROGRESS")
                .map_err(QueueExecutionError::from)
        }
        crate::transactional_mongodb::MongoDeliveryExecution::ContentionDeferred => Err(
            QueueExecutionError::requeue_deferred("QUEUE_MONGODB_INBOX_CONTENTION_DEFERRED"),
        ),
        crate::transactional_mongodb::MongoDeliveryExecution::CommitOutcomeDeferred => Err(
            QueueExecutionError::requeue_deferred("QUEUE_MONGODB_COMMIT_OUTCOME_DEFERRED"),
        ),
        crate::transactional_mongodb::MongoDeliveryExecution::Cancelled => Err(
            QueueExecutionError::framework_cancelled("QUEUE_MONGODB_TRANSACTION_CANCELLED"),
        ),
    }
}

struct CompiledQueueDispatchEntry {
    schema_version: u16,
    content_kind: &'static str,
    handler: Arc<CompiledQueueHandlerInner>,
}

/// Immutable local version/content dispatcher for one physical queue.
///
/// This remains Rust-public only as a hidden cross-crate ABI consumed by
/// `lily_consumer`. It creates exactly one broker registration and selects an
/// already-compiled generated handler before any application DI scope or
/// middleware is entered.
#[doc(hidden)]
pub struct CompiledQueueDispatch {
    queue_name: &'static str,
    entries: Box<[CompiledQueueDispatchEntry]>,
}

impl CompiledQueueDispatch {
    /// Freeze a non-empty exact `(schema_version, content_kind)` table.
    pub fn try_new(handlers: Vec<CompiledQueueHandler>) -> Result<Self, MessageBrokerError> {
        let Some(first) = handlers.first() else {
            return Err(configuration_error(
                "queue dispatch table requires at least one compiled handler",
            ));
        };
        let queue_name = first.0.queue_name;
        let mut entries = handlers
            .into_iter()
            .map(|handler| {
                let inner = handler.0;
                if inner.queue_name != queue_name {
                    return Err(configuration_error(
                        "queue dispatch table contains handlers for different physical queues",
                    ));
                }
                Ok(CompiledQueueDispatchEntry {
                    schema_version: inner.expected_schema_version,
                    content_kind: inner.expected_content_kind,
                    handler: inner,
                })
            })
            .collect::<Result<Vec<_>, MessageBrokerError>>()?;
        entries.sort_unstable_by(|left, right| {
            left.schema_version
                .cmp(&right.schema_version)
                .then_with(|| left.content_kind.cmp(right.content_kind))
        });
        if entries.windows(2).any(|pair| {
            pair[0].schema_version == pair[1].schema_version
                && pair[0].content_kind == pair[1].content_kind
        }) {
            return Err(configuration_error(
                "queue dispatch table contains a duplicate version/content contract",
            ));
        }

        Ok(Self {
            queue_name,
            entries: entries.into_boxed_slice(),
        })
    }

    /// Number of exact message contracts supported by this queue receiver.
    #[must_use]
    pub fn handler_count(&self) -> usize {
        self.entries.len()
    }

    fn into_registration(self) -> (&'static str, RegisteredQueueHandler) {
        let queue_name = self.queue_name;
        let entries = self.entries;
        let scope_tracker = Arc::new(DeliveryScopeTracker::default());
        let callback_tracker = Arc::clone(&scope_tracker);
        let callback = Arc::new(move |input: crate::DeliveryInput| {
            let schema_version = input.context.schema_version().into_inner();
            let content_kind = input.context.content_kind().as_str();
            let selected = entries.binary_search_by(|entry| {
                entry
                    .schema_version
                    .cmp(&schema_version)
                    .then_with(|| entry.content_kind.cmp(content_kind))
            });
            match selected {
                Ok(index) => CompiledQueueHandler::execute_inner(
                    Arc::clone(&entries[index].handler),
                    Arc::clone(&callback_tracker),
                    input,
                ),
                Err(_) => {
                    let version_supported = entries
                        .iter()
                        .any(|entry| entry.schema_version == schema_version);
                    let error = if version_supported {
                        QueueHandlerError::permanent("QUEUE_CONTENT_KIND_UNSUPPORTED")
                    } else {
                        QueueHandlerError::permanent("QUEUE_SCHEMA_VERSION_UNSUPPORTED")
                    };
                    Box::pin(std::future::ready(Err(error.into())))
                }
            }
        });
        (
            queue_name,
            RegisteredQueueHandler {
                callback,
                scope_tracker,
            },
        )
    }
}

pub(crate) struct RegisteredQueueHandler {
    pub(crate) callback: QueueDeliveryHandler,
    pub(crate) scope_tracker: Arc<DeliveryScopeTracker>,
}

/// Registration evidence for the explicit transport-free qualification seed.
#[cfg(feature = "test-support")]
#[doc(hidden)]
#[derive(Clone)]
pub struct QueueServiceTestProbe {
    registrations: Arc<AtomicUsize>,
    registration_attempts: Arc<AtomicUsize>,
    cancelled_registrations: Arc<AtomicUsize>,
    registration_gate: Arc<Mutex<TestSupportRegistrationGate>>,
    registration_attempted: Arc<Notify>,
    registration_released: Arc<Notify>,
    snapshot: Arc<Mutex<crate::DeliveryTerminalSnapshot>>,
    lifecycle: Arc<Mutex<TestSupportLifecycleState>>,
    lifecycle_entered: Arc<Notify>,
    lifecycle_completed: Arc<Notify>,
    lifecycle_released: Arc<Notify>,
    drain_reconciled: Arc<AtomicBool>,
}

/// One provider lifecycle entry observed by the transport-free queue seam.
///
/// This type is available only through the non-default `test-support` feature
/// and is a hidden cross-crate qualification ABI. Each variant represents one
/// direct call into the test provider; in particular, [`Self::StopAsync`] does
/// not synthesize additional phase entries.
#[cfg(feature = "test-support")]
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueueServiceTestLifecycleCall {
    /// `Queue::start_async` entered.
    StartAsync,
    /// `Queue::stop_admission_async` entered.
    StopAdmissionAsync,
    /// `Queue::drain_async` entered.
    DrainAsync,
    /// `Queue::force_drain_async` entered.
    ForceDrainAsync,
    /// `Queue::close_async` entered.
    CloseAsync,
    /// `Queue::stop_async` entered through service disposal.
    StopAsync,
    /// `Queue::wait_for_shutdown` entered.
    WaitForShutdown,
}

/// Coherent point-in-time copy of the transport-free lifecycle ledger.
#[cfg(feature = "test-support")]
#[doc(hidden)]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct QueueServiceTestLifecycleSnapshot {
    calls: Vec<QueueServiceTestLifecycleCall>,
    completions: [usize; QueueServiceTestLifecycleCall::COUNT],
}

#[cfg(feature = "test-support")]
impl QueueServiceTestLifecycleSnapshot {
    /// Calls in the exact order in which provider entrypoints were entered.
    #[must_use]
    pub fn calls(&self) -> &[QueueServiceTestLifecycleCall] {
        &self.calls
    }

    /// Number of times one lifecycle entrypoint was called.
    #[must_use]
    pub fn count(&self, call: QueueServiceTestLifecycleCall) -> usize {
        self.calls
            .iter()
            .filter(|observed| **observed == call)
            .count()
    }

    /// Number of matching lifecycle futures which returned success or error.
    /// A paused, dropped or panicking future is not counted as completed.
    #[must_use]
    pub fn completion_count(&self, call: QueueServiceTestLifecycleCall) -> usize {
        self.completions[call.index()]
    }
}

#[cfg(feature = "test-support")]
impl QueueServiceTestLifecycleCall {
    const COUNT: usize = 7;

    const fn index(self) -> usize {
        match self {
            Self::StartAsync => 0,
            Self::StopAdmissionAsync => 1,
            Self::DrainAsync => 2,
            Self::ForceDrainAsync => 3,
            Self::CloseAsync => 4,
            Self::StopAsync => 5,
            Self::WaitForShutdown => 6,
        }
    }

    const fn injected_failure_message(self) -> &'static str {
        match self {
            Self::StartAsync => "test-support queue start lifecycle failure",
            Self::StopAdmissionAsync => "test-support queue stop-admission lifecycle failure",
            Self::DrainAsync => "test-support queue drain lifecycle failure",
            Self::ForceDrainAsync => "test-support queue force-drain lifecycle failure",
            Self::CloseAsync => "test-support queue close lifecycle failure",
            Self::StopAsync => "test-support queue service-stop lifecycle failure",
            Self::WaitForShutdown => "test-support queue runtime-wait lifecycle failure",
        }
    }
}

#[cfg(feature = "test-support")]
impl QueueServiceTestProbe {
    /// Number of handler registrations admitted by the no-I/O provider.
    #[must_use]
    pub fn registration_count(&self) -> usize {
        self.registrations.load(Ordering::Acquire)
    }

    /// Number of registration futures which entered the no-I/O provider.
    #[must_use]
    pub fn registration_attempt_count(&self) -> usize {
        self.registration_attempts.load(Ordering::Acquire)
    }

    /// Pause subsequent registration attempts before they are admitted.
    ///
    /// Calling this method again discards unused release permits, giving each
    /// qualification scenario a fresh deterministic gate.
    pub fn pause_registration(&self) {
        let mut gate = self
            .registration_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        gate.paused = true;
        gate.permits = 0;
    }

    /// Wait without polling sleeps until at least `expected` attempts entered.
    pub async fn wait_for_registration_attempt(&self, expected: usize) {
        loop {
            let attempted = self.registration_attempted.notified();
            tokio::pin!(attempted);
            attempted.as_mut().enable();
            if self.registration_attempt_count() >= expected {
                return;
            }
            attempted.as_mut().await;
        }
    }

    /// Release exactly `count` paused registration attempts.
    ///
    /// Permits are consumed once and remain available for attempts which have
    /// entered but have not yet reached the gate.
    pub fn release_registration(&self, count: usize) {
        if count == 0 {
            return;
        }
        let mut gate = self
            .registration_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        gate.permits = gate.permits.saturating_add(count);
        drop(gate);
        self.registration_released.notify_waiters();
    }

    /// Number of entered registration futures dropped before admission.
    ///
    /// A per-attempt RAII owner increments this counter exactly once, including
    /// task abort while the attempt is paused.
    #[must_use]
    pub fn cancelled_registration_count(&self) -> usize {
        self.cancelled_registrations.load(Ordering::Acquire)
    }

    /// Replace the transport-free provider's operational snapshot.
    pub fn set_delivery_terminal_snapshot(&self, snapshot: crate::DeliveryTerminalSnapshot) {
        *self
            .snapshot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = snapshot;
    }

    /// Return one coherent copy of the provider lifecycle call ledger.
    ///
    /// The ledger records direct method entry before the no-I/O operation
    /// returns. Tests can therefore assert both canonical cleanup ordering and
    /// exact call counts without timing sleeps or transport side effects.
    #[must_use]
    pub fn lifecycle_snapshot(&self) -> QueueServiceTestLifecycleSnapshot {
        let lifecycle = self
            .lifecycle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        QueueServiceTestLifecycleSnapshot {
            calls: lifecycle.calls.clone(),
            completions: lifecycle.completions,
        }
    }

    /// Wait without polling sleeps until matching lifecycle futures have
    /// returned at least `expected` times.
    pub async fn wait_for_lifecycle_completion(
        &self,
        call: QueueServiceTestLifecycleCall,
        expected: usize,
    ) {
        loop {
            let completed = self.lifecycle_completed.notified();
            tokio::pin!(completed);
            completed.as_mut().enable();
            if self.lifecycle_snapshot().completion_count(call) >= expected {
                return;
            }
            completed.as_mut().await;
        }
    }

    /// Override whether the no-I/O provider proves delivery-task
    /// reconciliation before DI disposal.
    pub fn set_drain_reconciled(&self, reconciled: bool) {
        self.drain_reconciled.store(reconciled, Ordering::Release);
    }

    /// Make the next matching provider lifecycle call return a typed error.
    ///
    /// The matching call is still appended to the lifecycle ledger before the
    /// one-shot failure is returned. Multiple injections are consumed in the
    /// order in which they were configured for each matching entrypoint.
    pub fn fail_next_lifecycle(&self, call: QueueServiceTestLifecycleCall) {
        self.lifecycle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .faults
            .push(TestSupportLifecycleFault::Failure(call));
    }

    /// Make the next matching provider lifecycle call unwind with a fixed test
    /// message which contains no application or broker data.
    ///
    /// The one-shot decision is consumed when the matching call enters, but
    /// the panic is raised only after any configured lifecycle pause is
    /// released. This permits deterministic runtime-owner panic qualification
    /// without exposing application or broker values.
    pub fn panic_next_lifecycle(&self, call: QueueServiceTestLifecycleCall) {
        self.lifecycle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .faults
            .push(TestSupportLifecycleFault::Panic(call));
    }

    /// Pause subsequent entries to one provider lifecycle operation.
    ///
    /// Entry is still recorded before the future waits at the gate. Calling
    /// this method again discards unused permits for the selected operation.
    pub fn pause_lifecycle(&self, call: QueueServiceTestLifecycleCall) {
        let mut lifecycle = self
            .lifecycle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let index = call.index();
        lifecycle.paused[index] = true;
        lifecycle.permits[index] = 0;
    }

    /// Wait without polling sleeps until one lifecycle operation entered at
    /// least `expected` times.
    pub async fn wait_for_lifecycle_count(
        &self,
        call: QueueServiceTestLifecycleCall,
        expected: usize,
    ) {
        loop {
            let entered = self.lifecycle_entered.notified();
            tokio::pin!(entered);
            entered.as_mut().enable();
            if self.lifecycle_snapshot().count(call) >= expected {
                return;
            }
            entered.as_mut().await;
        }
    }

    /// Release exactly `count` paused entries for one lifecycle operation.
    ///
    /// Permits are retained until consumed, so releasing immediately after an
    /// entry notification cannot lose the wakeup or strand that operation.
    pub fn release_lifecycle(&self, call: QueueServiceTestLifecycleCall, count: usize) {
        if count == 0 {
            return;
        }
        let mut lifecycle = self
            .lifecycle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let index = call.index();
        lifecycle.permits[index] = lifecycle.permits[index].saturating_add(count);
        drop(lifecycle);
        self.lifecycle_released.notify_waiters();
    }

    /// Pause completion of subsequent queue-runtime wait futures.
    ///
    /// This is the runtime-owner-specific spelling of pausing
    /// [`QueueServiceTestLifecycleCall::WaitForShutdown`].
    pub fn pause_runtime_completion(&self) {
        self.pause_lifecycle(QueueServiceTestLifecycleCall::WaitForShutdown);
    }

    /// Wait until at least `expected` runtime wait futures entered.
    pub async fn wait_for_runtime_wait(&self, expected: usize) {
        self.wait_for_lifecycle_count(QueueServiceTestLifecycleCall::WaitForShutdown, expected)
            .await;
    }

    /// Release exactly `count` paused queue-runtime wait futures.
    pub fn release_runtime_completion(&self, count: usize) {
        self.release_lifecycle(QueueServiceTestLifecycleCall::WaitForShutdown, count);
    }
}

#[cfg(feature = "test-support")]
struct TestSupportQueue {
    registrations: Arc<AtomicUsize>,
    registration_attempts: Arc<AtomicUsize>,
    cancelled_registrations: Arc<AtomicUsize>,
    registration_gate: Arc<Mutex<TestSupportRegistrationGate>>,
    registration_attempted: Arc<Notify>,
    registration_released: Arc<Notify>,
    snapshot: Arc<Mutex<crate::DeliveryTerminalSnapshot>>,
    lifecycle: Arc<Mutex<TestSupportLifecycleState>>,
    lifecycle_entered: Arc<Notify>,
    lifecycle_completed: Arc<Notify>,
    lifecycle_released: Arc<Notify>,
    drain_reconciled: Arc<AtomicBool>,
    closed: AtomicBool,
}

#[cfg(feature = "test-support")]
#[derive(Default)]
struct TestSupportLifecycleState {
    calls: Vec<QueueServiceTestLifecycleCall>,
    completions: [usize; QueueServiceTestLifecycleCall::COUNT],
    faults: Vec<TestSupportLifecycleFault>,
    paused: [bool; QueueServiceTestLifecycleCall::COUNT],
    permits: [usize; QueueServiceTestLifecycleCall::COUNT],
}

#[cfg(feature = "test-support")]
#[derive(Clone, Copy)]
enum TestSupportLifecycleFault {
    Failure(QueueServiceTestLifecycleCall),
    Panic(QueueServiceTestLifecycleCall),
}

#[cfg(feature = "test-support")]
const TEST_SUPPORT_LIFECYCLE_PANIC_MESSAGE: &str = "test-support injected queue lifecycle panic";

#[cfg(feature = "test-support")]
impl TestSupportLifecycleFault {
    const fn call(self) -> QueueServiceTestLifecycleCall {
        match self {
            Self::Failure(call) | Self::Panic(call) => call,
        }
    }
}

#[cfg(feature = "test-support")]
#[derive(Default)]
struct TestSupportRegistrationGate {
    paused: bool,
    permits: usize,
}

#[cfg(feature = "test-support")]
struct TestSupportRegistrationAttempt {
    cancelled_registrations: Arc<AtomicUsize>,
    admitted: bool,
}

#[cfg(feature = "test-support")]
impl TestSupportRegistrationAttempt {
    fn new(cancelled_registrations: Arc<AtomicUsize>) -> Self {
        Self {
            cancelled_registrations,
            admitted: false,
        }
    }

    fn admit(mut self) {
        self.admitted = true;
    }
}

#[cfg(feature = "test-support")]
impl Drop for TestSupportRegistrationAttempt {
    fn drop(&mut self) {
        if !self.admitted {
            self.cancelled_registrations.fetch_add(1, Ordering::AcqRel);
        }
    }
}

#[cfg(feature = "test-support")]
impl TestSupportQueue {
    async fn enter_lifecycle(
        &self,
        call: QueueServiceTestLifecycleCall,
    ) -> Result<(), MessageBrokerError> {
        let fault = {
            let mut lifecycle = self
                .lifecycle
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            lifecycle.calls.push(call);
            let fault_index = lifecycle
                .faults
                .iter()
                .position(|pending| pending.call() == call);
            fault_index.map(|index| lifecycle.faults.remove(index))
        };
        self.lifecycle_entered.notify_waiters();
        self.wait_for_lifecycle_release(call).await;
        let result = match fault {
            Some(TestSupportLifecycleFault::Failure(_)) => Err(MessageBrokerError::RabbitMQError(
                RabbitMQError::General(call.injected_failure_message().to_string()),
            )),
            // `libfuzzer-sys` installs a panic hook which aborts before an
            // ordinary `panic!` can reach the production `catch_unwind`
            // boundary. Resuming an explicit, fixed unwind payload bypasses
            // that hook while exercising the same production containment path
            // used for an unwinding provider panic.
            Some(TestSupportLifecycleFault::Panic(_)) => {
                std::panic::resume_unwind(Box::new(TEST_SUPPORT_LIFECYCLE_PANIC_MESSAGE))
            }
            None => Ok(()),
        };
        self.lifecycle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .completions[call.index()] += 1;
        self.lifecycle_completed.notify_waiters();
        result
    }

    fn begin_registration_attempt(&self) -> TestSupportRegistrationAttempt {
        self.registration_attempts.fetch_add(1, Ordering::AcqRel);
        self.registration_attempted.notify_waiters();
        TestSupportRegistrationAttempt::new(Arc::clone(&self.cancelled_registrations))
    }

    async fn wait_for_registration_release(&self) {
        loop {
            let released = self.registration_released.notified();
            tokio::pin!(released);
            released.as_mut().enable();
            let admitted = {
                let mut gate = self
                    .registration_gate
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if !gate.paused {
                    true
                } else if gate.permits > 0 {
                    gate.permits -= 1;
                    true
                } else {
                    false
                }
            };
            if admitted {
                return;
            }
            released.as_mut().await;
        }
    }

    async fn wait_for_lifecycle_release(&self, call: QueueServiceTestLifecycleCall) {
        loop {
            let released = self.lifecycle_released.notified();
            tokio::pin!(released);
            released.as_mut().enable();
            let admitted = {
                let mut lifecycle = self
                    .lifecycle
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let index = call.index();
                if !lifecycle.paused[index] {
                    true
                } else if lifecycle.permits[index] > 0 {
                    lifecycle.permits[index] -= 1;
                    true
                } else {
                    false
                }
            };
            if admitted {
                return;
            }
            released.as_mut().await;
        }
    }
}

#[cfg(feature = "test-support")]
#[async_trait]
impl Queue for TestSupportQueue {
    async fn create_queue(
        &self,
        _exchange_name: &str,
        _queue: &str,
        _handler: RegisteredQueueHandler,
    ) -> Result<(), MessageBrokerError> {
        let attempt = self.begin_registration_attempt();
        self.wait_for_registration_release().await;
        self.registrations.fetch_add(1, Ordering::AcqRel);
        attempt.admit();
        Ok(())
    }

    async fn start_async(&self, _ct: CancellationToken) -> Result<(), MessageBrokerError> {
        self.enter_lifecycle(QueueServiceTestLifecycleCall::StartAsync)
            .await
    }

    async fn stop_async(&self) -> Result<(), MessageBrokerError> {
        self.enter_lifecycle(QueueServiceTestLifecycleCall::StopAsync)
            .await
    }

    async fn stop_admission_async(&self) -> Result<(), MessageBrokerError> {
        self.enter_lifecycle(QueueServiceTestLifecycleCall::StopAdmissionAsync)
            .await
    }

    async fn drain_async(&self) -> Result<(), MessageBrokerError> {
        self.enter_lifecycle(QueueServiceTestLifecycleCall::DrainAsync)
            .await
    }

    async fn force_drain_async(&self) -> Result<(), MessageBrokerError> {
        self.enter_lifecycle(QueueServiceTestLifecycleCall::ForceDrainAsync)
            .await
    }

    async fn close_async(&self) -> Result<(), MessageBrokerError> {
        let result = self
            .enter_lifecycle(QueueServiceTestLifecycleCall::CloseAsync)
            .await;
        if result.is_ok() {
            self.closed.store(true, Ordering::Release);
        }
        result
    }

    async fn wait_for_shutdown(&self) -> Result<(), MessageBrokerError> {
        self.enter_lifecycle(QueueServiceTestLifecycleCall::WaitForShutdown)
            .await
    }

    fn close_reconciled(&self) -> bool {
        self.closed.load(Ordering::Acquire) && self.drain_reconciled()
    }

    fn drain_reconciled(&self) -> bool {
        self.drain_reconciled.load(Ordering::Acquire)
    }

    fn delivery_terminal_snapshot(&self) -> crate::DeliveryTerminalSnapshot {
        *self
            .snapshot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn delivery_terminal_observations(&self) -> crate::DeliveryTerminalObservationsSnapshot {
        crate::DeliveryTerminalObservationsSnapshot::default()
    }
}

fn validate_generated_handler_metadata(
    metadata: &QueueHandlerMetadata,
) -> Result<(), MessageBrokerError> {
    if metadata.queue_name.is_empty()
        || metadata.service_type_name.is_empty()
        || metadata.method_name.is_empty()
        || metadata.handler_name.is_empty()
        || metadata.content_kind.is_empty()
    {
        return Err(configuration_error(
            "generated queue handler metadata contains an empty descriptor",
        ));
    }
    if metadata.schema_version == 0 {
        return Err(configuration_error(
            "generated queue handler schema version must be a positive u16",
        ));
    }
    #[cfg(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory",
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-mongodb-factory"
    ))]
    if metadata.delivery_guarantee == lily_queue_registry::DeliveryGuarantee::TransactionalInbox
        && !crate::transactional::handler_identity_is_valid(metadata.handler_name)
    {
        return Err(configuration_error(
            "transactional inbox handler identity is not canonical or exceeds its storage bound",
        ));
    }
    let content = metadata.content_kind.as_bytes();
    let content_valid = content.len() <= crate::MAX_QUEUE_CONTENT_KIND_BYTES
        && (content[0].is_ascii_lowercase() || content[0].is_ascii_digit())
        && content.iter().skip(1).all(|byte| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(byte, b'.' | b'_' | b'+' | b'-')
        });
    if !content_valid {
        return Err(configuration_error(
            "generated queue handler content kind is invalid",
        ));
    }
    let payload_contract_valid = match metadata.input_contract.payload_kind {
        QueuePayloadKind::None => metadata.input_contract.payload_type_name.is_none(),
        QueuePayloadKind::Json => {
            metadata.input_contract.payload_type_name.is_some() && metadata.content_kind == "json"
        }
        QueuePayloadKind::Text => {
            metadata.input_contract.payload_type_name.is_some() && metadata.content_kind == "text"
        }
        QueuePayloadKind::Binary => {
            metadata.input_contract.payload_type_name.is_some() && metadata.content_kind == "binary"
        }
        QueuePayloadKind::Raw | QueuePayloadKind::Custom => {
            metadata.input_contract.payload_type_name.is_some()
        }
    };
    if !payload_contract_valid {
        return Err(configuration_error(
            "generated queue handler payload and content contracts are inconsistent",
        ));
    }
    Ok(())
}

fn configuration_error(detail: impl Into<String>) -> MessageBrokerError {
    MessageBrokerError::RabbitMQError(RabbitMQError::Configuration(detail.into()))
}

async fn resolve_handler_service(
    container: Arc<ApplicationContainer>,
    service_type_id: TypeId,
) -> Result<Arc<dyn std::any::Any + Send + Sync>, QueueHandlerError> {
    match AssertUnwindSafe(container.resolve_by_type_id(service_type_id, None))
        .catch_unwind()
        .await
    {
        Ok(Ok(service)) => Ok(service),
        Ok(Err(error)) => Err(QueueHandlerError::retryable_with_source(
            "QUEUE_HANDLER_SERVICE_RESOLUTION_FAILED",
            error,
        )),
        Err(_) => Err(QueueHandlerError::retryable(
            "QUEUE_HANDLER_SERVICE_RESOLUTION_PANICKED",
        )),
    }
}

async fn run_handler(
    handler_fn: HandlerFunction,
    service_instance: Arc<dyn std::any::Any + Send + Sync>,
    invocation: &mut DeliveryInvocation,
    queue_name: &str,
) -> Result<(), QueueHandlerError> {
    AssertUnwindSafe(QueueService::execute_handler_traced(
        handler_fn,
        service_instance,
        invocation,
        queue_name,
    ))
    .catch_unwind()
    .await
    .unwrap_or_else(|_| Err(QueueHandlerError::retryable("QUEUE_HANDLER_PANICKED")))
}

/// Injectable RabbitMQ consumer service.
///
/// `lily_consumer` discovers `#[queue_service]`/`#[queue]` metadata and manages
/// this service. Applications do not register closure handlers directly: the
/// registry is the sole authority for typed extraction, per-delivery DI scope,
/// schema metadata and future AsyncAPI generation.
#[derive(Injectable, Default)]
#[service(lifetime = "Singleton")]
pub struct QueueService {
    provider: Option<Arc<dyn Queue>>,

    #[inject]
    config_service: Arc<ConfigService>,
}

impl QueueService {
    #[cfg(feature = "test-support")]
    pub(crate) fn test_support_seed(
        config_service: Arc<ConfigService>,
    ) -> (Self, QueueServiceTestProbe) {
        let registrations = Arc::new(AtomicUsize::new(0));
        let registration_attempts = Arc::new(AtomicUsize::new(0));
        let cancelled_registrations = Arc::new(AtomicUsize::new(0));
        let registration_gate = Arc::new(Mutex::new(TestSupportRegistrationGate::default()));
        let registration_attempted = Arc::new(Notify::new());
        let registration_released = Arc::new(Notify::new());
        let snapshot = Arc::new(Mutex::new(crate::DeliveryTerminalSnapshot::default()));
        let lifecycle = Arc::new(Mutex::new(TestSupportLifecycleState::default()));
        let lifecycle_entered = Arc::new(Notify::new());
        let lifecycle_completed = Arc::new(Notify::new());
        let lifecycle_released = Arc::new(Notify::new());
        let drain_reconciled = Arc::new(AtomicBool::new(true));
        let probe = QueueServiceTestProbe {
            registrations: Arc::clone(&registrations),
            registration_attempts: Arc::clone(&registration_attempts),
            cancelled_registrations: Arc::clone(&cancelled_registrations),
            registration_gate: Arc::clone(&registration_gate),
            registration_attempted: Arc::clone(&registration_attempted),
            registration_released: Arc::clone(&registration_released),
            snapshot: Arc::clone(&snapshot),
            lifecycle: Arc::clone(&lifecycle),
            lifecycle_entered: Arc::clone(&lifecycle_entered),
            lifecycle_completed: Arc::clone(&lifecycle_completed),
            lifecycle_released: Arc::clone(&lifecycle_released),
            drain_reconciled: Arc::clone(&drain_reconciled),
        };
        let provider = Arc::new(TestSupportQueue {
            closed: AtomicBool::new(false),
            registrations,
            registration_attempts,
            cancelled_registrations,
            registration_gate,
            registration_attempted,
            registration_released,
            snapshot,
            lifecycle,
            lifecycle_entered,
            lifecycle_completed,
            lifecycle_released,
            drain_reconciled,
        });
        (
            Self {
                provider: Some(provider),
                config_service,
            },
            probe,
        )
    }

    pub(crate) fn provider(&self) -> Result<Arc<dyn Queue>, MessageBrokerError> {
        self.provider
            .clone()
            .ok_or(MessageBrokerError::RabbitMQError(
                lily_error::application::message_broker::RabbitMQError::NotInitialized,
            ))
    }

    /// Install the provider before polling startup so DI cancellation can
    /// always reach the same provider through `dispose()`.
    async fn own_and_start_provider(
        &mut self,
        provider: Arc<dyn Queue>,
    ) -> Result<(), InjectionError> {
        self.provider = Some(Arc::clone(&provider));
        provider
            .start_async(CancellationToken::new())
            .await
            .map_err(InjectionError::from)
    }

    /// Sampling-independent delivery accounting. This read-only snapshot is
    /// suitable for shutdown reconciliation and contains no message payload,
    /// event identifier or broker credential.
    pub fn delivery_terminal_snapshot(
        &self,
    ) -> Result<crate::DeliveryTerminalSnapshot, MessageBrokerError> {
        Ok(self.provider()?.delivery_terminal_snapshot())
    }

    /// Bounded, sampling-independent event-level settlement evidence. Payloads
    /// and credentials are never retained.
    pub fn delivery_terminal_observations(
        &self,
    ) -> Result<crate::DeliveryTerminalObservationsSnapshot, MessageBrokerError> {
        Ok(self.provider()?.delivery_terminal_observations())
    }

    /// Sampling-independent transactional outbox relay health and lifecycle state.
    #[cfg(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory",
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-mongodb-factory"
    ))]
    pub fn transactional_outbox_relay_snapshot(
        &self,
    ) -> Result<TransactionalOutboxRelaySnapshot, MessageBrokerError> {
        Ok(self.provider()?.transactional_outbox_snapshot())
    }

    /// Sampling-independent transactional inbox execution evidence.
    ///
    /// The aggregate is payload-free and excludes handler, event, storage-cell
    /// and credential identities.
    #[cfg(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory",
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-mongodb-factory"
    ))]
    pub fn transactional_inbox_snapshot(
        &self,
    ) -> Result<crate::TransactionalInboxSnapshot, MessageBrokerError> {
        Ok(self.provider()?.transactional_inbox_snapshot())
    }

    #[cfg(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory"
    ))]
    pub(crate) async fn register_transactional_outbox(
        &self,
        runtime: Arc<PostgresTransactionalRuntime>,
    ) -> Result<(), MessageBrokerError> {
        self.provider()?
            .register_transactional_outbox(runtime)
            .await
    }

    #[cfg(any(
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-mongodb-factory"
    ))]
    pub(crate) async fn register_mongodb_transactional_outbox(
        &self,
        runtime: Arc<MongoTransactionalRuntime>,
    ) -> Result<(), MessageBrokerError> {
        self.provider()?
            .register_mongodb_transactional_outbox(runtime)
            .await
    }

    /// Wait until all queue runtime tasks have terminated.
    ///
    /// This does not request shutdown. Process-level consumers should normally
    /// use `lily_consumer`, which owns the full shutdown sequence.
    pub async fn wait_for_shutdown(&self) -> Result<(), MessageBrokerError> {
        self.provider()?.wait_for_shutdown().await
    }

    /// Register one compiled handler through the canonical singleton dispatch
    /// path. Retained only for transport-free crate qualification fixtures.
    #[cfg(test)]
    async fn register_compiled_handler(
        &self,
        exchange_name: &str,
        handler: CompiledQueueHandler,
    ) -> Result<(), MessageBrokerError> {
        self.register_compiled_dispatch(
            exchange_name,
            CompiledQueueDispatch::try_new(vec![handler])?,
        )
        .await
    }

    /// Register one fully validated immutable local dispatch table.
    pub(crate) async fn register_compiled_dispatch(
        &self,
        exchange_name: &str,
        dispatch: CompiledQueueDispatch,
    ) -> Result<(), MessageBrokerError> {
        let (queue_name, registration) = dispatch.into_registration();
        self.provider()?
            .create_queue(exchange_name, queue_name, registration)
            .await
    }

    /// Execute handler with tracing (child span of queue.consume_message)
    #[instrument(
        name = "queue.handler_execution",
        skip(handler_fn, service_instance, invocation),
        fields(
            queue_name = %queue_name,
            handler_duration_ms = tracing::field::Empty,
            lily.component.id = tracing::field::Empty,
            lily.component.kind = tracing::field::Empty,
            lily.component.name = tracing::field::Empty,
            lily.component.service.name = tracing::field::Empty,
            lily.worker.type = tracing::field::Empty
        )
    )]
    async fn execute_handler_traced(
        handler_fn: HandlerFunction,
        service_instance: Arc<dyn std::any::Any + Send + Sync>,
        invocation: &mut DeliveryInvocation,
        queue_name: &str,
    ) -> Result<(), QueueHandlerError> {
        lily_trace::record_current_component();
        let start = std::time::Instant::now();
        debug!("Executing handler for queue: {}", queue_name);

        // 🟢 USER HANDLER EXECUTION (generated by macro)
        let result = handler_fn(service_instance, invocation).await;

        let duration_ms = start.elapsed().as_millis();
        tracing::Span::current().record("handler_duration_ms", duration_ms);

        if result.is_ok() {
            debug!("Handler completed successfully in {}ms", duration_ms);
        } else {
            error!("Handler failed after {}ms", duration_ms);
        }

        result
    }
}

// ============================================================================
// Dependency Injection Support
// ============================================================================

#[async_trait]
impl ServiceTrait for QueueService {
    /// Initialize the configured RabbitMQ consumer runtime.
    #[lily_trace::prelude::instrument(name = "queue.service.initialize", skip(self))]
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        // A pre-installed provider is used only by application-container
        // seeds owned inside this crate's transport-free contract tests.
        // Starting it through the same ownership boundary keeps those tests
        // representative of production cancellation and failure behavior.
        if let Some(provider) = self.provider.clone() {
            return self.own_and_start_provider(provider).await;
        }
        let lily_config = self.config_service.get_lily_config().await;

        let consumer: RabbitMqConsumerConfig = lily_config.rabbitmq.consumer.ok_or_else(|| {
            InjectionError::General(
                "RabbitMQ consumer configuration not found in lily.toml".to_string(),
            )
        })?;

        let rabbitmq_options =
            RabbitMqOptions::from_consumer(&consumer).map_err(InjectionError::from)?;
        let opts = crate::setting::MessageBrokerSetting::from_definitions(
            rabbitmq_options.confirm_timeout(),
            &lily_config.rabbitmq.topology.queues,
        )
        .map_err(InjectionError::from)?;

        let connection_manager = Arc::new(RabbitMQConnectionManager::new(rabbitmq_options));
        let channel_manager = Arc::new(RabbitMQChannelManager::new(connection_manager.clone()));
        let retry_engine = Arc::new(RabbitMQRetryEngine::new(
            channel_manager.clone(),
            opts.clone(),
        ));
        let consumer_engine = Arc::new(RabbitMQQueueEngine::new(
            opts.clone(),
            channel_manager.clone(),
            retry_engine,
        ));
        #[cfg(any(
            feature = "transactional-inbox-postgresql",
            feature = "transactional-inbox-postgresql-factory",
            feature = "transactional-inbox-mongodb",
            feature = "transactional-inbox-mongodb-factory"
        ))]
        let provider: Arc<dyn Queue> = Arc::new(RabbitMQConsumer::new_with_outbox_relay(
            consumer_engine,
            connection_manager,
            channel_manager,
        ));
        #[cfg(not(any(
            feature = "transactional-inbox-postgresql",
            feature = "transactional-inbox-postgresql-factory",
            feature = "transactional-inbox-mongodb",
            feature = "transactional-inbox-mongodb-factory"
        )))]
        let provider: Arc<dyn Queue> =
            Arc::new(RabbitMQConsumer::new(consumer_engine, connection_manager));
        self.own_and_start_provider(provider).await
    }

    /// Stop admission, drain deliveries and close RabbitMQ resources.
    async fn dispose(&self) -> Result<(), InjectionError> {
        // Initialization can fail before a provider is installed. Cleanup must
        // remain safe both for that partially initialized state and on repeats.
        if let Some(provider) = self.provider.as_ref() {
            provider.stop_async().await.map_err(InjectionError::from)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory",
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-mongodb-factory"
    ))]
    fn metadata_validation_handler<'a>(
        _service: Arc<dyn std::any::Any + Send + Sync>,
        _invocation: &'a mut (dyn std::any::Any + Send),
    ) -> Pin<Box<dyn Future<Output = Result<(), QueueHandlerError>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }

    #[cfg(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory",
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-mongodb-factory"
    ))]
    #[test]
    fn transactional_handler_identity_fails_before_broker_admission() {
        let oversized: &'static str = Box::leak("h".repeat(513).into_boxed_str());
        let metadata = QueueHandlerMetadata {
            service_type_id: TypeId::of::<()>(),
            service_type_name: "QualificationService",
            component_kind: None,
            queue_name: "qualification.queue",
            method_name: "handle",
            handler_name: oversized,
            schema_version: 1,
            content_kind: "none",
            delivery_guarantee: lily_queue_registry::DeliveryGuarantee::TransactionalInbox,
            input_contract: lily_queue_registry::QueueHandlerInputContract::new(
                QueuePayloadKind::None,
                None,
            ),
            #[cfg(feature = "asyncapi")]
            asyncapi: lily_queue_registry::QueueAsyncApiRegistration::unspecified(),
            service_middlewares: Vec::new(),
            service_guards: Vec::new(),
            handler_middlewares: Vec::new(),
            handler_guards: Vec::new(),
            handler_fn: metadata_validation_handler,
        };

        assert!(validate_generated_handler_metadata(&metadata).is_err());
    }

    #[cfg(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory"
    ))]
    #[test]
    fn transactional_execution_maps_dedup_and_contention_without_settlement_authority() {
        assert!(
            transactional_execution_result(
                crate::transactional::TransactionalExecution::Applied(()),
                "QUEUE_TEST_IN_PROGRESS",
            )
            .is_ok()
        );
        assert!(
            transactional_execution_result(
                crate::transactional::TransactionalExecution::AlreadyCompleted,
                "QUEUE_TEST_IN_PROGRESS",
            )
            .is_ok()
        );
        let error = transactional_execution_result(
            crate::transactional::TransactionalExecution::InProgress,
            "QUEUE_POSTGRES_INBOX_IN_PROGRESS",
        )
        .expect_err("a live competing claim must retry through normal settlement");
        assert_eq!(error.code(), "QUEUE_POSTGRES_INBOX_IN_PROGRESS");
        assert_eq!(
            error.class(),
            lily_error::application::QueueHandlerFailureClass::Retryable
        );
    }

    #[cfg(any(
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-mongodb-factory"
    ))]
    #[test]
    fn mongodb_lease_deadline_maps_to_private_deferred_execution() {
        let deferred = mongodb_delivery_execution_result(
            crate::transactional_mongodb::MongoDeliveryExecution::ContentionDeferred,
        );
        assert!(matches!(
            deferred,
            Err(QueueExecutionError::RequeueDeferred {
                code: "QUEUE_MONGODB_INBOX_CONTENTION_DEFERRED"
            })
        ));

        let commit_unknown = mongodb_delivery_execution_result(
            crate::transactional_mongodb::MongoDeliveryExecution::CommitOutcomeDeferred,
        );
        assert!(matches!(
            commit_unknown,
            Err(QueueExecutionError::RequeueDeferred {
                code: "QUEUE_MONGODB_COMMIT_OUTCOME_DEFERRED"
            })
        ));

        let cancelled = mongodb_delivery_execution_result(
            crate::transactional_mongodb::MongoDeliveryExecution::Cancelled,
        );
        assert!(matches!(
            cancelled,
            Err(QueueExecutionError::FrameworkCancelled {
                code: "QUEUE_MONGODB_TRANSACTION_CANCELLED"
            })
        ));

        let ordinary_in_progress = mongodb_delivery_execution_result(
            crate::transactional_mongodb::MongoDeliveryExecution::Completed(
                crate::transactional::TransactionalExecution::InProgress,
            ),
        );
        assert!(matches!(
            ordinary_in_progress,
            Err(QueueExecutionError::Handler(_))
        ));
    }

    #[test]
    fn uninitialized_provider_is_a_typed_error() {
        let service = QueueService::default();

        let Err(error) = service.provider() else {
            panic!("provider must be absent");
        };
        assert_eq!(error.error_code(), "BROKER_NOT_INITIALIZED");
        let error = service
            .delivery_terminal_snapshot()
            .expect_err("snapshot must fail before initialization");
        assert_eq!(error.error_code(), "BROKER_NOT_INITIALIZED");
    }

    #[tokio::test]
    async fn aborted_cleanup_observer_preserves_failure_without_claiming_ownership_proof() {
        let tracker = Arc::new(DeliveryScopeTracker::default());
        tracker.begin();
        let observer = DeliveryScopeCleanupObserver::new(Arc::clone(&tracker));
        let entered = Arc::new(Notify::new());
        let task_entered = Arc::clone(&entered);
        let task = tokio::spawn(async move {
            let _observer = observer;
            task_entered.notify_one();
            std::future::pending::<()>().await;
        });

        entered.notified().await;
        task.abort();
        assert!(
            task.await
                .expect_err("observer task must be aborted")
                .is_cancelled()
        );

        let error = tokio::time::timeout(std::time::Duration::from_secs(1), tracker.drain())
            .await
            .expect("aborted observer must release the tracker")
            .expect_err("aborted observation must remain visible to shutdown");
        assert!(error.to_string().contains("delivery scope cleanup failed"));
        assert!(
            !tracker.reconciled(),
            "an observer cancelled before a terminal result cannot prove cleanup ownership"
        );
    }

    #[tokio::test]
    async fn exact_waiter_error_preserves_failure_after_proving_cleanup_ownership() {
        let tracker = Arc::new(DeliveryScopeTracker::default());
        tracker.begin();
        let mut observer = DeliveryScopeCleanupObserver::new(Arc::clone(&tracker));

        // `false` models the exact DI waiter returning an error only after its
        // disposer task was aborted and joined.
        observer.complete(false);
        drop(observer);

        let error = tracker
            .drain()
            .await
            .expect_err("exact waiter failure must remain visible to shutdown");
        assert!(error.to_string().contains("delivery scope cleanup failed"));
        assert!(
            tracker.reconciled(),
            "a terminal exact-wait result proves cleanup ownership even on error"
        );
    }

    #[cfg(feature = "test-support")]
    #[tokio::test]
    async fn test_support_lifecycle_failures_are_call_specific() {
        let (service, probe) = QueueService::test_support_seed(Arc::new(
            ConfigService::development("/tmp/lily-queue-lifecycle-failure-test.toml"),
        ));
        let provider = service.provider().expect("test-support queue provider");
        probe.fail_next_lifecycle(QueueServiceTestLifecycleCall::WaitForShutdown);
        probe.fail_next_lifecycle(QueueServiceTestLifecycleCall::CloseAsync);

        let runtime_error = provider
            .wait_for_shutdown()
            .await
            .expect_err("runtime wait failure must be injected");
        let close_error = provider
            .close_async()
            .await
            .expect_err("connection close failure must be injected");

        assert_ne!(runtime_error, close_error);
        assert_eq!(runtime_error.error_code(), "BROKER_GENERAL");
        assert_eq!(close_error.error_code(), "BROKER_GENERAL");
        assert_eq!(
            runtime_error,
            MessageBrokerError::RabbitMQError(RabbitMQError::General(
                "test-support queue runtime-wait lifecycle failure".to_string(),
            ))
        );
        assert_eq!(
            close_error,
            MessageBrokerError::RabbitMQError(RabbitMQError::General(
                "test-support queue close lifecycle failure".to_string(),
            ))
        );
        assert_eq!(
            probe
                .lifecycle_snapshot()
                .completion_count(QueueServiceTestLifecycleCall::WaitForShutdown),
            1
        );
        assert_eq!(
            probe
                .lifecycle_snapshot()
                .completion_count(QueueServiceTestLifecycleCall::CloseAsync),
            1
        );
    }

    #[cfg(feature = "test-support")]
    #[tokio::test]
    async fn test_support_lifecycle_panic_is_call_scoped_gated_and_one_shot() {
        let (service, probe) = QueueService::test_support_seed(Arc::new(
            ConfigService::development("/tmp/lily-queue-lifecycle-panic-test.toml"),
        ));
        let provider = service.provider().expect("test-support queue provider");
        probe.pause_runtime_completion();
        probe.panic_next_lifecycle(QueueServiceTestLifecycleCall::WaitForShutdown);

        provider
            .close_async()
            .await
            .expect("an unrelated lifecycle call must not consume the panic");
        let task_provider = Arc::clone(&provider);
        let runtime_wait = tokio::spawn(async move { task_provider.wait_for_shutdown().await });
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            probe.wait_for_runtime_wait(1),
        )
        .await
        .expect("runtime wait must enter the paused lifecycle gate");
        assert!(
            !runtime_wait.is_finished(),
            "the injected panic must remain pending until gate release"
        );

        probe.release_runtime_completion(1);
        let join_error = tokio::time::timeout(std::time::Duration::from_secs(1), runtime_wait)
            .await
            .expect("released runtime wait must terminate")
            .expect_err("the matching runtime wait must panic");
        assert!(join_error.is_panic());
        let payload = join_error.into_panic();
        let message = if let Some(message) = payload.downcast_ref::<&str>() {
            (*message).to_string()
        } else if let Some(message) = payload.downcast_ref::<String>() {
            message.clone()
        } else {
            panic!("test-support lifecycle panic must use a string payload")
        };
        assert_eq!(message, TEST_SUPPORT_LIFECYCLE_PANIC_MESSAGE);
        assert_eq!(
            probe
                .lifecycle_snapshot()
                .completion_count(QueueServiceTestLifecycleCall::WaitForShutdown),
            0,
            "a panicking lifecycle future must not claim normal completion"
        );

        probe.release_runtime_completion(1);
        provider
            .wait_for_shutdown()
            .await
            .expect("the panic injection must be consumed exactly once");
        assert_eq!(
            probe
                .lifecycle_snapshot()
                .count(QueueServiceTestLifecycleCall::WaitForShutdown),
            2
        );
        assert_eq!(
            probe
                .lifecycle_snapshot()
                .completion_count(QueueServiceTestLifecycleCall::WaitForShutdown),
            1
        );
    }
}

#[cfg(test)]
#[path = "queue_service_qualification_tests.rs"]
mod qualification_tests;

#[cfg(test)]
#[path = "queue_service_startup_ownership_tests.rs"]
mod startup_ownership_tests;
