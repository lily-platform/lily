use std::{collections::HashMap, fmt, sync::Arc};

use lily_config::QueueDefinition;
use lily_error::application::{
    MessageBrokerError, consumer::ConsumerPlanFailureKind, message_broker::RabbitMQError,
};
use lily_injection::ApplicationContainer;
#[cfg(any(
    feature = "transactional-inbox-postgresql",
    feature = "transactional-inbox-postgresql-factory",
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-mongodb-factory"
))]
use lily_queue::__private::PreparedTransactionalRuntime;
#[cfg(any(
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-mongodb-factory"
))]
use lily_queue::__private::prepare_mongodb_transactional_runtime;
#[cfg(any(
    feature = "transactional-inbox-postgresql",
    feature = "transactional-inbox-postgresql-factory"
))]
use lily_queue::__private::prepare_postgresql_transactional_runtime;
use lily_queue::__private::{
    CompiledQueueDispatch, DeliveryGuarantee, MAX_QUEUE_CONCURRENCY, MAX_QUEUE_PREFETCH,
    QueueGuardRegistration, QueueHandlerCompilationInput, QueueHandlerMetadata,
    QueueMiddlewareRegistration, QueuePayloadKind, compile_queue_handlers,
    validate_queue_definitions,
};
use lily_trace::{
    ComponentIdentity,
    runtime::{TraceCellConfig, TraceConfig},
};

pub(crate) const MAX_CONSUMER_QUEUES: usize = 256;
pub(crate) const MAX_CONSUMER_HANDLERS: usize = 512;
pub(crate) const MAX_CONSUMER_TRACE_CELLS: usize = 256;
pub(crate) const MAX_CONSUMER_DESCRIPTOR_BYTES: usize = 1_024;
pub(crate) const MAX_CONSUMER_PLAN_BYTES: usize = 256 * 1_024;
pub(crate) const MAX_CONSUMER_CONCURRENCY: u32 = MAX_QUEUE_CONCURRENCY;
pub(crate) const MAX_CONSUMER_PREFETCH: u16 = MAX_QUEUE_PREFETCH;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConsumerPlanError {
    TooManyQueues,
    TooManyHandlers,
    TooManyTraceCells,
    DescriptorInvalid,
    PlanBytesExceeded,
    QueueConfigurationInvalid,
    QueueConcurrencyExceeded,
    QueuePrefetchExceeded,
    HandlerMissing,
    HandlerDuplicate,
    #[allow(
        dead_code,
        reason = "constructed only by the feature-off transactional contract"
    )]
    TransactionalInboxFeatureDisabled,
    #[allow(
        dead_code,
        reason = "constructed only when a transactional backend feature is enabled"
    )]
    TransactionalInboxBindingMissing,
    #[allow(
        dead_code,
        reason = "constructed only when a transactional backend feature is enabled"
    )]
    TransactionalInboxBindingUnused,
    TraceConfigurationInvalid,
    TraceCellMissing,
    TraceCellKindMismatch,
}

impl ConsumerPlanError {
    pub(crate) const fn diagnostic_code(self) -> &'static str {
        match self {
            Self::TooManyQueues => "CONSUMER_QUEUE_LIMIT",
            Self::TooManyHandlers => "CONSUMER_HANDLER_LIMIT",
            Self::TooManyTraceCells => "CONSUMER_TRACE_CELL_LIMIT",
            Self::DescriptorInvalid => "CONSUMER_DESCRIPTOR_INVALID",
            Self::PlanBytesExceeded => "CONSUMER_PLAN_BYTES_EXCEEDED",
            Self::QueueConfigurationInvalid => "CONSUMER_QUEUE_CONFIG_INVALID",
            Self::QueueConcurrencyExceeded => "CONSUMER_QUEUE_CONCURRENCY_EXCEEDED",
            Self::QueuePrefetchExceeded => "CONSUMER_QUEUE_PREFETCH_EXCEEDED",
            Self::HandlerMissing => "CONSUMER_HANDLER_MISSING",
            Self::HandlerDuplicate => "CONSUMER_HANDLER_DUPLICATE",
            Self::TransactionalInboxFeatureDisabled => {
                "CONSUMER_TRANSACTIONAL_INBOX_FEATURE_DISABLED"
            }
            Self::TransactionalInboxBindingMissing => {
                "CONSUMER_TRANSACTIONAL_INBOX_BINDING_MISSING"
            }
            Self::TransactionalInboxBindingUnused => "CONSUMER_TRANSACTIONAL_INBOX_BINDING_UNUSED",
            Self::TraceConfigurationInvalid => "CONSUMER_TRACE_CONFIG_INVALID",
            Self::TraceCellMissing => "CONSUMER_TRACE_CELL_MISSING",
            Self::TraceCellKindMismatch => "CONSUMER_TRACE_CELL_KIND_MISMATCH",
        }
    }
}

impl fmt::Display for ConsumerPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.diagnostic_code())
    }
}

impl std::error::Error for ConsumerPlanError {}

impl From<ConsumerPlanError> for ConsumerPlanFailureKind {
    fn from(error: ConsumerPlanError) -> Self {
        match error {
            ConsumerPlanError::TooManyQueues => Self::TooManyQueues,
            ConsumerPlanError::TooManyHandlers => Self::TooManyHandlers,
            ConsumerPlanError::TooManyTraceCells => Self::TooManyTraceCells,
            ConsumerPlanError::DescriptorInvalid => Self::DescriptorInvalid,
            ConsumerPlanError::PlanBytesExceeded => Self::PlanBytesExceeded,
            ConsumerPlanError::QueueConfigurationInvalid => Self::QueueConfigurationInvalid,
            ConsumerPlanError::QueueConcurrencyExceeded => Self::QueueConcurrencyExceeded,
            ConsumerPlanError::QueuePrefetchExceeded => Self::QueuePrefetchExceeded,
            ConsumerPlanError::HandlerMissing => Self::HandlerMissing,
            ConsumerPlanError::HandlerDuplicate => Self::HandlerDuplicate,
            ConsumerPlanError::TransactionalInboxFeatureDisabled => {
                Self::TransactionalInboxFeatureDisabled
            }
            ConsumerPlanError::TransactionalInboxBindingMissing => {
                Self::TransactionalInboxBindingMissing
            }
            ConsumerPlanError::TransactionalInboxBindingUnused => {
                Self::TransactionalInboxBindingUnused
            }
            ConsumerPlanError::TraceConfigurationInvalid => Self::TraceConfigurationInvalid,
            ConsumerPlanError::TraceCellMissing => Self::TraceCellMissing,
            ConsumerPlanError::TraceCellKindMismatch => Self::TraceCellKindMismatch,
        }
    }
}

#[derive(Debug)]
pub(crate) enum ConsumerExecutionPlanError {
    Selection(ConsumerPlanError),
    Compilation(MessageBrokerError),
}

impl fmt::Display for ConsumerExecutionPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Selection(error) => error.fmt(formatter),
            Self::Compilation(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for ConsumerExecutionPlanError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Selection(error) => Some(error),
            Self::Compilation(error) => Some(error),
        }
    }
}

pub(crate) struct ValidatedTraceCells {
    cells: Vec<TraceCellConfig>,
}

impl ValidatedTraceCells {
    pub(crate) fn try_new(cells: Vec<TraceCellConfig>) -> Result<Self, ConsumerPlanError> {
        if cells.len() > MAX_CONSUMER_TRACE_CELLS {
            return Err(ConsumerPlanError::TooManyTraceCells);
        }

        let mut retained_bytes = 0usize;
        for cell in &cells {
            for value in [&cell.id, &cell.worker_type, &cell.worker_name, &cell.kind] {
                if value.len() > MAX_CONSUMER_DESCRIPTOR_BYTES {
                    return Err(ConsumerPlanError::DescriptorInvalid);
                }
                retained_bytes = retained_bytes.saturating_add(value.len());
            }
        }
        if retained_bytes > MAX_CONSUMER_PLAN_BYTES {
            return Err(ConsumerPlanError::PlanBytesExceeded);
        }

        let config = TraceConfig {
            cells,
            ..TraceConfig::default()
        };
        config
            .validate()
            .map_err(|_| ConsumerPlanError::TraceConfigurationInvalid)?;
        Ok(Self {
            cells: config.cells,
        })
    }

    pub(crate) fn as_slice(&self) -> &[TraceCellConfig] {
        &self.cells
    }
}

#[derive(Clone, Copy)]
pub(crate) struct HandlerDescriptor<'a> {
    pub(crate) queue_name: &'a str,
    pub(crate) service_type_name: &'a str,
    pub(crate) component_kind: Option<&'a str>,
    pub(crate) method_name: &'a str,
    pub(crate) handler_name: &'a str,
    #[allow(
        dead_code,
        reason = "carried by fuzz descriptors but interpreted only by the canonical queue compiler"
    )]
    pub(crate) schema_version: u16,
    pub(crate) content_kind: &'a str,
    pub(crate) delivery_guarantee: DeliveryGuarantee,
    #[allow(
        dead_code,
        reason = "carried by fuzz descriptors but interpreted only by the canonical queue compiler"
    )]
    pub(crate) payload_kind: QueuePayloadKind,
    pub(crate) payload_type_name: Option<&'a str>,
}

impl<'a> From<&'a QueueHandlerMetadata> for HandlerDescriptor<'a> {
    fn from(metadata: &'a QueueHandlerMetadata) -> Self {
        Self {
            queue_name: metadata.queue_name,
            service_type_name: metadata.service_type_name,
            component_kind: metadata.component_kind,
            method_name: metadata.method_name,
            handler_name: metadata.handler_name,
            schema_version: metadata.schema_version,
            content_kind: metadata.content_kind,
            delivery_guarantee: metadata.delivery_guarantee,
            payload_kind: metadata.input_contract.payload_kind,
            payload_type_name: metadata.input_contract.payload_type_name,
        }
    }
}

pub(crate) struct ConsumerPlanBinding {
    pub(crate) queue_index: usize,
    pub(crate) handlers: Box<[ConsumerPlanHandlerBinding]>,
}

pub(crate) struct ConsumerPlanHandlerBinding {
    pub(crate) handler_index: usize,
    pub(crate) component: Option<Arc<ComponentIdentity>>,
}

pub(crate) struct ConsumerPlan {
    bindings: Vec<ConsumerPlanBinding>,
}

/// Fully materialized, immutable runtime binding for one configured queue.
///
/// Queue selection indices are deliberately consumed before broker
/// registration begins. Runtime composition therefore cannot re-read or
/// accidentally cross-pair the parallel configuration and registry slices.
pub(crate) struct ConsumerExecutionPlanBinding {
    pub(crate) definition: QueueDefinition,
    pub(crate) dispatch: CompiledQueueDispatch,
    pub(crate) handler_count: usize,
    #[cfg(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory",
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-mongodb-factory"
    ))]
    pub(crate) transactional_runtime: Option<PreparedTransactionalRuntime>,
}

/// Immutable queue execution plan consumed by the Consumer composition root.
pub(crate) struct ConsumerExecutionPlan {
    bindings: Box<[ConsumerExecutionPlanBinding]>,
    #[cfg(feature = "asyncapi")]
    asyncapi_bindings: Box<[ConsumerAsyncApiPlanBinding]>,
}

#[cfg(feature = "asyncapi")]
pub(crate) struct ConsumerAsyncApiPlanBinding {
    pub(crate) definition: QueueDefinition,
    pub(crate) handlers: Box<[&'static QueueHandlerMetadata]>,
}

struct PendingConsumerExecutionPlanBinding {
    definition: QueueDefinition,
    handlers: Box<[PendingConsumerExecutionPlanHandler]>,
}

struct PendingConsumerExecutionPlanHandler {
    metadata: &'static QueueHandlerMetadata,
    component: Option<Arc<ComponentIdentity>>,
}

impl ConsumerExecutionPlan {
    /// Select and compile the exact generated handlers used by the runtime.
    ///
    /// Selection indices never escape this function. The same definition and
    /// metadata slices are used for both matching and materialization, so a
    /// caller cannot cross-pair a validated selector with a different input.
    pub(crate) async fn build(
        definitions: &[QueueDefinition],
        handlers: &[&'static QueueHandlerMetadata],
        trace_cells: &ValidatedTraceCells,
        container: Arc<ApplicationContainer>,
        middleware: &[QueueMiddlewareRegistration],
        guards: &[QueueGuardRegistration],
        initialization_timeout: std::time::Duration,
    ) -> Result<Self, ConsumerExecutionPlanError> {
        let descriptors = handlers
            .iter()
            .map(|handler| HandlerDescriptor::from(*handler))
            .collect::<Vec<_>>();
        let selected = ConsumerPlan::build(definitions, &descriptors, trace_cells)
            .map_err(ConsumerExecutionPlanError::Selection)?;
        let pending = Self::bind(selected, definitions, handlers, &descriptors)
            .map_err(ConsumerExecutionPlanError::Compilation)?;
        Self::materialize(
            pending,
            container,
            middleware,
            guards,
            initialization_timeout,
        )
        .await
        .map_err(ConsumerExecutionPlanError::Compilation)
    }

    pub(crate) fn len(&self) -> usize {
        self.bindings.len()
    }

    pub(crate) fn handler_count(&self) -> usize {
        self.bindings
            .iter()
            .map(|binding| binding.handler_count)
            .sum()
    }

    pub(crate) fn into_bindings(
        self,
    ) -> impl ExactSizeIterator<Item = ConsumerExecutionPlanBinding> {
        self.bindings.into_vec().into_iter()
    }

    #[cfg(feature = "asyncapi")]
    pub(crate) fn prepare_asyncapi(
        &self,
        config: &lily_asyncapi::AsyncApiConfig,
        virtual_host: &str,
    ) -> Result<crate::asyncapi::PreparedConsumerAsyncApi, lily_asyncapi::AsyncApiBuildError> {
        crate::asyncapi::prepare_consumer_asyncapi(config, virtual_host, &self.asyncapi_bindings)
    }

    /// Consume selection indices and freeze exact config/metadata pairs.
    fn bind(
        selected: ConsumerPlan,
        definitions: &[QueueDefinition],
        handlers: &[&'static QueueHandlerMetadata],
        descriptors: &[HandlerDescriptor<'_>],
    ) -> Result<Box<[PendingConsumerExecutionPlanBinding]>, MessageBrokerError> {
        let mut bindings = Vec::with_capacity(selected.bindings.len());
        for binding in selected.bindings {
            let definition = definitions.get(binding.queue_index).ok_or_else(|| {
                invalid_materialized_plan(
                    "validated consumer plan referenced an unavailable queue definition",
                )
            })?;
            let pending_handlers = binding
                .handlers
                .into_vec()
                .into_iter()
                .map(|handler| {
                    let descriptor = descriptors.get(handler.handler_index).ok_or_else(|| {
                        invalid_materialized_plan(
                            "validated consumer plan referenced an unavailable handler descriptor",
                        )
                    })?;
                    let metadata = handlers.get(handler.handler_index).copied().ok_or_else(
                        || {
                            invalid_materialized_plan(
                                "validated consumer plan referenced unavailable handler metadata",
                            )
                        },
                    )?;
                    if definition.name != metadata.queue_name
                        || !metadata_matches_descriptor(metadata, descriptor)
                    {
                        return Err(invalid_materialized_plan(
                            "validated consumer plan queue and handler identities diverged",
                        ));
                    }
                    Ok(PendingConsumerExecutionPlanHandler {
                        metadata,
                        component: handler.component,
                    })
                })
                .collect::<Result<Vec<_>, MessageBrokerError>>()?;

            bindings.push(PendingConsumerExecutionPlanBinding {
                definition: definition.clone(),
                handlers: pending_handlers.into_boxed_slice(),
            });
        }

        Ok(bindings.into_boxed_slice())
    }

    /// Compile every frozen generated handler before broker admission begins.
    async fn materialize(
        pending: Box<[PendingConsumerExecutionPlanBinding]>,
        container: Arc<ApplicationContainer>,
        middleware: &[QueueMiddlewareRegistration],
        guards: &[QueueGuardRegistration],
        initialization_timeout: std::time::Duration,
    ) -> Result<Self, MessageBrokerError> {
        let mut compilation_inputs =
            Vec::with_capacity(pending.iter().map(|binding| binding.handlers.len()).sum());
        #[cfg(any(
            feature = "transactional-inbox-postgresql",
            feature = "transactional-inbox-postgresql-factory",
            feature = "transactional-inbox-mongodb",
            feature = "transactional-inbox-mongodb-factory"
        ))]
        let mut transactional_runtimes = Vec::with_capacity(pending.len());
        for binding in pending.iter() {
            #[cfg(any(
                feature = "transactional-inbox-postgresql",
                feature = "transactional-inbox-postgresql-factory",
                feature = "transactional-inbox-mongodb",
                feature = "transactional-inbox-mongodb-factory"
            ))]
            let transactional_runtime = if binding.handlers.iter().any(|handler| {
                handler.metadata.delivery_guarantee == DeliveryGuarantee::TransactionalInbox
            }) {
                let binding_config =
                    binding
                        .definition
                        .transactional_inbox
                        .as_ref()
                        .ok_or_else(|| {
                            invalid_materialized_plan(
                                "transactional queue lost its configured storage binding",
                            )
                        })?;
                Some(match binding_config.backend {
                    #[cfg(any(
                        feature = "transactional-inbox-postgresql",
                        feature = "transactional-inbox-postgresql-factory"
                    ))]
                    lily_config::TransactionalInboxBackend::PostgreSql => {
                        PreparedTransactionalRuntime::PostgreSql(
                            prepare_postgresql_transactional_runtime(
                                Arc::clone(&container),
                                &binding.definition,
                            )
                            .await?,
                        )
                    }
                    #[cfg(any(
                        feature = "transactional-inbox-mongodb",
                        feature = "transactional-inbox-mongodb-factory"
                    ))]
                    lily_config::TransactionalInboxBackend::MongoDb => {
                        PreparedTransactionalRuntime::MongoDb(
                            prepare_mongodb_transactional_runtime(
                                Arc::clone(&container),
                                &binding.definition,
                            )
                            .await?,
                        )
                    }
                    #[allow(
                        unreachable_patterns,
                        reason = "Cargo feature unification can add a lily_config backend whose adapter is not enabled on lily_consumer"
                    )]
                    _ => {
                        return Err(invalid_materialized_plan(
                            "configured transactional inbox backend adapter is not enabled in lily_consumer",
                        ));
                    }
                })
            } else {
                None
            };

            for handler in binding.handlers.iter() {
                let input =
                    QueueHandlerCompilationInput::new(handler.metadata, handler.component.clone());
                #[cfg(any(
                    feature = "transactional-inbox-postgresql",
                    feature = "transactional-inbox-postgresql-factory",
                    feature = "transactional-inbox-mongodb",
                    feature = "transactional-inbox-mongodb-factory"
                ))]
                let input = if handler.metadata.delivery_guarantee
                    == DeliveryGuarantee::TransactionalInbox
                {
                    input.with_transactional_runtime(
                        transactional_runtime
                            .as_ref()
                            .ok_or_else(|| {
                                invalid_materialized_plan(
                                    "transactional handler lost its prepared storage runtime",
                                )
                            })?
                            .clone(),
                    )
                } else {
                    input
                };
                compilation_inputs.push(input);
            }
            #[cfg(any(
                feature = "transactional-inbox-postgresql",
                feature = "transactional-inbox-postgresql-factory",
                feature = "transactional-inbox-mongodb",
                feature = "transactional-inbox-mongodb-factory"
            ))]
            transactional_runtimes.push(transactional_runtime);
        }
        let compiled = compile_queue_handlers(
            container,
            compilation_inputs,
            middleware,
            guards,
            initialization_timeout,
        )
        .await?;
        let expected_handler_count = pending
            .iter()
            .map(|binding| binding.handlers.len())
            .sum::<usize>();
        if compiled.len() != expected_handler_count {
            return Err(invalid_materialized_plan(
                "queue pipeline compiler returned a divergent handler count",
            ));
        }

        let mut bindings = Vec::with_capacity(pending.len());
        #[cfg(feature = "asyncapi")]
        let mut asyncapi_bindings = Vec::with_capacity(pending.len());
        let mut compiled = compiled.into_vec().into_iter();
        #[cfg(any(
            feature = "transactional-inbox-postgresql",
            feature = "transactional-inbox-postgresql-factory",
            feature = "transactional-inbox-mongodb",
            feature = "transactional-inbox-mongodb-factory"
        ))]
        let mut transactional_runtimes = transactional_runtimes.into_iter();
        for binding in pending.into_vec() {
            let handler_count = binding.handlers.len();
            #[cfg(feature = "asyncapi")]
            let asyncapi_handlers = binding
                .handlers
                .iter()
                .map(|handler| handler.metadata)
                .collect::<Vec<_>>()
                .into_boxed_slice();
            #[cfg(feature = "asyncapi")]
            asyncapi_bindings.push(ConsumerAsyncApiPlanBinding {
                definition: binding.definition.clone(),
                handlers: asyncapi_handlers,
            });
            let handlers = (0..handler_count)
                .map(|_| {
                    compiled.next().ok_or_else(|| {
                        invalid_materialized_plan(
                            "queue pipeline compiler omitted a dispatch handler",
                        )
                    })
                })
                .collect::<Result<Vec<_>, MessageBrokerError>>()?;
            let dispatch = CompiledQueueDispatch::try_new(handlers)?;
            bindings.push(ConsumerExecutionPlanBinding {
                definition: binding.definition,
                dispatch,
                handler_count,
                #[cfg(any(
                    feature = "transactional-inbox-postgresql",
                    feature = "transactional-inbox-postgresql-factory",
                    feature = "transactional-inbox-mongodb",
                    feature = "transactional-inbox-mongodb-factory"
                ))]
                transactional_runtime: transactional_runtimes.next().ok_or_else(|| {
                    invalid_materialized_plan(
                        "transactional runtime plan omitted a physical queue binding",
                    )
                })?,
            });
        }
        if compiled.next().is_some() {
            return Err(invalid_materialized_plan(
                "queue pipeline compiler returned unbound dispatch handlers",
            ));
        }
        #[cfg(any(
            feature = "transactional-inbox-postgresql",
            feature = "transactional-inbox-postgresql-factory",
            feature = "transactional-inbox-mongodb",
            feature = "transactional-inbox-mongodb-factory"
        ))]
        if transactional_runtimes.next().is_some() {
            return Err(invalid_materialized_plan(
                "transactional runtime plan returned an unbound physical queue runtime",
            ));
        }

        Ok(Self {
            bindings: bindings.into_boxed_slice(),
            #[cfg(feature = "asyncapi")]
            asyncapi_bindings: asyncapi_bindings.into_boxed_slice(),
        })
    }
}

fn metadata_matches_descriptor(
    metadata: &QueueHandlerMetadata,
    descriptor: &HandlerDescriptor<'_>,
) -> bool {
    metadata.queue_name == descriptor.queue_name
        && metadata.service_type_name == descriptor.service_type_name
        && metadata.component_kind == descriptor.component_kind
        && metadata.method_name == descriptor.method_name
        && metadata.handler_name == descriptor.handler_name
        && metadata.schema_version == descriptor.schema_version
        && metadata.content_kind == descriptor.content_kind
        && metadata.delivery_guarantee == descriptor.delivery_guarantee
        && metadata.input_contract.payload_kind == descriptor.payload_kind
        && metadata.input_contract.payload_type_name == descriptor.payload_type_name
}

impl ConsumerPlan {
    pub(crate) fn build(
        definitions: &[QueueDefinition],
        handlers: &[HandlerDescriptor<'_>],
        trace_cells: &ValidatedTraceCells,
    ) -> Result<Self, ConsumerPlanError> {
        Self::validate_queue_definitions(definitions)?;
        Self::validate_handler_descriptors(handlers)?;

        let mut handlers_by_queue = HashMap::<&str, Vec<usize>>::with_capacity(handlers.len());
        let mut dispatch_keys = HashMap::with_capacity(handlers.len());
        for (index, handler) in handlers.iter().enumerate() {
            if dispatch_keys
                .insert(
                    (
                        handler.queue_name,
                        handler.schema_version,
                        handler.content_kind,
                    ),
                    (),
                )
                .is_some()
            {
                return Err(ConsumerPlanError::HandlerDuplicate);
            }
            handlers_by_queue
                .entry(handler.queue_name)
                .or_default()
                .push(index);
        }

        let mut bindings = Vec::with_capacity(definitions.len());
        for (queue_index, definition) in definitions.iter().enumerate() {
            let handler_indices = handlers_by_queue
                .get(definition.name.as_str())
                .ok_or(ConsumerPlanError::HandlerMissing)?;
            Self::validate_transactional_binding(definition, handler_indices, handlers)?;
            let handler_bindings = handler_indices
                .iter()
                .map(|handler_index| {
                    let handler = handlers[*handler_index];
                    Ok(ConsumerPlanHandlerBinding {
                        handler_index: *handler_index,
                        component: Self::resolve_component(handler, trace_cells.as_slice())?,
                    })
                })
                .collect::<Result<Vec<_>, ConsumerPlanError>>()?;
            bindings.push(ConsumerPlanBinding {
                queue_index,
                handlers: handler_bindings.into_boxed_slice(),
            });
        }

        Ok(Self { bindings })
    }

    fn validate_transactional_binding(
        definition: &QueueDefinition,
        handler_indices: &[usize],
        handlers: &[HandlerDescriptor<'_>],
    ) -> Result<(), ConsumerPlanError> {
        let mut has_transactional_handler = false;
        for index in handler_indices {
            let handler = handlers
                .get(*index)
                .ok_or(ConsumerPlanError::DescriptorInvalid)?;
            has_transactional_handler |=
                handler.delivery_guarantee == DeliveryGuarantee::TransactionalInbox;
        }

        #[cfg(not(any(
            feature = "transactional-inbox-postgresql",
            feature = "transactional-inbox-postgresql-factory",
            feature = "transactional-inbox-mongodb",
            feature = "transactional-inbox-mongodb-factory"
        )))]
        {
            let _ = definition;
            if has_transactional_handler {
                return Err(ConsumerPlanError::TransactionalInboxFeatureDisabled);
            }
        }

        #[cfg(any(
            feature = "transactional-inbox-postgresql",
            feature = "transactional-inbox-postgresql-factory",
            feature = "transactional-inbox-mongodb",
            feature = "transactional-inbox-mongodb-factory"
        ))]
        match (
            has_transactional_handler,
            definition.transactional_inbox.is_some(),
        ) {
            (true, false) => {
                return Err(ConsumerPlanError::TransactionalInboxBindingMissing);
            }
            (false, true) => {
                return Err(ConsumerPlanError::TransactionalInboxBindingUnused);
            }
            _ => {}
        }

        Ok(())
    }

    pub(crate) fn validate_queue_definitions(
        definitions: &[QueueDefinition],
    ) -> Result<(), ConsumerPlanError> {
        if definitions.len() > MAX_CONSUMER_QUEUES {
            return Err(ConsumerPlanError::TooManyQueues);
        }

        let mut retained_bytes = 0usize;
        for definition in definitions {
            if definition.name.is_empty()
                || definition.exchange_name.is_empty()
                || definition.routing_key.is_empty()
                || definition.name.len() > MAX_CONSUMER_DESCRIPTOR_BYTES
                || definition.exchange_name.len() > MAX_CONSUMER_DESCRIPTOR_BYTES
                || definition.routing_key.len() > MAX_CONSUMER_DESCRIPTOR_BYTES
            {
                return Err(ConsumerPlanError::DescriptorInvalid);
            }
            if definition.concurrency > MAX_CONSUMER_CONCURRENCY {
                return Err(ConsumerPlanError::QueueConcurrencyExceeded);
            }
            if definition.prefetch_count > MAX_CONSUMER_PREFETCH {
                return Err(ConsumerPlanError::QueuePrefetchExceeded);
            }
            for value in [
                Some(definition.name.as_str()),
                Some(definition.exchange_name.as_str()),
                Some(definition.routing_key.as_str()),
                definition.dead_letter_exchange.as_deref(),
                definition.dead_letter_routing_key.as_deref(),
            ]
            .into_iter()
            .flatten()
            {
                retained_bytes = retained_bytes.saturating_add(value.len());
            }
            #[cfg(any(
                feature = "transactional-inbox-postgresql",
                feature = "transactional-inbox-postgresql-factory",
                feature = "transactional-inbox-mongodb",
                feature = "transactional-inbox-mongodb-factory"
            ))]
            if let Some(database_cell) = definition
                .transactional_inbox
                .as_ref()
                .and_then(|binding| binding.database_cell.as_deref())
            {
                retained_bytes = retained_bytes.saturating_add(database_cell.len());
            }
        }
        if retained_bytes > MAX_CONSUMER_PLAN_BYTES {
            return Err(ConsumerPlanError::PlanBytesExceeded);
        }

        validate_queue_definitions(definitions)
            .map_err(|_| ConsumerPlanError::QueueConfigurationInvalid)
    }

    fn validate_handler_descriptors(
        handlers: &[HandlerDescriptor<'_>],
    ) -> Result<(), ConsumerPlanError> {
        if handlers.len() > MAX_CONSUMER_HANDLERS {
            return Err(ConsumerPlanError::TooManyHandlers);
        }
        let mut retained_bytes = 0usize;
        for handler in handlers {
            if handler.schema_version == 0 {
                return Err(ConsumerPlanError::DescriptorInvalid);
            }
            for value in [
                Some(handler.queue_name),
                Some(handler.service_type_name),
                Some(handler.method_name),
                Some(handler.handler_name),
                Some(handler.content_kind),
                handler.payload_type_name,
                handler.component_kind,
            ]
            .into_iter()
            .flatten()
            {
                if value.is_empty() || value.len() > MAX_CONSUMER_DESCRIPTOR_BYTES {
                    return Err(ConsumerPlanError::DescriptorInvalid);
                }
                retained_bytes = retained_bytes.saturating_add(value.len());
            }
        }
        if retained_bytes > MAX_CONSUMER_PLAN_BYTES {
            return Err(ConsumerPlanError::PlanBytesExceeded);
        }
        Ok(())
    }

    fn resolve_component(
        handler: HandlerDescriptor<'_>,
        trace_cells: &[TraceCellConfig],
    ) -> Result<Option<Arc<ComponentIdentity>>, ConsumerPlanError> {
        if trace_cells.is_empty() {
            return Ok(None);
        }
        let cell = trace_cells
            .iter()
            .find(|cell| cell.worker_type == handler.service_type_name);
        let Some(cell) = cell else {
            return if handler.component_kind.is_some() {
                Err(ConsumerPlanError::TraceCellMissing)
            } else {
                Ok(None)
            };
        };
        if handler
            .component_kind
            .is_some_and(|expected| cell.kind != expected)
        {
            return Err(ConsumerPlanError::TraceCellKindMismatch);
        }
        Ok(Some(Arc::new(ComponentIdentity::from_cell(cell))))
    }

    #[cfg(any(test, feature = "fuzzing"))]
    pub(crate) fn bindings(&self) -> &[ConsumerPlanBinding] {
        &self.bindings
    }
}

pub(crate) fn invalid_materialized_plan(detail: &'static str) -> MessageBrokerError {
    MessageBrokerError::RabbitMQError(RabbitMQError::Configuration(detail.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use lily_config::{ConfigService, QueueRetentionConfig};

    #[derive(Default, lily_injection::Injectable)]
    #[service(lifetime = "Singleton")]
    struct ExecutionPlanRegistryHandler;

    impl lily_injection::ServiceTrait for ExecutionPlanRegistryHandler {}

    #[lily_queue::queue_service]
    impl ExecutionPlanRegistryHandler {
        #[lily_queue::queue("capq01d.consumer-plan.alpha", version = 1, content = "none")]
        async fn alpha(&self) -> Result<(), lily_queue::QueueHandlerError> {
            Ok(())
        }

        #[lily_queue::queue("capq01d.consumer-plan.beta", version = 1, content = "none")]
        async fn beta(&self) -> Result<(), lily_queue::QueueHandlerError> {
            Ok(())
        }
    }

    #[derive(Default, lily_injection::Injectable)]
    #[service(lifetime = "Scoped")]
    struct ExecutionPlanVersionedHandler;

    impl lily_injection::ServiceTrait for ExecutionPlanVersionedHandler {}

    #[lily_queue::queue_service]
    impl ExecutionPlanVersionedHandler {
        #[lily_queue::queue("capq04.consumer-plan.versioned", version = 1, content = "json")]
        async fn version_one(
            &self,
            _payload: lily_queue::Json<serde_json::Value>,
        ) -> Result<(), lily_queue::QueueHandlerError> {
            Ok(())
        }

        #[lily_queue::queue("capq04.consumer-plan.versioned", version = 2, content = "json")]
        async fn version_two(
            &self,
            _payload: lily_queue::Json<serde_json::Value>,
        ) -> Result<(), lily_queue::QueueHandlerError> {
            Ok(())
        }

        #[lily_queue::queue("capq04.consumer-plan.versioned", version = 2, content = "binary")]
        async fn version_two_binary(
            &self,
            _payload: lily_queue::BinaryPayload,
        ) -> Result<(), lily_queue::QueueHandlerError> {
            Ok(())
        }
    }

    struct UnregisteredExecutionPlanHandler;

    #[lily_queue::queue_service]
    impl UnregisteredExecutionPlanHandler {
        #[lily_queue::queue("capq01d.consumer-plan.unregistered", version = 1, content = "none")]
        async fn handle(&self) -> Result<(), lily_queue::QueueHandlerError> {
            Ok(())
        }
    }

    struct TransactionalMetadataHandler;

    #[lily_queue::queue_service]
    impl TransactionalMetadataHandler {
        #[lily_queue::queue(
            "cap08.consumer-plan.transactional-metadata",
            version = 1,
            content = "none",
            delivery_guarantee = "transactional_inbox"
        )]
        async fn handle(&self) -> Result<(), lily_queue::QueueHandlerError> {
            Ok(())
        }
    }

    fn execution_plan_registry_handlers() -> Vec<&'static QueueHandlerMetadata> {
        lily_queue::__private::get_all_queue_handlers()
            .into_iter()
            .filter(|metadata| {
                matches!(
                    metadata.queue_name,
                    "capq01d.consumer-plan.alpha" | "capq01d.consumer-plan.beta"
                )
            })
            .collect()
    }

    fn execution_plan_versioned_handlers() -> Vec<&'static QueueHandlerMetadata> {
        let mut handlers = lily_queue::__private::get_all_queue_handlers()
            .into_iter()
            .filter(|metadata| metadata.queue_name == "capq04.consumer-plan.versioned")
            .collect::<Vec<_>>();
        handlers.sort_unstable_by_key(|metadata| (metadata.schema_version, metadata.content_kind));
        handlers
    }

    async fn execution_plan_container() -> (
        Arc<ApplicationContainer>,
        lily_queue::__private::QueueServiceTestProbe,
    ) {
        let config_path = "/tmp/lily-capq01d-consumer-execution-plan.toml";
        let config = ConfigService::development(config_path);
        let (queue_service, probe) = lily_queue::__private::queue_service_test_seed(Arc::new(
            ConfigService::development(config_path),
        ));
        let container = crate::test_application_container_builder()
            .seed_singleton(config)
            .seed_singleton(queue_service)
            .build()
            .await
            .expect("transport-free qualification container must build");
        (Arc::new(container), probe)
    }

    fn padded(prefix: &str, len: usize) -> String {
        assert!(prefix.len() <= len);
        format!("{prefix}{}", "x".repeat(len - prefix.len()))
    }

    fn queue(name: &str) -> QueueDefinition {
        QueueDefinition {
            name: name.into(),
            exchange_name: "products".into(),
            routing_key: name.into(),
            retry_backoff_millis: Some(250),
            max_retry_backoff_millis: Some(5_000),
            retention: Some(QueueRetentionConfig {
                main_max_messages: 100_000,
                main_max_bytes: 1024 * 1024 * 1024,
                retry_bucket_max_messages: 10_000,
                retry_bucket_max_bytes: 256 * 1024 * 1024,
                dead_letter_max_messages: 10_000,
                dead_letter_max_bytes: 256 * 1024 * 1024,
            }),
            ..QueueDefinition::default()
        }
    }

    fn handler<'a>(queue_name: &'a str) -> HandlerDescriptor<'a> {
        HandlerDescriptor {
            queue_name,
            service_type_name: "ProductManageWorker",
            component_kind: Some("manage_worker"),
            method_name: "handle",
            handler_name: "application::ProductManageWorker::handle",
            schema_version: 1,
            content_kind: "json",
            delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
            payload_kind: QueuePayloadKind::Json,
            payload_type_name: Some("Json<ProductRequest>"),
        }
    }

    fn cells() -> ValidatedTraceCells {
        ValidatedTraceCells::try_new(vec![TraceCellConfig {
            id: "product-worker-id".into(),
            worker_type: "ProductManageWorker".into(),
            worker_name: "product".into(),
            kind: "manage_worker".into(),
        }])
        .unwrap()
    }

    fn trace_cell(index: usize) -> TraceCellConfig {
        TraceCellConfig {
            id: format!("cell-{index}"),
            worker_type: format!("Worker{index}"),
            worker_name: format!("worker-{index}"),
            kind: "consumer".into(),
        }
    }

    #[test]
    fn builds_one_binding_per_configured_queue() {
        let definitions = [queue("product.manage.create")];
        let handlers = [handler("product.manage.create")];
        let plan = ConsumerPlan::build(&definitions, &handlers, &cells()).unwrap();
        assert_eq!(plan.bindings().len(), 1);
        assert_eq!(plan.bindings()[0].queue_index, 0);
        assert_eq!(plan.bindings()[0].handlers.len(), 1);
        assert_eq!(plan.bindings()[0].handlers[0].handler_index, 0);
        assert!(plan.bindings()[0].handlers[0].component.is_some());
    }

    #[test]
    fn real_registry_selection_freezes_exact_definition_metadata_pairs() {
        let handlers = execution_plan_registry_handlers();
        assert_eq!(handlers.len(), 2);
        let definitions = [
            queue("capq01d.consumer-plan.beta"),
            queue("capq01d.consumer-plan.alpha"),
        ];
        let descriptors = handlers
            .iter()
            .map(|handler| HandlerDescriptor::from(*handler))
            .collect::<Vec<_>>();
        let trace_cells = ValidatedTraceCells::try_new(Vec::new()).unwrap();
        let selected = ConsumerPlan::build(&definitions, &descriptors, &trace_cells).unwrap();
        let pending =
            ConsumerExecutionPlan::bind(selected, &definitions, &handlers, &descriptors).unwrap();

        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].definition.name, "capq01d.consumer-plan.beta");
        assert_eq!(pending[0].handlers.len(), 1);
        assert_eq!(
            pending[0].handlers[0].metadata.queue_name,
            "capq01d.consumer-plan.beta"
        );
        assert_eq!(pending[0].handlers[0].metadata.method_name, "beta");
        assert_eq!(pending[1].definition.name, "capq01d.consumer-plan.alpha");
        assert_eq!(
            pending[1].handlers[0].metadata.queue_name,
            "capq01d.consumer-plan.alpha"
        );
        assert_eq!(pending[1].handlers[0].metadata.method_name, "alpha");
    }

    #[test]
    fn frozen_registry_binding_rejects_a_different_metadata_slice() {
        let handlers = execution_plan_registry_handlers();
        assert_eq!(handlers.len(), 2);
        let definitions = [
            queue("capq01d.consumer-plan.alpha"),
            queue("capq01d.consumer-plan.beta"),
        ];
        let descriptors = handlers
            .iter()
            .map(|handler| HandlerDescriptor::from(*handler))
            .collect::<Vec<_>>();
        let trace_cells = ValidatedTraceCells::try_new(Vec::new()).unwrap();
        let selected = ConsumerPlan::build(&definitions, &descriptors, &trace_cells).unwrap();
        let reordered = [handlers[1], handlers[0]];

        let error = ConsumerExecutionPlan::bind(selected, &definitions, &reordered, &descriptors)
            .err()
            .expect("a different metadata slice must fail closed");
        assert_eq!(error.error_code(), "BROKER_CONFIGURATION");
    }

    #[tokio::test]
    async fn full_execution_plan_build_compiles_real_registry_without_broker_admission() {
        let (container, probe) = execution_plan_container().await;
        let handlers = execution_plan_registry_handlers();
        let definitions = [
            queue("capq01d.consumer-plan.beta"),
            queue("capq01d.consumer-plan.alpha"),
        ];
        let trace_cells = ValidatedTraceCells::try_new(Vec::new()).unwrap();

        let plan = ConsumerExecutionPlan::build(
            &definitions,
            &handlers,
            &trace_cells,
            Arc::clone(&container),
            &[],
            &[],
            std::time::Duration::from_secs(30),
        )
        .await
        .expect("every valid generated handler must compile before admission");
        let bindings = plan.into_bindings().collect::<Vec<_>>();
        assert_eq!(bindings.len(), 2);
        assert_eq!(bindings[0].definition.name, "capq01d.consumer-plan.beta");
        assert_eq!(bindings[0].handler_count, 1);
        assert_eq!(bindings[1].definition.name, "capq01d.consumer-plan.alpha");
        assert_eq!(bindings[1].handler_count, 1);
        assert_eq!(probe.registration_count(), 0);

        container
            .close()
            .await
            .expect("container must close cleanly");
    }

    #[test]
    fn same_physical_queue_freezes_all_distinct_version_content_handlers() {
        let handlers = execution_plan_versioned_handlers();
        assert_eq!(handlers.len(), 3);
        let definitions = [queue("capq04.consumer-plan.versioned")];
        let descriptors = handlers
            .iter()
            .map(|handler| HandlerDescriptor::from(*handler))
            .collect::<Vec<_>>();
        let trace_cells = ValidatedTraceCells::try_new(Vec::new()).unwrap();

        let selected = ConsumerPlan::build(&definitions, &descriptors, &trace_cells)
            .expect("distinct version/content contracts must coexist");
        assert_eq!(selected.bindings().len(), 1);
        assert_eq!(selected.bindings()[0].handlers.len(), 3);

        let pending = ConsumerExecutionPlan::bind(selected, &definitions, &handlers, &descriptors)
            .expect("exact grouped metadata slice must bind");
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].handlers.len(), 3);
    }

    #[test]
    fn same_queue_reordered_metadata_slice_fails_closed() {
        let handlers = execution_plan_versioned_handlers();
        assert_eq!(handlers.len(), 3);
        let definitions = [queue("capq04.consumer-plan.versioned")];
        let descriptors = handlers
            .iter()
            .map(|handler| HandlerDescriptor::from(*handler))
            .collect::<Vec<_>>();
        let trace_cells = ValidatedTraceCells::try_new(Vec::new()).unwrap();
        let selected = ConsumerPlan::build(&definitions, &descriptors, &trace_cells).unwrap();
        let reordered = [handlers[2], handlers[1], handlers[0]];

        let error = ConsumerExecutionPlan::bind(selected, &definitions, &reordered, &descriptors)
            .err()
            .expect("same-queue metadata substitution must fail closed");
        assert_eq!(error.error_code(), "BROKER_CONFIGURATION");
    }

    #[tokio::test]
    async fn grouped_execution_plan_compiles_three_handlers_without_broker_admission() {
        let (container, probe) = execution_plan_container().await;
        let handlers = execution_plan_versioned_handlers();
        let definitions = [queue("capq04.consumer-plan.versioned")];
        let trace_cells = ValidatedTraceCells::try_new(Vec::new()).unwrap();

        let plan = ConsumerExecutionPlan::build(
            &definitions,
            &handlers,
            &trace_cells,
            Arc::clone(&container),
            &[],
            &[],
            std::time::Duration::from_secs(30),
        )
        .await
        .expect("all versioned handlers must compile in one aggregate pass");
        let bindings = plan.into_bindings().collect::<Vec<_>>();
        assert_eq!(bindings.len(), 1);
        assert_eq!(bindings[0].handler_count, 3);
        assert_eq!(probe.registration_count(), 0);

        container.close().await.unwrap();
    }

    #[tokio::test]
    async fn later_unregistered_registry_owner_fails_before_broker_admission() {
        let (container, probe) = execution_plan_container().await;
        let registry = lily_queue::__private::get_all_queue_handlers();
        let registered = registry
            .iter()
            .copied()
            .find(|metadata| metadata.queue_name == "capq01d.consumer-plan.alpha")
            .expect("registered alpha handler metadata must exist");
        let unregistered = registry
            .iter()
            .copied()
            .find(|metadata| metadata.queue_name == "capq01d.consumer-plan.unregistered")
            .expect("unregistered owner handler metadata must exist");
        let handlers = [registered, unregistered];
        let definitions = [
            queue("capq01d.consumer-plan.alpha"),
            queue("capq01d.consumer-plan.unregistered"),
        ];
        let trace_cells = ValidatedTraceCells::try_new(Vec::new()).unwrap();

        let error = ConsumerExecutionPlan::build(
            &definitions,
            &handlers,
            &trace_cells,
            Arc::clone(&container),
            &[],
            &[],
            std::time::Duration::from_secs(30),
        )
        .await
        .err()
        .expect("a later unregistered handler owner must fail startup compilation");
        let ConsumerExecutionPlanError::Compilation(error) = error else {
            panic!("unregistered owner must fail in the canonical compiler");
        };
        assert_eq!(error.error_code(), "BROKER_CONFIGURATION");
        assert_eq!(probe.registration_count(), 0);

        container
            .close()
            .await
            .expect("container must close cleanly");
    }

    #[test]
    fn rejects_missing_and_exact_duplicate_but_accepts_distinct_dispatch_contracts() {
        let definitions = [queue("product.manage.create")];
        assert_eq!(
            ConsumerPlan::build(&definitions, &[], &cells())
                .err()
                .unwrap(),
            ConsumerPlanError::HandlerMissing
        );
        let duplicate = [
            handler("product.manage.create"),
            handler("product.manage.create"),
        ];
        assert_eq!(
            ConsumerPlan::build(&definitions, &duplicate, &cells())
                .err()
                .unwrap(),
            ConsumerPlanError::HandlerDuplicate
        );

        let version_one = handler("product.manage.create");
        let mut version_two = handler("product.manage.create");
        version_two.schema_version = 2;
        let mut version_two_binary = version_two;
        version_two_binary.content_kind = "binary";
        let plan = ConsumerPlan::build(
            &definitions,
            &[version_one, version_two, version_two_binary],
            &cells(),
        )
        .expect("distinct version/content contracts must share one physical queue");
        assert_eq!(plan.bindings().len(), 1);
        assert_eq!(plan.bindings()[0].handlers.len(), 3);
    }

    #[test]
    fn trace_validation_and_component_policy_are_single_source() {
        let duplicate = TraceCellConfig {
            id: "product-worker-id".into(),
            worker_type: "ProductManageWorker".into(),
            worker_name: "product".into(),
            kind: "manage_worker".into(),
        };
        assert_eq!(
            ValidatedTraceCells::try_new(vec![duplicate.clone(), duplicate])
                .err()
                .unwrap(),
            ConsumerPlanError::TraceConfigurationInvalid
        );

        let wrong_kind = ValidatedTraceCells::try_new(vec![TraceCellConfig {
            id: "product-worker-id".into(),
            worker_type: "ProductManageWorker".into(),
            worker_name: "product".into(),
            kind: "save_worker".into(),
        }])
        .unwrap();
        assert_eq!(
            ConsumerPlan::build(
                &[queue("product.manage.create")],
                &[handler("product.manage.create")],
                &wrong_kind,
            )
            .err()
            .unwrap(),
            ConsumerPlanError::TraceCellKindMismatch
        );
    }

    #[test]
    fn queue_resource_bounds_precede_runtime_setting_allocation() {
        let mut definition = queue("product.manage.create");
        definition.concurrency = MAX_CONSUMER_CONCURRENCY + 1;
        assert_eq!(
            ConsumerPlan::validate_queue_definitions(&[definition])
                .err()
                .unwrap(),
            ConsumerPlanError::QueueConcurrencyExceeded
        );
    }

    #[test]
    fn diagnostic_codes_and_display_are_stable() {
        for (error, code) in [
            (ConsumerPlanError::TooManyQueues, "CONSUMER_QUEUE_LIMIT"),
            (ConsumerPlanError::TooManyHandlers, "CONSUMER_HANDLER_LIMIT"),
            (
                ConsumerPlanError::TooManyTraceCells,
                "CONSUMER_TRACE_CELL_LIMIT",
            ),
            (
                ConsumerPlanError::DescriptorInvalid,
                "CONSUMER_DESCRIPTOR_INVALID",
            ),
            (
                ConsumerPlanError::PlanBytesExceeded,
                "CONSUMER_PLAN_BYTES_EXCEEDED",
            ),
            (
                ConsumerPlanError::QueueConfigurationInvalid,
                "CONSUMER_QUEUE_CONFIG_INVALID",
            ),
            (
                ConsumerPlanError::QueueConcurrencyExceeded,
                "CONSUMER_QUEUE_CONCURRENCY_EXCEEDED",
            ),
            (
                ConsumerPlanError::QueuePrefetchExceeded,
                "CONSUMER_QUEUE_PREFETCH_EXCEEDED",
            ),
            (
                ConsumerPlanError::HandlerMissing,
                "CONSUMER_HANDLER_MISSING",
            ),
            (
                ConsumerPlanError::HandlerDuplicate,
                "CONSUMER_HANDLER_DUPLICATE",
            ),
            (
                ConsumerPlanError::TransactionalInboxFeatureDisabled,
                "CONSUMER_TRANSACTIONAL_INBOX_FEATURE_DISABLED",
            ),
            (
                ConsumerPlanError::TransactionalInboxBindingMissing,
                "CONSUMER_TRANSACTIONAL_INBOX_BINDING_MISSING",
            ),
            (
                ConsumerPlanError::TransactionalInboxBindingUnused,
                "CONSUMER_TRANSACTIONAL_INBOX_BINDING_UNUSED",
            ),
            (
                ConsumerPlanError::TraceConfigurationInvalid,
                "CONSUMER_TRACE_CONFIG_INVALID",
            ),
            (
                ConsumerPlanError::TraceCellMissing,
                "CONSUMER_TRACE_CELL_MISSING",
            ),
            (
                ConsumerPlanError::TraceCellKindMismatch,
                "CONSUMER_TRACE_CELL_KIND_MISMATCH",
            ),
        ] {
            assert_eq!(error.diagnostic_code(), code);
            assert_eq!(error.to_string(), code);
        }
        assert_eq!(MAX_CONSUMER_PLAN_BYTES, 262_144);
    }

    #[test]
    fn trace_cell_count_descriptor_and_retained_byte_bounds_are_exact() {
        let at_count_limit = (0..MAX_CONSUMER_TRACE_CELLS)
            .map(trace_cell)
            .collect::<Vec<_>>();
        assert!(ValidatedTraceCells::try_new(at_count_limit).is_ok());
        let above_count_limit = (0..=MAX_CONSUMER_TRACE_CELLS)
            .map(trace_cell)
            .collect::<Vec<_>>();
        assert_eq!(
            ValidatedTraceCells::try_new(above_count_limit)
                .err()
                .unwrap(),
            ConsumerPlanError::TooManyTraceCells
        );

        let mut exact_descriptor = trace_cell(0);
        exact_descriptor.worker_name = padded("worker-", MAX_CONSUMER_DESCRIPTOR_BYTES);
        assert!(ValidatedTraceCells::try_new(vec![exact_descriptor]).is_ok());
        let mut oversized_descriptor = trace_cell(0);
        oversized_descriptor.worker_name = padded("worker-", MAX_CONSUMER_DESCRIPTOR_BYTES + 1);
        assert_eq!(
            ValidatedTraceCells::try_new(vec![oversized_descriptor])
                .err()
                .unwrap(),
            ConsumerPlanError::DescriptorInvalid
        );

        let exact_plan_bytes = (0..MAX_CONSUMER_TRACE_CELLS)
            .map(|index| TraceCellConfig {
                id: padded(&format!("id-{index}-"), 256),
                worker_type: padded(&format!("type-{index}-"), 256),
                worker_name: padded(&format!("name-{index}-"), 256),
                kind: padded("kind-", 256),
            })
            .collect::<Vec<_>>();
        assert!(ValidatedTraceCells::try_new(exact_plan_bytes.clone()).is_ok());
        let mut above_plan_bytes = exact_plan_bytes;
        above_plan_bytes[0].kind.push('x');
        assert_eq!(
            ValidatedTraceCells::try_new(above_plan_bytes)
                .err()
                .unwrap(),
            ConsumerPlanError::PlanBytesExceeded
        );
    }

    #[test]
    fn queue_count_descriptor_and_numeric_bounds_are_exact() {
        let at_count_limit = (0..MAX_CONSUMER_QUEUES)
            .map(|index| queue(&format!("queue.{index}")))
            .collect::<Vec<_>>();
        assert!(ConsumerPlan::validate_queue_definitions(&at_count_limit).is_ok());
        let above_count_limit = (0..=MAX_CONSUMER_QUEUES)
            .map(|index| queue(&format!("queue.{index}")))
            .collect::<Vec<_>>();
        assert_eq!(
            ConsumerPlan::validate_queue_definitions(&above_count_limit)
                .err()
                .unwrap(),
            ConsumerPlanError::TooManyQueues
        );

        for (name, exchange_name, routing_key) in [
            ("", "events", "queue"),
            ("queue", "", "queue"),
            ("queue", "events", ""),
        ] {
            let mut definition = queue("queue");
            definition.name = name.into();
            definition.exchange_name = exchange_name.into();
            definition.routing_key = routing_key.into();
            assert_eq!(
                ConsumerPlan::validate_queue_definitions(&[definition])
                    .err()
                    .unwrap(),
                ConsumerPlanError::DescriptorInvalid
            );
        }

        for field_is_name in [true, false] {
            let mut exact = queue("queue");
            if field_is_name {
                exact.name = padded("queue-", MAX_CONSUMER_DESCRIPTOR_BYTES);
            } else {
                exact.exchange_name = padded("exchange-", MAX_CONSUMER_DESCRIPTOR_BYTES);
                exact.routing_key = padded("routing-", MAX_CONSUMER_DESCRIPTOR_BYTES);
            }
            assert_eq!(
                ConsumerPlan::validate_queue_definitions(&[exact])
                    .err()
                    .unwrap(),
                ConsumerPlanError::QueueConfigurationInvalid
            );

            let mut oversized = queue("queue");
            if field_is_name {
                oversized.name = padded("queue-", MAX_CONSUMER_DESCRIPTOR_BYTES + 1);
            } else {
                oversized.exchange_name = padded("exchange-", MAX_CONSUMER_DESCRIPTOR_BYTES + 1);
            }
            assert_eq!(
                ConsumerPlan::validate_queue_definitions(&[oversized])
                    .err()
                    .unwrap(),
                ConsumerPlanError::DescriptorInvalid
            );
        }

        let mut exact_concurrency = queue("queue");
        exact_concurrency.concurrency = MAX_CONSUMER_CONCURRENCY;
        assert!(ConsumerPlan::validate_queue_definitions(&[exact_concurrency]).is_ok());
        let mut above_concurrency = queue("queue");
        above_concurrency.concurrency = MAX_CONSUMER_CONCURRENCY + 1;
        assert_eq!(
            ConsumerPlan::validate_queue_definitions(&[above_concurrency])
                .err()
                .unwrap(),
            ConsumerPlanError::QueueConcurrencyExceeded
        );

        let mut exact_prefetch = queue("queue");
        exact_prefetch.prefetch_count = MAX_CONSUMER_PREFETCH;
        assert!(ConsumerPlan::validate_queue_definitions(&[exact_prefetch]).is_ok());
        let mut above_prefetch = queue("queue");
        above_prefetch.prefetch_count = MAX_CONSUMER_PREFETCH + 1;
        assert_eq!(
            ConsumerPlan::validate_queue_definitions(&[above_prefetch])
                .err()
                .unwrap(),
            ConsumerPlanError::QueuePrefetchExceeded
        );
    }

    #[test]
    fn queue_retained_byte_budget_is_enforced_after_the_exact_constant() {
        let within_budget = (0..10)
            .map(|index| {
                let mut definition = queue("queue");
                definition.name = padded(&format!("queue-{index}-"), 100);
                definition.exchange_name = padded(&format!("exchange-{index}-"), 100);
                definition
            })
            .collect::<Vec<_>>();
        assert!(ConsumerPlan::validate_queue_definitions(&within_budget).is_ok());

        let at_budget = (0..MAX_CONSUMER_QUEUES)
            .map(|index| {
                let mut definition = queue("queue");
                definition.name = padded(&format!("queue-{index}-"), 200);
                definition.exchange_name = padded(&format!("exchange-{index}-"), 200);
                definition.dead_letter_exchange = Some(padded("exchange-", 255));
                definition.dead_letter_routing_key = Some(padded("routing-", 364));
                definition
            })
            .collect::<Vec<_>>();
        assert_eq!(
            ConsumerPlan::validate_queue_definitions(&at_budget)
                .err()
                .unwrap(),
            ConsumerPlanError::QueueConfigurationInvalid
        );

        let above_budget = (0..MAX_CONSUMER_QUEUES)
            .map(|index| {
                let mut definition = queue("queue");
                definition.name = padded(&format!("queue-{index}-"), 600);
                definition.exchange_name = padded(&format!("exchange-{index}-"), 600);
                definition
            })
            .collect::<Vec<_>>();
        assert_eq!(
            ConsumerPlan::validate_queue_definitions(&above_budget)
                .err()
                .unwrap(),
            ConsumerPlanError::PlanBytesExceeded
        );
    }

    #[test]
    fn handler_count_descriptor_and_retained_byte_bounds_are_exact() {
        let base = handler("queue");
        assert!(
            ConsumerPlan::validate_handler_descriptors(&vec![base; MAX_CONSUMER_HANDLERS]).is_ok()
        );
        assert_eq!(
            ConsumerPlan::validate_handler_descriptors(&vec![base; MAX_CONSUMER_HANDLERS + 1])
                .err()
                .unwrap(),
            ConsumerPlanError::TooManyHandlers
        );

        for invalid in [
            HandlerDescriptor {
                queue_name: "",
                ..base
            },
            HandlerDescriptor {
                service_type_name: "",
                ..base
            },
            HandlerDescriptor {
                method_name: "",
                ..base
            },
            HandlerDescriptor {
                handler_name: "",
                ..base
            },
            HandlerDescriptor {
                content_kind: "",
                ..base
            },
            HandlerDescriptor {
                component_kind: Some(""),
                ..base
            },
        ] {
            assert_eq!(
                ConsumerPlan::validate_handler_descriptors(&[invalid])
                    .err()
                    .unwrap(),
                ConsumerPlanError::DescriptorInvalid
            );
        }

        let exact_descriptor = padded("method-", MAX_CONSUMER_DESCRIPTOR_BYTES);
        let exact = HandlerDescriptor {
            method_name: &exact_descriptor,
            ..base
        };
        assert!(ConsumerPlan::validate_handler_descriptors(&[exact]).is_ok());
        let oversized_descriptor = padded("method-", MAX_CONSUMER_DESCRIPTOR_BYTES + 1);
        let oversized = HandlerDescriptor {
            method_name: &oversized_descriptor,
            ..base
        };
        assert_eq!(
            ConsumerPlan::validate_handler_descriptors(&[oversized])
                .err()
                .unwrap(),
            ConsumerPlanError::DescriptorInvalid
        );

        let queue_name = padded("queue-", 100);
        let service = padded("service-", 100);
        let method = padded("method-", 100);
        let handler_name = padded("handler-", 100);
        let kind = padded("kind-", 108);
        let exact_budget = HandlerDescriptor {
            queue_name: &queue_name,
            service_type_name: &service,
            component_kind: Some(&kind),
            method_name: &method,
            handler_name: &handler_name,
            payload_kind: QueuePayloadKind::None,
            payload_type_name: None,
            ..base
        };
        assert!(
            ConsumerPlan::validate_handler_descriptors(&vec![exact_budget; MAX_CONSUMER_HANDLERS])
                .is_ok()
        );
        let oversized_kind = padded("kind-", 109);
        let above_budget = HandlerDescriptor {
            component_kind: Some(&oversized_kind),
            ..exact_budget
        };
        assert_eq!(
            ConsumerPlan::validate_handler_descriptors(&vec![above_budget; MAX_CONSUMER_HANDLERS])
                .err()
                .unwrap(),
            ConsumerPlanError::PlanBytesExceeded
        );
    }

    #[test]
    fn handler_matching_preserves_order() {
        let definitions = [queue("queue.a"), queue("queue.b")];
        let handlers = [handler("queue.b"), handler("queue.a")];
        let plan = ConsumerPlan::build(&definitions, &handlers, &cells()).unwrap();
        assert_eq!(plan.bindings()[0].queue_index, 0);
        assert_eq!(plan.bindings()[0].handlers[0].handler_index, 1);
        assert_eq!(plan.bindings()[1].queue_index, 1);
        assert_eq!(plan.bindings()[1].handlers[0].handler_index, 0);
    }

    #[test]
    fn handler_descriptor_freezes_transactional_delivery_intent() {
        let metadata = lily_queue::__private::get_all_queue_handlers()
            .into_iter()
            .find(|metadata| metadata.queue_name == "cap08.consumer-plan.transactional-metadata")
            .expect("transactional metadata must be linked");
        let descriptor = HandlerDescriptor::from(metadata);

        assert_eq!(
            descriptor.delivery_guarantee,
            DeliveryGuarantee::TransactionalInbox
        );
        assert!(metadata_matches_descriptor(metadata, &descriptor));
        assert!(!metadata_matches_descriptor(
            metadata,
            &HandlerDescriptor {
                delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
                ..descriptor
            }
        ));
    }

    #[cfg(not(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory",
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-mongodb-factory"
    )))]
    #[test]
    fn transactional_handler_fails_before_admission_when_feature_is_disabled() {
        let definitions = [queue("orders.transactional")];
        let handlers = [HandlerDescriptor {
            delivery_guarantee: DeliveryGuarantee::TransactionalInbox,
            ..handler("orders.transactional")
        }];

        let error = ConsumerPlan::build(&definitions, &handlers, &cells())
            .err()
            .expect("transactional metadata without its feature must fail closed");

        assert_eq!(error, ConsumerPlanError::TransactionalInboxFeatureDisabled);
        assert_eq!(
            ConsumerPlanFailureKind::from(error).error_code(),
            "CONSUMER_TRANSACTIONAL_INBOX_FEATURE_DISABLED"
        );
    }

    #[cfg(not(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory",
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-mongodb-factory"
    )))]
    #[tokio::test]
    async fn feature_off_execution_plan_rejects_transactional_registry_before_compilation() {
        let (container, probe) = execution_plan_container().await;
        let handlers = lily_queue::__private::get_all_queue_handlers()
            .into_iter()
            .filter(|metadata| metadata.queue_name == "cap08.consumer-plan.transactional-metadata")
            .collect::<Vec<_>>();
        assert_eq!(handlers.len(), 1);
        let definitions = [queue("cap08.consumer-plan.transactional-metadata")];

        let error = ConsumerExecutionPlan::build(
            &definitions,
            &handlers,
            &ValidatedTraceCells::try_new(Vec::new()).unwrap(),
            container,
            &[],
            &[],
            std::time::Duration::from_secs(1),
        )
        .await
        .err()
        .expect("feature-off plan must reject transactional metadata");

        assert!(matches!(
            error,
            ConsumerExecutionPlanError::Selection(
                ConsumerPlanError::TransactionalInboxFeatureDisabled
            )
        ));
        assert_eq!(probe.registration_count(), 0);
    }

    #[cfg(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory"
    ))]
    #[test]
    fn transactional_handler_requires_an_explicit_queue_binding() {
        let definitions = [queue("orders.transactional")];
        let handlers = [HandlerDescriptor {
            delivery_guarantee: DeliveryGuarantee::TransactionalInbox,
            ..handler("orders.transactional")
        }];

        let error = ConsumerPlan::build(&definitions, &handlers, &cells())
            .err()
            .expect("transactional handler without a binding must fail closed");

        assert_eq!(error, ConsumerPlanError::TransactionalInboxBindingMissing);
    }

    #[cfg(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory"
    ))]
    #[test]
    fn unused_transactional_binding_fails_closed() {
        let mut definition = queue("orders.transactional");
        definition.transactional_inbox = Some(lily_config::TransactionalInboxConfig::default());
        let definitions = [definition];
        let handlers = [handler("orders.transactional")];

        let error = ConsumerPlan::build(&definitions, &handlers, &cells())
            .err()
            .expect("a binding with no transactional handler must fail closed");

        assert_eq!(error, ConsumerPlanError::TransactionalInboxBindingUnused);
    }

    #[cfg(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory"
    ))]
    #[test]
    fn matching_transactional_handler_and_binding_are_selected() {
        let mut definition = queue("orders.transactional");
        definition.transactional_inbox = Some(lily_config::TransactionalInboxConfig::default());
        let definitions = [definition];
        let handlers = [HandlerDescriptor {
            delivery_guarantee: DeliveryGuarantee::TransactionalInbox,
            ..handler("orders.transactional")
        }];

        let plan = ConsumerPlan::build(&definitions, &handlers, &cells())
            .expect("matching transactional authorities must be accepted");

        assert_eq!(plan.bindings().len(), 1);
        assert_eq!(plan.bindings()[0].handlers.len(), 1);
    }
}
