use std::{fmt, future::Future, time::Duration};

use async_trait::async_trait;
use lapin::{
    options::{ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions},
    protocol::{AMQPErrorKind, AMQPSoftError},
    types::{AMQPValue, FieldTable, ShortString},
    Channel, Connection, ExchangeKind,
};
use lily_config::{
    QueueClientCellConfig, QueueClientConfig, RabbitMqExchangeKind, RabbitMqQueueTopologyPlan,
    RabbitMqQueueType, RabbitMqTopologyConfig, RabbitMqTopologyOwnership, RabbitMqTopologyPlan,
};
use lily_error::application::{
    message_broker::{
        RabbitMQError, RabbitMqTopologyError, RabbitMqTopologyErrorKind, RabbitMqTopologyOperation,
        RabbitMqTopologyResourceKind,
    },
    MessageBrokerError,
};
use tokio_util::sync::CancellationToken;

use crate::RabbitMqOptions;

/// Explicit, one-shot RabbitMQ topology bootstrap for publisher-only processes.
///
/// Constructing this value performs validation only. [`Self::run`] opens one
/// dedicated connection and channel, applies the immutable topology plan, and
/// closes that connection before returning. Ordinary publisher startup and
/// channel acquisition never invoke this bootstrap implicitly.
#[derive(Clone)]
pub struct RabbitMqTopologyBootstrap {
    options: RabbitMqOptions,
    plan: RabbitMqTopologyPlan,
}

impl RabbitMqTopologyBootstrap {
    /// Compiles a topology bootstrap for a single-mode publisher config.
    pub fn from_client(
        client: &QueueClientConfig,
        topology: &RabbitMqTopologyConfig,
    ) -> Result<Self, MessageBrokerError> {
        Ok(Self::new(
            RabbitMqOptions::from_client(client)?,
            RabbitMqTopologyPlan::compile(topology).map_err(plan_configuration)?,
        ))
    }

    /// Compiles a topology bootstrap for one factory-mode publisher cell.
    pub fn from_cell(
        cell: &QueueClientCellConfig,
        topology: &RabbitMqTopologyConfig,
    ) -> Result<Self, MessageBrokerError> {
        Ok(Self::new(
            RabbitMqOptions::from_cell(cell)?,
            RabbitMqTopologyPlan::compile(topology).map_err(plan_configuration)?,
        ))
    }

    fn new(options: RabbitMqOptions, plan: RabbitMqTopologyPlan) -> Self {
        Self { options, plan }
    }

    /// Applies the accepted topology exactly once on a dedicated connection.
    ///
    /// Framework-managed resources are actively declared in exchange, queue,
    /// then binding order. Externally managed exchanges and queues are checked
    /// passively and their bindings remain explicitly operator-unverified.
    pub async fn run(
        &self,
        cancellation: CancellationToken,
    ) -> Result<RabbitMqTopologyBootstrapReport, MessageBrokerError> {
        if self.plan.is_empty() {
            return Ok(RabbitMqTopologyBootstrapReport::default());
        }

        let connection = self.options.connect(&cancellation).await?;
        let outcome = execute_with_deadline(
            &cancellation,
            self.options.connection_timeout(),
            execute_on_dedicated_connection(&connection, &self.plan),
        )
        .await;
        let close =
            close_dedicated_connection(&connection, self.options.connection_timeout()).await;

        match outcome {
            Err(primary) => {
                if let Err(cleanup) = close {
                    lily_trace::tracing::warn!(
                        error_code = cleanup.error_code(),
                        "RabbitMQ topology bootstrap cleanup failed after a primary error"
                    );
                }
                Err(primary)
            }
            Ok(report) => {
                close?;
                Ok(report)
            }
        }
    }
}

async fn execute_with_deadline<F>(
    cancellation: &CancellationToken,
    deadline: Duration,
    execution: F,
) -> Result<RabbitMqTopologyBootstrapReport, MessageBrokerError>
where
    F: Future<Output = Result<RabbitMqTopologyBootstrapReport, MessageBrokerError>>,
{
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => {
            Err(MessageBrokerError::RabbitMQError(RabbitMQError::Cancelled))
        }
        result = tokio::time::timeout(deadline, execution) => {
            result.map_err(|_| {
                MessageBrokerError::RabbitMQError(RabbitMQError::Timeout(
                    "topology bootstrap execution".into(),
                ))
            })?
        }
    }
}

impl fmt::Debug for RabbitMqTopologyBootstrap {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RabbitMqTopologyBootstrap")
            .field("connection", &"<redacted>")
            .field("plan", &self.plan)
            .finish()
    }
}

/// Bounded outcome counters from one explicit topology bootstrap.
///
/// `external_bindings_unverified` is deliberate evidence rather than a
/// success counter: AMQP passive declarations can prove exchange/queue
/// existence but provide no passive binding verification primitive.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RabbitMqTopologyBootstrapReport {
    managed_exchanges_declared: usize,
    managed_queues_declared: usize,
    managed_bindings_declared: usize,
    external_exchanges_verified: usize,
    external_queues_verified: usize,
    external_bindings_unverified: usize,
}

impl RabbitMqTopologyBootstrapReport {
    /// Number of unique framework-managed exchanges actively declared.
    pub const fn managed_exchanges_declared(self) -> usize {
        self.managed_exchanges_declared
    }

    /// Number of framework-managed physical queues actively declared.
    pub const fn managed_queues_declared(self) -> usize {
        self.managed_queues_declared
    }

    /// Number of framework-managed bindings actively declared.
    pub const fn managed_bindings_declared(self) -> usize {
        self.managed_bindings_declared
    }

    /// Number of unique externally managed exchanges passively verified.
    pub const fn external_exchanges_verified(self) -> usize {
        self.external_exchanges_verified
    }

    /// Number of externally managed physical queues passively verified.
    pub const fn external_queues_verified(self) -> usize {
        self.external_queues_verified
    }

    /// Number of externally managed bindings that remain operator-unverified.
    pub const fn external_bindings_unverified(self) -> usize {
        self.external_bindings_unverified
    }
}

/// Applies an already accepted topology plan on a caller-owned RabbitMQ channel.
///
/// This hidden cross-crate seam lets `lily_queue` use the same declaration and
/// passive-verification authority as explicit publisher bootstrap. It neither
/// opens nor closes the supplied channel.
#[doc(hidden)]
pub async fn execute_rabbitmq_topology_plan(
    channel: &Channel,
    plan: &RabbitMqTopologyPlan,
) -> Result<RabbitMqTopologyBootstrapReport, MessageBrokerError> {
    let transport = LapinTopologyTransport { channel };
    execute_with_transport(&transport, plan).await
}

async fn execute_on_dedicated_connection(
    connection: &Connection,
    plan: &RabbitMqTopologyPlan,
) -> Result<RabbitMqTopologyBootstrapReport, MessageBrokerError> {
    let channel = connection.create_channel().await.map_err(|error| {
        MessageBrokerError::RabbitMQError(RabbitMQError::Lapin(format!(
            "failed to create RabbitMQ topology bootstrap channel: {error}"
        )))
    })?;
    execute_rabbitmq_topology_plan(&channel, plan).await
}

async fn close_dedicated_connection(
    connection: &Connection,
    timeout: Duration,
) -> Result<(), MessageBrokerError> {
    tokio::time::timeout(
        timeout,
        connection.close(200, "Lily topology bootstrap complete".into()),
    )
    .await
    .map_err(|_| {
        MessageBrokerError::RabbitMQError(RabbitMQError::Timeout(
            "topology bootstrap connection close".into(),
        ))
    })?
    .map_err(|error| {
        MessageBrokerError::RabbitMQError(RabbitMQError::Lapin(format!(
            "failed to close RabbitMQ topology bootstrap connection: {error}"
        )))
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ExchangeSpec {
    name: String,
    kind: RabbitMqExchangeKind,
    ownership: RabbitMqTopologyOwnership,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct QueueSpec {
    name: String,
    queue_type: RabbitMqQueueType,
    ownership: RabbitMqTopologyOwnership,
    durable: bool,
    exclusive: bool,
    auto_delete: bool,
    max_messages: u64,
    max_bytes: u64,
    dead_letter_exchange: Option<String>,
    dead_letter_routing_key: Option<String>,
    message_ttl_millis: Option<u64>,
    single_active_consumer: bool,
    max_priority: Option<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct BindingSpec {
    queue: String,
    exchange: String,
    routing_key: String,
    ownership: RabbitMqTopologyOwnership,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct TopologyMaterialization {
    exchanges: Vec<ExchangeSpec>,
    queues: Vec<QueueSpec>,
    bindings: Vec<BindingSpec>,
}

impl TopologyMaterialization {
    fn from_plan(plan: &RabbitMqTopologyPlan) -> Self {
        let mut materialized = Self::default();
        for queue in plan.queues() {
            materialized.push_queue_plan(queue);
        }
        materialized
    }

    fn push_queue_plan(&mut self, queue: &RabbitMqQueueTopologyPlan) {
        let definition = queue.definition();
        let ownership = queue.topology_ownership();
        self.push_exchange(queue.exchange_name(), queue.exchange_kind(), ownership);
        if !queue.retry_buckets().is_empty() {
            self.push_exchange(
                queue.retry_exchange_name(),
                RabbitMqExchangeKind::Direct,
                ownership,
            );
        }
        self.push_exchange(
            queue.dead_letter_exchange_name(),
            RabbitMqExchangeKind::Direct,
            ownership,
        );

        let retention = definition
            .retention
            .as_ref()
            .expect("accepted topology plan has explicit retention");
        self.queues.push(QueueSpec {
            name: queue.main_queue_name().to_owned(),
            queue_type: queue.system_queue_type(),
            ownership,
            durable: definition.durable,
            exclusive: definition.exclusive,
            auto_delete: definition.auto_delete,
            max_messages: retention.main_max_messages,
            max_bytes: retention.main_max_bytes,
            dead_letter_exchange: Some(queue.dead_letter_exchange_name().to_owned()),
            dead_letter_routing_key: Some(queue.dead_letter_routing_key().to_owned()),
            message_ttl_millis: definition.message_ttl_ms.map(u64::from),
            single_active_consumer: definition.single_active_consumer,
            max_priority: definition.max_priority,
        });
        self.bindings.push(BindingSpec {
            queue: queue.main_queue_name().to_owned(),
            exchange: queue.exchange_name().to_owned(),
            routing_key: queue.routing_key().to_owned(),
            ownership,
        });

        for bucket in queue.retry_buckets() {
            self.queues.push(QueueSpec {
                name: bucket.queue_name().to_owned(),
                queue_type: bucket.queue_type(),
                ownership,
                durable: true,
                exclusive: false,
                auto_delete: false,
                max_messages: retention.retry_bucket_max_messages,
                max_bytes: retention.retry_bucket_max_bytes,
                dead_letter_exchange: Some(queue.exchange_name().to_owned()),
                dead_letter_routing_key: Some(queue.routing_key().to_owned()),
                message_ttl_millis: Some(bucket.delay_millis()),
                single_active_consumer: false,
                max_priority: None,
            });
            self.bindings.push(BindingSpec {
                queue: bucket.queue_name().to_owned(),
                exchange: queue.retry_exchange_name().to_owned(),
                routing_key: bucket.routing_key().to_owned(),
                ownership,
            });
        }

        self.queues.push(QueueSpec {
            name: queue.dead_letter_queue_name().to_owned(),
            queue_type: queue.system_queue_type(),
            ownership,
            durable: true,
            exclusive: false,
            auto_delete: false,
            max_messages: retention.dead_letter_max_messages,
            max_bytes: retention.dead_letter_max_bytes,
            dead_letter_exchange: None,
            dead_letter_routing_key: None,
            message_ttl_millis: None,
            single_active_consumer: false,
            max_priority: None,
        });
        self.bindings.push(BindingSpec {
            queue: queue.dead_letter_queue_name().to_owned(),
            exchange: queue.dead_letter_exchange_name().to_owned(),
            routing_key: queue.dead_letter_routing_key().to_owned(),
            ownership,
        });
    }

    fn push_exchange(
        &mut self,
        name: &str,
        kind: RabbitMqExchangeKind,
        ownership: RabbitMqTopologyOwnership,
    ) {
        if self.exchanges.iter().all(|exchange| exchange.name != name) {
            self.exchanges.push(ExchangeSpec {
                name: name.to_owned(),
                kind,
                ownership,
            });
        }
    }
}

#[async_trait]
trait TopologyTransport: Send + Sync {
    async fn declare_exchange(&self, exchange: &ExchangeSpec) -> Result<(), lapin::Error>;
    async fn verify_exchange(&self, exchange: &ExchangeSpec) -> Result<(), lapin::Error>;
    async fn declare_queue(&self, queue: &QueueSpec) -> Result<(), lapin::Error>;
    async fn verify_queue(&self, queue: &QueueSpec) -> Result<(), lapin::Error>;
    async fn bind_queue(&self, binding: &BindingSpec) -> Result<(), lapin::Error>;
}

struct LapinTopologyTransport<'a> {
    channel: &'a Channel,
}

#[async_trait]
impl TopologyTransport for LapinTopologyTransport<'_> {
    async fn declare_exchange(&self, exchange: &ExchangeSpec) -> Result<(), lapin::Error> {
        self.channel
            .exchange_declare(
                ShortString::from(exchange.name.clone()),
                exchange_kind(exchange.kind),
                ExchangeDeclareOptions {
                    durable: true,
                    auto_delete: false,
                    internal: false,
                    nowait: false,
                    passive: false,
                },
                FieldTable::default(),
            )
            .await
    }

    async fn verify_exchange(&self, exchange: &ExchangeSpec) -> Result<(), lapin::Error> {
        self.channel
            .exchange_declare(
                ShortString::from(exchange.name.clone()),
                exchange_kind(exchange.kind),
                ExchangeDeclareOptions {
                    passive: true,
                    ..ExchangeDeclareOptions::default()
                },
                FieldTable::default(),
            )
            .await
    }

    async fn declare_queue(&self, queue: &QueueSpec) -> Result<(), lapin::Error> {
        self.channel
            .queue_declare(
                ShortString::from(queue.name.clone()),
                QueueDeclareOptions {
                    durable: queue.durable,
                    exclusive: queue.exclusive,
                    auto_delete: queue.auto_delete,
                    nowait: false,
                    passive: false,
                },
                queue_arguments(queue),
            )
            .await
            .map(|_| ())
    }

    async fn verify_queue(&self, queue: &QueueSpec) -> Result<(), lapin::Error> {
        self.channel
            .queue_declare(
                ShortString::from(queue.name.clone()),
                QueueDeclareOptions {
                    passive: true,
                    ..QueueDeclareOptions::default()
                },
                FieldTable::default(),
            )
            .await
            .map(|_| ())
    }

    async fn bind_queue(&self, binding: &BindingSpec) -> Result<(), lapin::Error> {
        self.channel
            .queue_bind(
                ShortString::from(binding.queue.clone()),
                ShortString::from(binding.exchange.clone()),
                ShortString::from(binding.routing_key.clone()),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
    }
}

async fn execute_with_transport<T: TopologyTransport>(
    transport: &T,
    plan: &RabbitMqTopologyPlan,
) -> Result<RabbitMqTopologyBootstrapReport, MessageBrokerError> {
    let topology = TopologyMaterialization::from_plan(plan);
    let mut report = RabbitMqTopologyBootstrapReport::default();

    for exchange in &topology.exchanges {
        match exchange.ownership {
            RabbitMqTopologyOwnership::FrameworkManaged => {
                transport
                    .declare_exchange(exchange)
                    .await
                    .map_err(|error| {
                        topology_error(
                            error,
                            RabbitMqTopologyOperation::Declare,
                            RabbitMqTopologyResourceKind::Exchange,
                            &exchange.name,
                        )
                    })?;
                report.managed_exchanges_declared += 1;
            }
            RabbitMqTopologyOwnership::External => {
                transport.verify_exchange(exchange).await.map_err(|error| {
                    topology_error(
                        error,
                        RabbitMqTopologyOperation::Verify,
                        RabbitMqTopologyResourceKind::Exchange,
                        &exchange.name,
                    )
                })?;
                report.external_exchanges_verified += 1;
            }
        }
    }

    for queue in &topology.queues {
        match queue.ownership {
            RabbitMqTopologyOwnership::FrameworkManaged => {
                transport.declare_queue(queue).await.map_err(|error| {
                    topology_error(
                        error,
                        RabbitMqTopologyOperation::Declare,
                        RabbitMqTopologyResourceKind::Queue,
                        &queue.name,
                    )
                })?;
                report.managed_queues_declared += 1;
            }
            RabbitMqTopologyOwnership::External => {
                transport.verify_queue(queue).await.map_err(|error| {
                    topology_error(
                        error,
                        RabbitMqTopologyOperation::Verify,
                        RabbitMqTopologyResourceKind::Queue,
                        &queue.name,
                    )
                })?;
                report.external_queues_verified += 1;
            }
        }
    }

    for binding in &topology.bindings {
        match binding.ownership {
            RabbitMqTopologyOwnership::FrameworkManaged => {
                transport.bind_queue(binding).await.map_err(|error| {
                    topology_error(
                        error,
                        RabbitMqTopologyOperation::Bind,
                        RabbitMqTopologyResourceKind::Binding,
                        &binding.queue,
                    )
                })?;
                report.managed_bindings_declared += 1;
            }
            RabbitMqTopologyOwnership::External => {
                report.external_bindings_unverified += 1;
            }
        }
    }

    Ok(report)
}

fn exchange_kind(kind: RabbitMqExchangeKind) -> ExchangeKind {
    match kind {
        RabbitMqExchangeKind::Direct => ExchangeKind::Direct,
    }
}

fn queue_arguments(queue: &QueueSpec) -> FieldTable {
    let mut arguments = FieldTable::default();
    let queue_type = match queue.queue_type {
        RabbitMqQueueType::Classic => "classic",
        RabbitMqQueueType::Quorum => "quorum",
    };
    arguments.insert(
        "x-queue-type".into(),
        AMQPValue::LongString(queue_type.into()),
    );
    arguments.insert(
        "x-max-length".into(),
        AMQPValue::LongLongInt(
            i64::try_from(queue.max_messages).expect("accepted topology retention count"),
        ),
    );
    arguments.insert(
        "x-max-length-bytes".into(),
        AMQPValue::LongLongInt(
            i64::try_from(queue.max_bytes).expect("accepted topology retention bytes"),
        ),
    );
    arguments.insert(
        "x-overflow".into(),
        AMQPValue::LongString("reject-publish".into()),
    );
    if let Some(exchange) = &queue.dead_letter_exchange {
        arguments.insert(
            "x-dead-letter-exchange".into(),
            AMQPValue::LongString(exchange.clone().into()),
        );
    }
    if let Some(routing_key) = &queue.dead_letter_routing_key {
        arguments.insert(
            "x-dead-letter-routing-key".into(),
            AMQPValue::LongString(routing_key.clone().into()),
        );
    }
    if let Some(ttl) = queue.message_ttl_millis {
        arguments.insert(
            "x-message-ttl".into(),
            AMQPValue::LongLongInt(i64::try_from(ttl).expect("accepted topology message TTL")),
        );
    }
    if queue.single_active_consumer {
        arguments.insert("x-single-active-consumer".into(), AMQPValue::Boolean(true));
    }
    if let Some(priority) = queue.max_priority {
        arguments.insert(
            "x-max-priority".into(),
            AMQPValue::LongInt(i32::from(priority)),
        );
    }
    arguments
}

fn topology_error(
    error: lapin::Error,
    operation: RabbitMqTopologyOperation,
    resource_kind: RabbitMqTopologyResourceKind,
    resource_name: &str,
) -> MessageBrokerError {
    let kind = match error.kind() {
        lapin::ErrorKind::ProtocolError(protocol) => match protocol.kind() {
            AMQPErrorKind::Soft(AMQPSoftError::ACCESSREFUSED) => {
                Some(RabbitMqTopologyErrorKind::PermissionDenied)
            }
            AMQPErrorKind::Soft(AMQPSoftError::NOTFOUND)
                if operation == RabbitMqTopologyOperation::Verify =>
            {
                Some(RabbitMqTopologyErrorKind::PassiveResourceNotFound)
            }
            AMQPErrorKind::Soft(AMQPSoftError::PRECONDITIONFAILED) => {
                Some(RabbitMqTopologyErrorKind::DeclarationMismatch)
            }
            _ => None,
        },
        _ => None,
    };

    if let Some(kind) = kind {
        MessageBrokerError::RabbitMQError(RabbitMQError::Topology(RabbitMqTopologyError {
            kind,
            operation,
            resource_kind,
            resource_name: resource_name.to_owned(),
        }))
    } else {
        MessageBrokerError::RabbitMQError(RabbitMQError::Lapin(format!(
            "RabbitMQ topology {operation} failed for {resource_kind} {resource_name:?}"
        )))
    }
}

fn plan_configuration(error: impl fmt::Display) -> MessageBrokerError {
    MessageBrokerError::RabbitMQError(RabbitMQError::Configuration(error.to_string()))
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::sync::Mutex;

    use lapin::protocol::AMQPError;
    use lily_config::{QueueDefinition, QueueRetentionConfig};

    use super::*;

    #[derive(Clone, Debug, PartialEq, Eq)]
    enum RecordedOperation {
        DeclareExchange(String),
        VerifyExchange(String),
        DeclareQueue(String),
        VerifyQueue(String),
        Bind(String),
    }

    #[derive(Default)]
    struct RecordingTransport {
        operations: Mutex<Vec<RecordedOperation>>,
    }

    impl RecordingTransport {
        fn record(&self, operation: RecordedOperation) {
            self.operations.lock().unwrap().push(operation);
        }

        fn operations(&self) -> Vec<RecordedOperation> {
            self.operations.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl TopologyTransport for RecordingTransport {
        async fn declare_exchange(&self, exchange: &ExchangeSpec) -> Result<(), lapin::Error> {
            self.record(RecordedOperation::DeclareExchange(exchange.name.clone()));
            Ok(())
        }

        async fn verify_exchange(&self, exchange: &ExchangeSpec) -> Result<(), lapin::Error> {
            self.record(RecordedOperation::VerifyExchange(exchange.name.clone()));
            Ok(())
        }

        async fn declare_queue(&self, queue: &QueueSpec) -> Result<(), lapin::Error> {
            self.record(RecordedOperation::DeclareQueue(queue.name.clone()));
            Ok(())
        }

        async fn verify_queue(&self, queue: &QueueSpec) -> Result<(), lapin::Error> {
            self.record(RecordedOperation::VerifyQueue(queue.name.clone()));
            Ok(())
        }

        async fn bind_queue(&self, binding: &BindingSpec) -> Result<(), lapin::Error> {
            self.record(RecordedOperation::Bind(binding.queue.clone()));
            Ok(())
        }
    }

    fn retention() -> QueueRetentionConfig {
        QueueRetentionConfig {
            main_max_messages: 100,
            main_max_bytes: 1024 * 1024,
            retry_bucket_max_messages: 20,
            retry_bucket_max_bytes: 256 * 1024,
            dead_letter_max_messages: 50,
            dead_letter_max_bytes: 512 * 1024,
        }
    }

    fn managed_definition(name: &str) -> QueueDefinition {
        QueueDefinition {
            name: name.into(),
            exchange_name: "managed.events".into(),
            routing_key: name.into(),
            retry_attempts: 2,
            retention: Some(retention()),
            ..QueueDefinition::default()
        }
    }

    fn external_definition(name: &str) -> QueueDefinition {
        QueueDefinition {
            name: name.into(),
            exchange_name: "external.events".into(),
            routing_key: name.into(),
            topology_ownership: RabbitMqTopologyOwnership::External,
            retention: Some(retention()),
            dead_letter_exchange: Some("external.failures".into()),
            dead_letter_routing_key: Some(format!("{name}.failed")),
            ..QueueDefinition::default()
        }
    }

    fn publisher_config() -> QueueClientConfig {
        QueueClientConfig {
            connection_string: Some("amqp://guest:guest@127.0.0.1:5672/%2f".into()),
            use_tls: Some(false),
            ..QueueClientConfig::default()
        }
    }

    #[test]
    fn publisher_bootstrap_rejects_managed_exclusive_queue_before_broker_io() {
        let mut exclusive = managed_definition("exclusive.events");
        exclusive.exclusive = true;
        let error = RabbitMqTopologyBootstrap::from_client(
            &publisher_config(),
            &RabbitMqTopologyConfig {
                queues: vec![exclusive],
            },
        )
        .unwrap_err();

        assert!(matches!(
            error,
            MessageBrokerError::RabbitMQError(RabbitMQError::Configuration(ref message))
                if message.contains("exclusive") && message.contains("exclusive.events")
        ));
    }

    #[tokio::test]
    async fn publisher_bootstrap_execution_has_an_aggregate_deadline() {
        let cancellation = CancellationToken::new();
        let error = execute_with_deadline(
            &cancellation,
            Duration::from_millis(10),
            pending::<Result<RabbitMqTopologyBootstrapReport, MessageBrokerError>>(),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            MessageBrokerError::RabbitMQError(RabbitMQError::Timeout(ref operation))
                if operation == "topology bootstrap execution"
        ));
    }

    #[tokio::test]
    async fn publisher_bootstrap_cancellation_preempts_a_ready_execution() {
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let error = execute_with_deadline(
            &cancellation,
            Duration::from_secs(1),
            std::future::ready(Ok(RabbitMqTopologyBootstrapReport::default())),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            MessageBrokerError::RabbitMQError(RabbitMQError::Cancelled)
        ));
    }

    #[tokio::test]
    async fn managed_resources_are_ordered_and_external_resources_are_never_mutated() {
        let plan = RabbitMqTopologyPlan::compile(&RabbitMqTopologyConfig {
            queues: vec![
                managed_definition("orders.created"),
                external_definition("payments.created"),
            ],
        })
        .unwrap();
        let transport = RecordingTransport::default();

        let report = execute_with_transport(&transport, &plan).await.unwrap();
        let operations = transport.operations();

        let first_queue = operations
            .iter()
            .position(|operation| {
                matches!(
                    operation,
                    RecordedOperation::DeclareQueue(_) | RecordedOperation::VerifyQueue(_)
                )
            })
            .unwrap();
        let first_binding = operations
            .iter()
            .position(|operation| matches!(operation, RecordedOperation::Bind(_)))
            .unwrap();
        assert!(operations[..first_queue].iter().all(|operation| matches!(
            operation,
            RecordedOperation::DeclareExchange(_) | RecordedOperation::VerifyExchange(_)
        )));
        assert!(operations[first_queue..first_binding]
            .iter()
            .all(|operation| matches!(
                operation,
                RecordedOperation::DeclareQueue(_) | RecordedOperation::VerifyQueue(_)
            )));
        assert!(operations[first_binding..]
            .iter()
            .all(|operation| matches!(operation, RecordedOperation::Bind(_))));

        assert!(!operations.iter().any(|operation| matches!(
            operation,
            RecordedOperation::DeclareExchange(name)
                | RecordedOperation::DeclareQueue(name)
                | RecordedOperation::Bind(name)
                if name.starts_with("external") || name.starts_with("payments")
        )));
        assert_eq!(report.external_bindings_unverified(), 2);
        assert_eq!(report.external_exchanges_verified(), 2);
        assert_eq!(report.external_queues_verified(), 2);
        assert_eq!(report.managed_exchanges_declared(), 3);
        assert_eq!(report.managed_queues_declared(), 4);
        assert_eq!(report.managed_bindings_declared(), 4);
    }

    #[test]
    fn exact_queue_materialization_inherits_only_the_accepted_system_policy() {
        let mut definition = managed_definition("orders.created");
        definition.queue_type = RabbitMqQueueType::Quorum;
        definition.durable = true;
        definition.single_active_consumer = true;
        definition.message_ttl_ms = Some(30_000);
        let plan = RabbitMqTopologyPlan::compile(&RabbitMqTopologyConfig {
            queues: vec![definition],
        })
        .unwrap();

        let topology = TopologyMaterialization::from_plan(&plan);
        let main = &topology.queues[0];
        assert_eq!(main.queue_type, RabbitMqQueueType::Quorum);
        assert!(main.durable);
        assert!(main.single_active_consumer);
        assert_eq!(main.message_ttl_millis, Some(30_000));
        assert_eq!(main.max_messages, 100);
        assert_eq!(main.max_bytes, 1024 * 1024);

        for system in &topology.queues[1..] {
            assert_eq!(system.queue_type, RabbitMqQueueType::Quorum);
            assert!(system.durable);
            assert!(!system.exclusive);
            assert!(!system.auto_delete);
            assert!(!system.single_active_consumer);
            assert_eq!(system.max_priority, None);
        }
    }

    #[test]
    fn classic_non_durable_main_keeps_retry_and_dead_letter_queues_durable() {
        let mut definition = managed_definition("ephemeral.events");
        definition.durable = false;
        definition.retry_attempts = 1;
        let plan = RabbitMqTopologyPlan::compile(&RabbitMqTopologyConfig {
            queues: vec![definition],
        })
        .unwrap();

        let topology = TopologyMaterialization::from_plan(&plan);
        assert!(!topology.queues[0].durable);
        assert!(topology.queues[1..].iter().all(|queue| queue.durable));
        assert!(topology.queues[1..]
            .iter()
            .all(|queue| !queue.exclusive && !queue.auto_delete));
    }

    #[test]
    fn shared_exchange_declarations_are_deduplicated() {
        let mut second = managed_definition("orders.updated");
        second.retry_attempts = 0;
        let mut first = managed_definition("orders.created");
        first.retry_attempts = 0;
        let plan = RabbitMqTopologyPlan::compile(&RabbitMqTopologyConfig {
            queues: vec![first, second],
        })
        .unwrap();

        let topology = TopologyMaterialization::from_plan(&plan);
        assert_eq!(
            topology
                .exchanges
                .iter()
                .filter(|exchange| exchange.name == "managed.events")
                .count(),
            1
        );
        assert_eq!(
            topology
                .exchanges
                .iter()
                .filter(|exchange| exchange.name == "managed.events.dlx.v2")
                .count(),
            1
        );
    }

    #[test]
    fn protocol_reply_kinds_map_without_retaining_broker_reply_text() {
        for (soft, operation, expected) in [
            (
                AMQPSoftError::ACCESSREFUSED,
                RabbitMqTopologyOperation::Declare,
                RabbitMqTopologyErrorKind::PermissionDenied,
            ),
            (
                AMQPSoftError::NOTFOUND,
                RabbitMqTopologyOperation::Verify,
                RabbitMqTopologyErrorKind::PassiveResourceNotFound,
            ),
            (
                AMQPSoftError::PRECONDITIONFAILED,
                RabbitMqTopologyOperation::Declare,
                RabbitMqTopologyErrorKind::DeclarationMismatch,
            ),
        ] {
            let provider = lapin::ErrorKind::ProtocolError(AMQPError::new(
                AMQPErrorKind::Soft(soft),
                "RAW_BROKER_REPLY".into(),
            ))
            .into();
            let mapped = topology_error(
                provider,
                operation,
                RabbitMqTopologyResourceKind::Queue,
                "orders.created",
            );
            assert!(matches!(
                mapped,
                MessageBrokerError::RabbitMQError(RabbitMQError::Topology(
                    RabbitMqTopologyError { kind, .. }
                )) if kind == expected
            ));
            assert!(!mapped.to_string().contains("RAW_BROKER_REPLY"));
        }
    }

    #[test]
    fn unmapped_protocol_reply_keeps_context_without_retaining_broker_reply_text() {
        let provider = lapin::ErrorKind::ProtocolError(AMQPError::new(
            AMQPErrorKind::Soft(AMQPSoftError::RESOURCELOCKED),
            "RAW_BROKER_REPLY_WITH_SECRET".into(),
        ))
        .into();

        let mapped = topology_error(
            provider,
            RabbitMqTopologyOperation::Declare,
            RabbitMqTopologyResourceKind::Queue,
            "orders.created",
        );

        assert_eq!(
            mapped,
            MessageBrokerError::RabbitMQError(RabbitMQError::Lapin(
                "RabbitMQ topology declare failed for queue \"orders.created\"".into(),
            ))
        );
        assert!(!mapped.to_string().contains("RAW_BROKER_REPLY_WITH_SECRET"));
    }
}
