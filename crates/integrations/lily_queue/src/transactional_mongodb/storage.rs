use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::owned_tasks::{TaskReceipt, TransactionTasks};
use bytes::Bytes;
use futures::FutureExt as _;
use futures::TryStreamExt as _;
use lily_mongo_repository::MongoOperationContext;
use lily_config::{
    MongoTransactionalInboxConfig, TransactionalInboxBackend, TransactionalInboxConfig,
};
use lily_error::application::QueueHandlerError;
use lily_injection::{ApplicationContainer, Extensions};
use lily_mongodb::{DatabaseService, MongoDbError};
use mongodb::Collection;
use mongodb::bson::{Binary, Bson, DateTime, Document, doc, spec::BinarySubtype};
use mongodb::options::{ReturnDocument, ValidationAction, ValidationLevel};
use mongodb::results::{CollectionSpecification, CollectionType};
use sha2::{Digest, Sha256};
use tokio::sync::{Notify, oneshot};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::MongoReliabilityError;
use super::migration::{
    INBOX_COLLECTION, INBOX_COMPLETED_INDEX, INBOX_IDENTITY_INDEX, LEASE_COLLECTION,
    LEASE_EXPIRY_INDEX, MAX_FAILURE_CODE_BYTES, MIGRATION_COMPONENT, OUTBOX_COLLECTION,
    OUTBOX_DELIVERED_INDEX, OUTBOX_EVENT_INDEX, OUTBOX_READY_INDEX, SCHEMA_COLLECTION,
    SCHEMA_FINGERPRINT, SCHEMA_VERSION, inbox_validator, lease_validator, outbox_validator,
};
use crate::transactional::{
    CleanupReport, MAX_OUTBOX_MESSAGE_BYTES, MAX_OUTBOX_TRANSPORT_IDENTITY_BYTES,
    TransactionalExecution, TransactionalInboxSnapshot, TransactionalOutboxMessage,
    handler_identity_is_valid,
};

const INBOX_PROCESSING: &str = "processing";
const INBOX_COMPLETED: &str = "completed";
// MongoDB's wire protocol rejects a BSON document larger than 16 MiB. The
// shared outbox contract permits a 16 MiB body, so the adapter must inspect the
// final encoded document rather than assuming the body-only bound is enough.
const MAX_MONGODB_BSON_DOCUMENT_BYTES: usize = 16 * 1024 * 1024;
const MAX_EXTERNAL_LEASE_POLL_INTERVAL: Duration = Duration::from_millis(100);
const MAX_TRANSACTION_FINALIZATION_RESERVE: Duration = Duration::from_millis(100);
const REQUIRED_COLLECTIONS: [&str; 4] = [
    SCHEMA_COLLECTION,
    INBOX_COLLECTION,
    LEASE_COLLECTION,
    OUTBOX_COLLECTION,
];

fn transaction_execution_deadlines(
    delivery_deadline: Instant,
    now: Instant,
) -> (Instant, Instant, Instant) {
    let remaining = delivery_deadline.saturating_duration_since(now);
    let reserve = (remaining / 4).min(MAX_TRANSACTION_FINALIZATION_RESERVE);
    let body_deadline = delivery_deadline
        .checked_sub(reserve)
        .unwrap_or(delivery_deadline);
    (body_deadline, delivery_deadline, delivery_deadline)
}

#[derive(Default)]
struct TransactionExecutionLedger {
    body_attempts_total: AtomicU64,
    transient_body_retries_total: AtomicU64,
    commit_attempts_total: AtomicU64,
    unknown_commit_retries_total: AtomicU64,
    transaction_retry_exhausted_total: AtomicU64,
    commit_outcome_unknown_total: AtomicU64,
    lease_contentions_total: AtomicU64,
    post_commit_release_failures_total: AtomicU64,
    last_failure_code: Mutex<Option<&'static str>>,
}

impl TransactionExecutionLedger {
    fn snapshot(&self, active_transactions: usize) -> TransactionalInboxSnapshot {
        TransactionalInboxSnapshot {
            active_transactions: u64::try_from(active_transactions).unwrap_or(u64::MAX),
            body_attempts_total: self.body_attempts_total.load(Ordering::Acquire),
            transient_body_retries_total: self.transient_body_retries_total.load(Ordering::Acquire),
            commit_attempts_total: self.commit_attempts_total.load(Ordering::Acquire),
            unknown_commit_retries_total: self.unknown_commit_retries_total.load(Ordering::Acquire),
            transaction_retry_exhausted_total: self
                .transaction_retry_exhausted_total
                .load(Ordering::Acquire),
            commit_outcome_unknown_total: self.commit_outcome_unknown_total.load(Ordering::Acquire),
            lease_contentions_total: self.lease_contentions_total.load(Ordering::Acquire),
            post_commit_release_failures_total: self
                .post_commit_release_failures_total
                .load(Ordering::Acquire),
            last_failure_code: *self
                .last_failure_code
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        }
    }

    fn record(&self, counter: &AtomicU64) {
        saturating_increment(counter);
    }

    fn record_failure(&self, code: &'static str) {
        *self
            .last_failure_code
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(code);
    }
}

fn saturating_increment(counter: &AtomicU64) {
    let mut current = counter.load(Ordering::Acquire);
    while current != u64::MAX {
        match counter.compare_exchange_weak(
            current,
            current.saturating_add(1),
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return,
            Err(observed) => current = observed,
        }
    }
}

enum InboxClaimState {
    Acquired(Arc<str>),
    AlreadyCompleted,
    InProgress,
}

pub(crate) enum MongoDeliveryExecution<T> {
    Completed(TransactionalExecution<T>),
    ContentionDeferred,
    CommitOutcomeDeferred,
    Cancelled,
}

enum ExternalLeaseAdmission {
    Acquired,
    AlreadyCompleted,
    InProgress,
    ContentionDeferred,
}

/// One durable MongoDB outbox document exclusively leased to a relay.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaimedMongoOutboxMessage {
    record_id: Uuid,
    event_id: Uuid,
    exchange: Arc<str>,
    routing_key: Arc<str>,
    schema_version: u16,
    content_kind: Arc<str>,
    content_type: Arc<str>,
    body: Bytes,
    traceparent: Option<Arc<str>>,
    claim_token: Uuid,
    publish_attempts: u64,
}

impl ClaimedMongoOutboxMessage {
    /// Lily-owned durable record identity.
    #[must_use]
    pub const fn record_id(&self) -> Uuid {
        self.record_id
    }

    /// Stable application event identity retained across publish attempts.
    #[must_use]
    pub const fn event_id(&self) -> Uuid {
        self.event_id
    }

    /// RabbitMQ exchange destination.
    #[must_use]
    pub fn exchange(&self) -> &str {
        &self.exchange
    }

    /// RabbitMQ routing-key destination.
    #[must_use]
    pub fn routing_key(&self) -> &str {
        &self.routing_key
    }

    /// Positive Lily envelope schema version.
    #[must_use]
    pub const fn schema_version(&self) -> u16 {
        self.schema_version
    }

    /// Canonical Lily content-kind token.
    #[must_use]
    pub fn content_kind(&self) -> &str {
        &self.content_kind
    }

    /// Canonical MIME value paired with the content kind.
    #[must_use]
    pub fn content_type(&self) -> &str {
        &self.content_type
    }

    /// Serialized durable payload.
    #[must_use]
    pub fn body(&self) -> &[u8] {
        &self.body
    }

    /// Captured W3C trace parent, when present.
    #[must_use]
    pub fn traceparent(&self) -> Option<&str> {
        self.traceparent.as_deref()
    }

    /// Opaque claim fencing token.
    #[must_use]
    pub const fn claim_token(&self) -> Uuid {
        self.claim_token
    }

    /// Persisted attempts before this record enters its next broker operation.
    #[must_use]
    pub const fn publish_attempts(&self) -> u64 {
        self.publish_attempts
    }
}

/// Handler-side access to the exact MongoDB session owning inbox completion.
///
/// Every business mutation covered by the effectively-once guarantee must use
/// [`Self::operation_context`], and every covered outgoing event must use
/// [`Self::enqueue`]. An independently resolved collection operation without
/// this context is intentionally outside the guarantee.
#[derive(Clone)]
pub struct MongoTransaction {
    database: Arc<DatabaseService>,
    transaction: Arc<lily_mongo_repository::MongoTransaction>,
    cancellation: CancellationToken,
    source_handler: Arc<str>,
    source_event_id: Uuid,
    max_outbox_bytes: usize,
}

impl MongoTransaction {
    /// Creates a repository/collection operation context bound to this exact
    /// transaction and its framework-owned cancellation lifecycle.
    pub fn operation_context(&self) -> Result<MongoOperationContext<'_>, MongoReliabilityError> {
        self.database
            .operation_context(self.cancellation.clone())
            .map(|operation| operation.with_transaction(&self.transaction))
            .map_err(Into::into)
    }

    /// Inserts one durable event in the active business transaction.
    pub async fn enqueue(
        &self,
        message: TransactionalOutboxMessage,
    ) -> Result<(), MongoReliabilityError> {
        if message.body.len() > self.max_outbox_bytes {
            return Err(MongoReliabilityError::InvalidContract {
                code: "QUEUE_OUTBOX_POLICY_BYTES_EXCEEDED",
            });
        }

        let operation = self.operation_context()?;
        let collection = self.database.collection::<Document>(OUTBOX_COLLECTION)?;
        let record_id = Uuid::new_v4().to_string();
        let maximum_uuid = Uuid::nil().to_string();
        let mut document = doc! {
            "_id": &record_id,
            "source_handler_identity": self.source_handler.as_ref(),
            "source_event_id": self.source_event_id.to_string(),
            "event_id": message.event_id.to_string(),
            "exchange_name": message.exchange.as_ref(),
            "routing_key": message.routing_key.as_ref(),
            "schema_version": i32::from(message.schema_version),
            "content_kind": message.content_kind.as_ref(),
            "content_type": message.content_type.as_ref(),
            "body": Binary { subtype: BinarySubtype::Generic, bytes: message.body.to_vec() },
            "traceparent": message.traceparent.as_deref(),
            // These placeholders are replaced with the database server's
            // single operation time before the transaction can commit.
            "available_at": DateTime::from_millis(0),
            // Size the maximum legal future representation, not merely the
            // smaller unclaimed row inserted below. Otherwise a document near
            // MongoDB's BSON ceiling could be accepted here and fail later
            // when claim fencing and failure metadata are materialized.
            "claim_owner": &maximum_uuid,
            "claim_token": &maximum_uuid,
            "claimed_until": DateTime::from_millis(0),
            "publish_attempts": 0_i64,
            "last_failure_code": "X".repeat(MAX_FAILURE_CODE_BYTES),
            "created_at": DateTime::from_millis(0),
            "delivered_at": DateTime::from_millis(0),
        };
        let encoded_size = mongodb::bson::to_vec(&document)
            .map_err(MongoDbError::from)?
            .len();
        if encoded_size > MAX_MONGODB_BSON_DOCUMENT_BYTES {
            return Err(MongoReliabilityError::InvalidContract {
                code: "QUEUE_MONGODB_OUTBOX_DOCUMENT_TOO_LARGE",
            });
        }
        document.insert("claim_owner", Bson::Null);
        document.insert("claim_token", Bson::Null);
        document.insert("claimed_until", Bson::Null);
        document.insert("last_failure_code", Bson::Null);
        document.insert("delivered_at", Bson::Null);
        operation
            .execute(async {
                let mut session = self.transaction.lock_session().await;
                collection
                    .insert_one(document)
                    .session(&mut *session)
                    .await
                    .map(|_| ())
                    .map_err(|error| operation.map_driver_error(error))
            })
            .await
            .map_err(map_outbox_write_error)?;
        operation
            .execute(async {
                let mut session = self.transaction.lock_session().await;
                collection
                    .update_one(
                        doc! { "_id": &record_id },
                        doc! { "$currentDate": { "available_at": true, "created_at": true } },
                    )
                    .session(&mut *session)
                    .await
                    .map(|_| ())
                    .map_err(|error| operation.map_driver_error(error))
            })
            .await
            .map_err(map_outbox_write_error)
    }
}

impl crate::FromDeliveryParts for MongoTransaction {
    type Rejection = QueueHandlerError;

    async fn from_delivery_parts(
        invocation: &mut crate::DeliveryInvocation,
    ) -> Result<Self, Self::Rejection> {
        invocation.remove_local::<Self>().ok_or_else(|| {
            QueueHandlerError::permanent("QUEUE_MONGODB_TRANSACTION_CONTEXT_UNAVAILABLE")
        })
    }
}

struct TransactionTracker {
    accepting: AtomicBool,
    active: AtomicUsize,
    changed: Notify,
    force: CancellationToken,
    owners: Arc<TransactionTasks>,
}

impl Default for TransactionTracker {
    fn default() -> Self {
        Self {
            accepting: AtomicBool::new(false),
            active: AtomicUsize::new(0),
            changed: Notify::new(),
            force: CancellationToken::new(),
            owners: Arc::default(),
        }
    }
}

impl TransactionTracker {
    fn open(&self) {
        self.accepting.store(true, Ordering::Release);
    }

    fn begin(self: &Arc<Self>, owner: Uuid) -> Result<TransactionLease, MongoReliabilityError> {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(MongoReliabilityError::TransactionAdmissionClosed);
        }
        self.active.fetch_add(1, Ordering::AcqRel);
        if !self.accepting.load(Ordering::Acquire) {
            self.finish(owner);
            return Err(MongoReliabilityError::TransactionAdmissionClosed);
        }
        Ok(TransactionLease {
            tracker: Arc::clone(self),
            owner,
        })
    }

    fn finish(&self, _owner: Uuid) {
        if self.active.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.changed.notify_waiters();
        }
    }
}

struct TransactionLease {
    tracker: Arc<TransactionTracker>,
    owner: Uuid,
}

struct LeaseHeartbeat {
    stop: CancellationToken,
    handle: Option<TaskReceipt>,
}

struct ExternalLeaseHeartbeatContext {
    database: Arc<DatabaseService>,
    collection: Collection<Document>,
    key: String,
    owner: Uuid,
    lease_millis: u64,
    stop: CancellationToken,
    force: CancellationToken,
    deadline: Option<Instant>,
    lost: Arc<AtomicBool>,
}

#[derive(Clone)]
struct TransactionExecutionContext {
    database: Arc<DatabaseService>,
    config: TransactionalInboxConfig,
    policy: MongoTransactionalInboxConfig,
    logical_handler: Arc<str>,
    event_id: Uuid,
    force: CancellationToken,
    delivery_cancellation: CancellationToken,
    body_deadline: Option<Instant>,
    work_deadline: Option<Instant>,
    cleanup_deadline: Option<Instant>,
    ledger: Arc<TransactionExecutionLedger>,
    owners: Arc<TransactionTasks>,
}

impl LeaseHeartbeat {
    fn new(stop: CancellationToken, handle: TaskReceipt) -> Self {
        Self {
            stop,
            handle: Some(handle),
        }
    }

    async fn stop_before(
        self,
        graceful_deadline: Option<Instant>,
        final_deadline: Option<Instant>,
    ) -> bool {
        self.stop.cancel();
        let Some(handle) = self.handle.as_ref() else {
            return true;
        };
        if let Some(deadline) = graceful_deadline {
            if tokio::time::timeout_at(deadline, handle.join())
                .await
                .is_err()
            {
                handle.abort();
                return match final_deadline {
                    Some(deadline) => tokio::time::timeout_at(deadline, handle.join())
                        .await
                        .is_ok(),
                    None => handle.terminal(),
                };
            }
        } else {
            let _ = handle.join().await;
        }
        true
    }
}

impl Drop for LeaseHeartbeat {
    fn drop(&mut self) {
        self.stop.cancel();
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

impl Drop for TransactionLease {
    fn drop(&mut self) {
        self.tracker.finish(self.owner);
    }
}

/// Prepared MongoDB transactional inbox/outbox runtime.
///
/// Construction resolves an already initialized application-owned MongoDB
/// service, verifies transaction topology and inspects the explicit migration
/// artifact. It never provisions storage.
pub struct MongoTransactionalRuntime {
    database: Arc<DatabaseService>,
    config: TransactionalInboxConfig,
    mongo_policy: MongoTransactionalInboxConfig,
    queue_identity: Arc<str>,
    database_cell: Option<Arc<str>>,
    transactions: Arc<TransactionTracker>,
    execution_ledger: Arc<TransactionExecutionLedger>,
}

impl MongoTransactionalRuntime {
    /// Resolves the configured MongoDB authority from one application.
    pub async fn resolve(
        container: Arc<ApplicationContainer>,
        config: TransactionalInboxConfig,
    ) -> Result<Self, MongoReliabilityError> {
        Self::resolve_extensions(container.services(), config).await
    }

    async fn resolve_extensions(
        extensions: Arc<Extensions>,
        config: TransactionalInboxConfig,
    ) -> Result<Self, MongoReliabilityError> {
        #[cfg(feature = "transactional-inbox-mongodb")]
        let database = {
            if config.database_cell.is_some() {
                return Err(MongoReliabilityError::InvalidContract {
                    code: "QUEUE_MONGODB_SINGLE_CELL_FORBIDDEN",
                });
            }
            extensions
                .get_service::<DatabaseService>(None)
                .await
                .map_err(|_| MongoReliabilityError::DependencyUnavailable)?
        };

        #[cfg(feature = "transactional-inbox-mongodb-factory")]
        let database =
            {
                let cell = config.database_cell.as_deref().ok_or(
                    MongoReliabilityError::InvalidContract {
                        code: "QUEUE_MONGODB_FACTORY_CELL_REQUIRED",
                    },
                )?;
                let factory = extensions
                    .get_service::<lily_mongodb::MongoFactory>(None)
                    .await
                    .map_err(|_| MongoReliabilityError::DependencyUnavailable)?;
                factory
                    .get(cell)
                    .ok_or(MongoReliabilityError::DependencyUnavailable)?
            };

        Self::from_database(database, config).await
    }

    /// Prepares a runtime from an already selected application-owned service.
    pub async fn from_database(
        database: Arc<DatabaseService>,
        config: TransactionalInboxConfig,
    ) -> Result<Self, MongoReliabilityError> {
        validate_policy(&config)?;
        database
            .verify_transaction_capability()
            .await
            .map_err(map_topology_error)?;
        ensure_schema_ready(&database).await?;
        let mongo_policy = config.mongodb.clone().unwrap_or_default();
        let database_cell = config.database_cell.as_deref().map(Arc::from);
        let transactions = Arc::new(TransactionTracker::default());
        transactions.open();
        Ok(Self {
            database,
            config,
            mongo_policy,
            queue_identity: Arc::from("manual"),
            database_cell,
            transactions,
            execution_ledger: Arc::new(TransactionExecutionLedger::default()),
        })
    }

    pub(crate) fn bind_queue_identity(&mut self, queue: &str) {
        self.queue_identity = Arc::from(queue);
    }

    /// Performs the read-only schema and deployment checks used before broker
    /// admission.
    pub async fn ensure_schema_ready(&self) -> Result<(), MongoReliabilityError> {
        self.database
            .verify_transaction_capability()
            .await
            .map_err(map_topology_error)?;
        ensure_schema_ready(&self.database).await
    }

    /// Exact physical queue owning this prepared binding.
    #[must_use]
    pub fn queue_identity(&self) -> &str {
        &self.queue_identity
    }

    /// Exact factory cell, or `None` for single mode.
    #[must_use]
    pub fn database_cell(&self) -> Option<&str> {
        self.database_cell.as_deref()
    }

    /// Validated bounded policy for this queue binding.
    #[must_use]
    pub(crate) const fn config(&self) -> &TransactionalInboxConfig {
        &self.config
    }

    /// Number of admitted transaction owners not yet finalized.
    #[must_use]
    pub fn active_transactions(&self) -> usize {
        self.transactions.active.load(Ordering::Acquire)
    }

    /// Returns payload-free, sampling-independent transaction execution evidence.
    #[must_use]
    pub fn transactional_inbox_snapshot(&self) -> TransactionalInboxSnapshot {
        self.execution_ledger.snapshot(self.active_transactions())
    }

    /// Rejects new transaction owners without cancelling admitted work.
    pub fn stop_transaction_admission(&self) {
        self.transactions.accepting.store(false, Ordering::Release);
        self.transactions.changed.notify_waiters();
    }

    /// Waits until every admitted owner commits or aborts.
    pub async fn drain_transactions(&self, timeout: Duration) -> Result<(), MongoReliabilityError> {
        drain_tracker(&self.transactions, timeout).await
    }

    /// Cancels active operations, then aborts owners which exceed the bounded
    /// force budget. This never reopens admission.
    pub async fn force_drain_transactions(
        &self,
        timeout: Duration,
    ) -> Result<(), MongoReliabilityError> {
        signal_force_drain(&self.transactions);
        drain_tracker(&self.transactions, timeout).await
    }

    pub(crate) fn set_shutdown_deadlines(
        &self,
        deadlines: crate::shutdown_budget::QueueShutdownDeadlines,
    ) {
        self.transactions.owners.budget.install(deadlines);
    }

    pub(crate) fn request_force(&self) {
        signal_force_drain(&self.transactions);
    }

    pub(crate) fn transactions_reconciled(&self) -> bool {
        self.active_transactions() == 0 && self.transactions.owners.tasks.reconciled()
    }

    /// Owns the complete MongoDB transaction lifecycle for one delivery.
    ///
    /// The operation is replayable because `TransientTransactionError`
    /// requires a fresh session and a complete body rerun. Application code
    /// must therefore keep every covered side effect inside the supplied
    /// transaction context.
    pub async fn execute<T, Operation, OperationFuture>(
        &self,
        logical_handler: impl AsRef<str>,
        event_id: Uuid,
        operation: Operation,
    ) -> Result<TransactionalExecution<T>, QueueHandlerError>
    where
        T: Send + 'static,
        Operation: Fn(MongoTransaction) -> OperationFuture + Send + Sync + 'static,
        OperationFuture: Future<Output = Result<T, QueueHandlerError>> + Send + 'static,
    {
        let operation = move |transaction, _body_deadline| operation(transaction);
        match self
            .execute_with_budget(
                logical_handler,
                event_id,
                None,
                CancellationToken::new(),
                operation,
            )
            .await?
        {
            MongoDeliveryExecution::Completed(result) => Ok(result),
            MongoDeliveryExecution::ContentionDeferred => {
                // Standalone callers have no broker settlement deadline, so
                // the internal contention-deferred state is unreachable.
                Err(QueueHandlerError::retryable(
                    "QUEUE_MONGODB_INBOX_CONTENTION_DEFERRED",
                ))
            }
            MongoDeliveryExecution::CommitOutcomeDeferred => Err(QueueHandlerError::retryable(
                MongoReliabilityError::CommitOutcomeUnknown.code(),
            )),
            MongoDeliveryExecution::Cancelled => Err(QueueHandlerError::retryable(
                MongoReliabilityError::TransactionCancelled.code(),
            )),
        }
    }

    pub(crate) async fn execute_delivery<T, Operation, OperationFuture>(
        &self,
        logical_handler: impl AsRef<str>,
        event_id: Uuid,
        delivery_deadline: Instant,
        delivery_cancellation: CancellationToken,
        operation: Operation,
    ) -> Result<MongoDeliveryExecution<T>, QueueHandlerError>
    where
        T: Send + 'static,
        Operation: Fn(MongoTransaction, Instant) -> OperationFuture + Send + Sync + 'static,
        OperationFuture: Future<Output = Result<T, QueueHandlerError>> + Send + 'static,
    {
        let operation = move |transaction, body_deadline: Option<Instant>| {
            operation(transaction, body_deadline.unwrap_or(delivery_deadline))
        };
        self.execute_with_budget(
            logical_handler,
            event_id,
            Some(delivery_deadline),
            delivery_cancellation,
            operation,
        )
        .await
    }

    async fn execute_with_budget<T, Operation, OperationFuture>(
        &self,
        logical_handler: impl AsRef<str>,
        event_id: Uuid,
        delivery_deadline: Option<Instant>,
        delivery_cancellation: CancellationToken,
        operation: Operation,
    ) -> Result<MongoDeliveryExecution<T>, QueueHandlerError>
    where
        T: Send + 'static,
        Operation: Fn(MongoTransaction, Option<Instant>) -> OperationFuture + Send + Sync + 'static,
        OperationFuture: Future<Output = Result<T, QueueHandlerError>> + Send + 'static,
    {
        let logical_handler = logical_handler.as_ref();
        if !handler_identity_is_valid(logical_handler) || event_id.is_nil() {
            return Err(QueueHandlerError::permanent(
                "QUEUE_MONGODB_INBOX_IDENTITY_INVALID",
            ));
        }

        let owner = Uuid::new_v4();
        let lease = self
            .transactions
            .begin(owner)
            .map_err(reliability_as_handler)?;
        let (result_tx, result_rx) = oneshot::channel();
        let (body_deadline, work_deadline, cleanup_deadline) =
            delivery_deadline.map_or((None, None, None), |deadline| {
                let (body, work, cleanup) =
                    transaction_execution_deadlines(deadline, Instant::now());
                (Some(body), Some(work), Some(cleanup))
            });
        // Driver cancellation follows disappearance of the delivery waiter,
        // not the read-only execution notification received by user callbacks.
        let _waiter = delivery_cancellation.clone().drop_guard();
        let context = TransactionExecutionContext {
            database: Arc::clone(&self.database),
            config: self.config.clone(),
            policy: self.mongo_policy.clone(),
            logical_handler: Arc::from(logical_handler),
            event_id,
            force: self.transactions.force.child_token(),
            delivery_cancellation,
            body_deadline,
            work_deadline,
            cleanup_deadline,
            ledger: Arc::clone(&self.execution_ledger),
            owners: Arc::clone(&self.transactions.owners),
        };

        let owners = Arc::clone(&self.transactions.owners);
        let owner_task = self.transactions.owners.tasks.spawn(async move {
            let _lease = lease;
            let result = owners
                .run(
                    cleanup_deadline,
                    AssertUnwindSafe(execute_owned(context, owner, operation)).catch_unwind(),
                )
                .await;
            let result = match result {
                Some(Ok(result)) => result,
                Some(Err(_)) => {
                    owners.tasks.record_panic();
                    Err(QueueHandlerError::retryable(
                        "QUEUE_MONGODB_TRANSACTION_OWNER_PANICKED",
                    ))
                }
                None => Err(QueueHandlerError::retryable(
                    "QUEUE_MONGODB_TRANSACTION_OWNER_INTERRUPTED",
                )),
            };
            let _ = result_tx.send(result);
        });
        let result = result_rx.await.unwrap_or_else(|_| {
            Err(QueueHandlerError::retryable(
                "QUEUE_MONGODB_TRANSACTION_OWNER_INTERRUPTED",
            ))
        });
        if owner_task.join().await.is_err() {
            return Err(QueueHandlerError::retryable(
                "QUEUE_MONGODB_TRANSACTION_OWNER_JOIN_FAILED",
            ));
        }
        result
    }

    /// Claims a deterministic, byte/count-bounded outbox batch.
    pub async fn claim_outbox_batch(
        &self,
        claim_owner: Uuid,
    ) -> Result<Vec<ClaimedMongoOutboxMessage>, MongoReliabilityError> {
        if claim_owner.is_nil() {
            return Err(invalid("QUEUE_OUTBOX_CLAIM_TOKEN_INVALID"));
        }
        let collection = self.database.collection::<Document>(OUTBOX_COLLECTION)?;
        let claim_lease_millis = i64::try_from(self.config.outbox_claim_lease_millis)
            .map_err(|_| invalid("QUEUE_MONGODB_DURATION_INVALID"))?;
        let claim_owner = claim_owner.to_string();
        let mut retained_bytes = 0_usize;
        let mut records = Vec::with_capacity(self.config.relay_batch_size);

        for _ in 0..self.config.relay_batch_size {
            let claim_token = Uuid::new_v4();
            let claim_token_text = claim_token.to_string();
            let row = bounded_database_operation(&self.database, CancellationToken::new(), async {
                collection
                    .find_one_and_update(
                        doc! {
                            "delivered_at": Bson::Null,
                            "publish_attempts": {
                                "$lt": i64::from(self.config.relay_max_publish_attempts)
                            },
                            "$expr": { "$and": [
                                { "$lte": ["$available_at", "$$NOW"] },
                                { "$or": [
                                    { "$eq": [
                                        { "$ifNull": ["$claimed_until", Bson::Null] },
                                        Bson::Null
                                    ] },
                                    { "$lte": ["$claimed_until", "$$NOW"] }
                                ] }
                            ] },
                        },
                        vec![doc! { "$set": {
                            "claim_owner": { "$literal": &claim_owner },
                            "claim_token": { "$literal": &claim_token_text },
                            "claimed_until": {
                                "$add": ["$$NOW", claim_lease_millis]
                            },
                        }}],
                    )
                    .sort(doc! { "created_at": 1, "_id": 1 })
                    .return_document(ReturnDocument::After)
                    .await
                    .map_err(MongoDbError::from)
            })
            .await?;
            let Some(row) = row else { break };
            let record = map_claimed_outbox(row)?;
            let next_bytes = retained_bytes
                .checked_add(record.body.len())
                .ok_or_else(|| invalid("QUEUE_OUTBOX_BATCH_BYTES_INVALID"))?;
            if next_bytes > self.config.relay_max_in_flight_bytes {
                release_outbox_claim(
                    &self.database,
                    &collection,
                    record.record_id,
                    record.claim_token,
                )
                .await?;
                if records.is_empty() {
                    return Err(MongoReliabilityError::OutboxRecordTooLarge {
                        bytes: record.body.len(),
                        maximum: self.config.relay_max_in_flight_bytes,
                    });
                }
                break;
            }
            retained_bytes = next_bytes;
            records.push(record);
        }
        Ok(records)
    }

    /// Persists one broker attempt and renews its exact fenced lease.
    pub async fn begin_outbox_publish(
        &self,
        record_id: Uuid,
        claim_token: Uuid,
    ) -> Result<u64, MongoReliabilityError> {
        let collection = self.database.collection::<Document>(OUTBOX_COLLECTION)?;
        let claim_lease_millis = i64::try_from(self.config.outbox_claim_lease_millis)
            .map_err(|_| invalid("QUEUE_MONGODB_DURATION_INVALID"))?;
        let row = bounded_database_operation(&self.database, CancellationToken::new(), async {
            collection
                .find_one_and_update(
                    doc! {
                        "_id": record_id.to_string(),
                        "claim_token": claim_token.to_string(),
                        "delivered_at": Bson::Null,
                        "publish_attempts": {
                            "$lt": i64::from(self.config.relay_max_publish_attempts)
                        },
                    },
                    vec![doc! { "$set": {
                        "publish_attempts": { "$add": ["$publish_attempts", 1_i64] },
                        "claimed_until": { "$add": ["$$NOW", claim_lease_millis] },
                    }}],
                )
                .return_document(ReturnDocument::After)
                .await
                .map_err(MongoDbError::from)
        })
        .await?
        .ok_or(MongoReliabilityError::OutboxClaimLost)?;
        positive_u64(&row, "publish_attempts")
    }

    /// Marks a row delivered after Ack-without-Return broker confirmation.
    pub async fn mark_outbox_delivered(
        &self,
        record_id: Uuid,
        claim_token: Uuid,
    ) -> Result<(), MongoReliabilityError> {
        let collection = self.database.collection::<Document>(OUTBOX_COLLECTION)?;
        let result = bounded_database_operation(&self.database, CancellationToken::new(), async {
            collection
                .update_one(
                    doc! {
                        "_id": record_id.to_string(),
                        "claim_token": claim_token.to_string(),
                        "delivered_at": Bson::Null,
                        "publish_attempts": { "$gt": 0_i64 },
                    },
                    doc! {
                        "$currentDate": { "delivered_at": true },
                        "$set": {
                            "claim_owner": Bson::Null,
                            "claim_token": Bson::Null,
                            "claimed_until": Bson::Null,
                            "last_failure_code": Bson::Null,
                        },
                    },
                )
                .await
                .map_err(MongoDbError::from)
        })
        .await?;
        exactly_one(result.modified_count)
    }

    /// Releases a failed publish for a bounded delayed retry.
    pub async fn record_outbox_failure(
        &self,
        record_id: Uuid,
        claim_token: Uuid,
        retry_after: Duration,
        failure_code: &'static str,
    ) -> Result<(), MongoReliabilityError> {
        if !failure_code_is_valid(failure_code) {
            return Err(invalid("QUEUE_OUTBOX_FAILURE_CODE_INVALID"));
        }
        let delay = i64::try_from(retry_after.as_millis())
            .map_err(|_| invalid("QUEUE_OUTBOX_RETRY_DELAY_INVALID"))?;
        let collection = self.database.collection::<Document>(OUTBOX_COLLECTION)?;
        let result = bounded_database_operation(&self.database, CancellationToken::new(), async {
            collection
                .update_one(
                    doc! {
                        "_id": record_id.to_string(),
                        "claim_token": claim_token.to_string(),
                        "delivered_at": Bson::Null,
                        "publish_attempts": { "$gt": 0_i64 },
                    },
                    vec![doc! { "$set": {
                        "available_at": { "$add": ["$$NOW", delay] },
                        "last_failure_code": { "$literal": failure_code },
                        "claim_owner": Bson::Null,
                        "claim_token": Bson::Null,
                        "claimed_until": Bson::Null,
                    }}],
                )
                .await
                .map_err(MongoDbError::from)
        })
        .await?;
        exactly_one(result.modified_count)
    }

    /// Counts retained undelivered rows which exhausted their publish budget.
    pub async fn exhausted_outbox_count(&self) -> Result<u64, MongoReliabilityError> {
        let collection = self.database.collection::<Document>(OUTBOX_COLLECTION)?;
        bounded_database_operation(&self.database, CancellationToken::new(), async {
            collection
                .count_documents(doc! {
                    "delivered_at": Bson::Null,
                    "publish_attempts": {
                        "$gte": i64::from(self.config.relay_max_publish_attempts)
                    },
                })
                .await
                .map_err(MongoDbError::from)
        })
        .await
    }

    /// Deletes at most one configured batch from each completed ledger.
    pub async fn cleanup(&self) -> Result<CleanupReport, MongoReliabilityError> {
        let inbox_retention_millis =
            duration_seconds_as_i64_millis(self.config.inbox_retention_secs)?;
        let outbox_retention_millis =
            duration_seconds_as_i64_millis(self.config.outbox_retention_secs)?;
        let limit = i64::try_from(self.config.relay_batch_size)
            .map_err(|_| invalid("QUEUE_MONGODB_CLEANUP_BATCH_INVALID"))?;
        let inbox = self.database.collection::<Document>(INBOX_COLLECTION)?;
        let leases = self.database.collection::<Document>(LEASE_COLLECTION)?;
        let outbox = self.database.collection::<Document>(OUTBOX_COLLECTION)?;

        let inbox_ids = bounded_ids(
            &self.database,
            &inbox,
            doc! {
                "state": INBOX_COMPLETED,
                "$expr": { "$lt": [
                    "$completed_at",
                    { "$subtract": ["$$NOW", inbox_retention_millis] }
                ] }
            },
            limit,
        )
        .await?;
        let outbox_ids = bounded_ids(
            &self.database,
            &outbox,
            doc! {
                "delivered_at": { "$ne": Bson::Null },
                "$expr": { "$lt": [
                    "$delivered_at",
                    { "$subtract": ["$$NOW", outbox_retention_millis] }
                ] }
            },
            limit,
        )
        .await?;
        let expired_lease_ids = bounded_ids(
            &self.database,
            &leases,
            doc! { "$expr": { "$lt": ["$expires_at", "$$NOW"] } },
            limit,
        )
        .await?;
        let inbox_rows = delete_ids(&self.database, &inbox, inbox_ids).await?;
        let outbox_rows = delete_ids(&self.database, &outbox, outbox_ids).await?;
        let _expired_leases =
            delete_expired_leases(&self.database, &leases, expired_lease_ids).await?;
        Ok(CleanupReport {
            inbox_rows,
            outbox_rows,
        })
    }
}

async fn execute_owned<T, Operation, OperationFuture>(
    context: TransactionExecutionContext,
    owner: Uuid,
    operation: Operation,
) -> Result<MongoDeliveryExecution<T>, QueueHandlerError>
where
    T: Send + 'static,
    Operation: Fn(MongoTransaction, Option<Instant>) -> OperationFuture + Send + Sync + 'static,
    OperationFuture: Future<Output = Result<T, QueueHandlerError>> + Send + 'static,
{
    let lease_key = inbox_key(&context.logical_handler, context.event_id);
    let lease_collection = context
        .database
        .collection::<Document>(LEASE_COLLECTION)
        .map_err(|error| reliability_as_handler(error.into()))?;
    let admission = match await_external_lease(&context, &lease_collection, &lease_key, owner).await
    {
        Ok(admission) => admission,
        Err(_) if context.force.is_cancelled() || context.delivery_cancellation.is_cancelled() => {
            return Ok(MongoDeliveryExecution::Cancelled);
        }
        Err(error) => return Err(error),
    };
    match admission {
        ExternalLeaseAdmission::Acquired => {}
        ExternalLeaseAdmission::AlreadyCompleted => {
            return Ok(MongoDeliveryExecution::Completed(
                TransactionalExecution::AlreadyCompleted,
            ));
        }
        ExternalLeaseAdmission::InProgress => {
            return Ok(MongoDeliveryExecution::Completed(
                TransactionalExecution::InProgress,
            ));
        }
        ExternalLeaseAdmission::ContentionDeferred => {
            return Ok(MongoDeliveryExecution::ContentionDeferred);
        }
    }

    let heartbeat_stop = CancellationToken::new();
    let lease_lost = Arc::new(AtomicBool::new(false));
    let heartbeat = LeaseHeartbeat::new(
        heartbeat_stop.clone(),
        context
            .owners
            .tasks
            .spawn(heartbeat_external_lease(ExternalLeaseHeartbeatContext {
                database: Arc::clone(&context.database),
                collection: lease_collection.clone(),
                key: lease_key.clone(),
                owner,
                lease_millis: context.config.inbox_lock_timeout_millis,
                stop: heartbeat_stop.clone(),
                force: context.force.clone(),
                deadline: context.work_deadline,
                lost: Arc::clone(&lease_lost),
            })),
    );

    let result = execute_attempts(&context, &lease_lost, operation).await;

    let final_deadline = context.cleanup_deadline;
    let graceful_deadline = final_deadline.map(|deadline| {
        let now = Instant::now();
        now + deadline.saturating_duration_since(now) / 2
    });
    let heartbeat_joined = heartbeat
        .stop_before(graceful_deadline, final_deadline)
        .await;
    let retain_lease_for_reconciliation = retain_external_lease_after(&result);
    let release = if !heartbeat_joined {
        // Do not remove a lease while an unconfirmed child may still renew it.
        // The registry retains its real join for runtime reconciliation.
        Err(MongoReliabilityError::TransactionTerminationIncomplete)
    } else if retain_lease_for_reconciliation {
        lily_trace::tracing::warn!(
            lily.error_code = MongoReliabilityError::CommitOutcomeUnknown.code(),
            lily.storage.backend = "mongodb",
            lily.transaction.phase = "external_lease_release",
            lily.outcome = "retained_until_expiry",
            "transactional inbox retained its external lease after an unknown commit outcome"
        );
        Ok(())
    } else {
        release_external_lease_before(&context, &lease_collection, &lease_key, owner).await
    };
    let delivery_execution = context.body_deadline.is_some();
    let cancelled = context.force.is_cancelled() || context.delivery_cancellation.is_cancelled();
    let primary = retain_primary_transaction_result(&context.ledger, result, release);
    if cancelled
        && delivery_execution
        && matches!(&primary, Err(TransactionAttemptError::Handler(_)))
    {
        Ok(MongoDeliveryExecution::Cancelled)
    } else {
        match primary {
            Ok(result) => Ok(classify_delivery_execution(result, delivery_execution)),
            Err(TransactionAttemptError::CommitOutcomeUnknown) if delivery_execution => {
                Ok(MongoDeliveryExecution::CommitOutcomeDeferred)
            }
            Err(TransactionAttemptError::CommitOutcomeUnknown) => Err(reliability_as_handler(
                MongoReliabilityError::CommitOutcomeUnknown,
            )),
            Err(TransactionAttemptError::Handler(error)) => Err(error),
        }
    }
}

fn retain_external_lease_after<T>(
    result: &Result<TransactionalExecution<T>, TransactionAttemptError>,
) -> bool {
    matches!(result, Err(TransactionAttemptError::CommitOutcomeUnknown))
}

fn classify_delivery_execution<T>(
    result: TransactionalExecution<T>,
    delivery_execution: bool,
) -> MongoDeliveryExecution<T> {
    if delivery_execution && matches!(&result, TransactionalExecution::InProgress) {
        MongoDeliveryExecution::ContentionDeferred
    } else {
        MongoDeliveryExecution::Completed(result)
    }
}

fn retain_primary_transaction_result<T, E>(
    ledger: &TransactionExecutionLedger,
    result: Result<TransactionalExecution<T>, E>,
    release: Result<(), MongoReliabilityError>,
) -> Result<TransactionalExecution<T>, E> {
    if let Err(error) = release {
        let code = error.code();
        ledger.record_failure(code);
        if result.is_ok() {
            ledger.record(&ledger.post_commit_release_failures_total);
        }
        lily_trace::tracing::warn!(
            lily.error_code = code,
            lily.storage.backend = "mongodb",
            lily.transaction.phase = "external_lease_release",
            lily.outcome = "advisory_failure",
            "transactional inbox external lease release was not confirmed"
        );
    }
    // The transaction result is the sole primary authority. External lease
    // deletion is advisory after commit and must never rewrite Applied or
    // AlreadyCompleted; cleanup also cannot replace a pre-commit failure.
    result
}

async fn await_external_lease(
    context: &TransactionExecutionContext,
    collection: &Collection<Document>,
    key: &str,
    owner: Uuid,
) -> Result<ExternalLeaseAdmission, QueueHandlerError> {
    let poll_interval = Duration::from_millis(
        (context.config.inbox_lock_timeout_millis / 10)
            .max(1)
            .min(u64::try_from(MAX_EXTERNAL_LEASE_POLL_INTERVAL.as_millis()).unwrap_or(u64::MAX)),
    );
    let mut contention_recorded = false;

    loop {
        if context.force.is_cancelled() || context.delivery_cancellation.is_cancelled() {
            return Err(reliability_as_handler(
                MongoReliabilityError::TransactionCancelled,
            ));
        }
        if context
            .body_deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Ok(ExternalLeaseAdmission::ContentionDeferred);
        }

        let operation_cancellation = CancellationToken::new();
        let acquired = run_storage_until(
            context,
            &operation_cancellation,
            context.body_deadline,
            acquire_external_lease(
                &context.database,
                collection,
                key,
                owner,
                context.config.inbox_lock_timeout_millis,
                operation_cancellation.child_token(),
            ),
        )
        .await;
        let acquired = match acquired {
            Ok(acquired) => acquired,
            Err(MongoReliabilityError::TransactionDeadlineExceeded) => {
                return Ok(ExternalLeaseAdmission::ContentionDeferred);
            }
            Err(error) => return Err(recorded_handler_failure(context, error)),
        };
        if acquired {
            return Ok(ExternalLeaseAdmission::Acquired);
        }

        if !contention_recorded {
            contention_recorded = true;
            context
                .ledger
                .record(&context.ledger.lease_contentions_total);
            lily_trace::tracing::info!(
                lily.storage.backend = "mongodb",
                lily.transaction.phase = "external_lease_admission",
                lily.outcome = "contended",
                "transactional inbox is waiting for an external owner"
            );
        }

        let operation_cancellation = CancellationToken::new();
        let completed = run_storage_until(
            context,
            &operation_cancellation,
            context.body_deadline,
            inbox_completed(
                &context.database,
                &context.logical_handler,
                context.event_id,
                operation_cancellation.child_token(),
            ),
        )
        .await;
        match completed {
            Ok(true) => return Ok(ExternalLeaseAdmission::AlreadyCompleted),
            Ok(false) if context.body_deadline.is_none() => {
                return Ok(ExternalLeaseAdmission::InProgress);
            }
            Ok(false) => {}
            Err(MongoReliabilityError::TransactionDeadlineExceeded) => {
                return Ok(ExternalLeaseAdmission::ContentionDeferred);
            }
            Err(error) => return Err(recorded_handler_failure(context, error)),
        }

        let sleep = tokio::time::sleep(poll_interval);
        tokio::pin!(sleep);
        let wait = async {
            tokio::select! {
                biased;
                _ = context.force.cancelled() => Err(MongoReliabilityError::TransactionCancelled),
                _ = context.delivery_cancellation.cancelled() => {
                    Err(MongoReliabilityError::TransactionCancelled)
                }
                _ = &mut sleep => Ok(()),
            }
        };
        let waited = if let Some(deadline) = context.body_deadline {
            tokio::time::timeout_at(deadline, wait)
                .await
                .map_err(|_| MongoReliabilityError::TransactionDeadlineExceeded)?
        } else {
            wait.await
        };
        match waited {
            Ok(()) => {}
            Err(MongoReliabilityError::TransactionDeadlineExceeded) => {
                return Ok(ExternalLeaseAdmission::ContentionDeferred);
            }
            Err(error) => return Err(recorded_handler_failure(context, error)),
        }
    }
}

async fn inbox_completed(
    database: &DatabaseService,
    logical_handler: &str,
    event_id: Uuid,
    cancellation: CancellationToken,
) -> Result<bool, MongoReliabilityError> {
    let collection = database.collection::<Document>(INBOX_COLLECTION)?;
    bounded_database_operation(database, cancellation, async {
        collection
            .find_one(doc! { "_id": inbox_key(logical_handler, event_id) })
            .projection(doc! { "state": 1 })
            .await
            .map(|document| {
                document.is_some_and(|document| document.get_str("state") == Ok(INBOX_COMPLETED))
            })
            .map_err(MongoDbError::from)
    })
    .await
}

async fn run_storage_before<T, Operation>(
    context: &TransactionExecutionContext,
    operation_cancellation: &CancellationToken,
    operation: Operation,
) -> Result<T, MongoReliabilityError>
where
    T: Send,
    Operation: Future<Output = Result<T, MongoReliabilityError>> + Send,
{
    run_storage_until(
        context,
        operation_cancellation,
        context.work_deadline,
        operation,
    )
    .await
}

async fn run_storage_until<T, Operation>(
    context: &TransactionExecutionContext,
    operation_cancellation: &CancellationToken,
    deadline: Option<Instant>,
    operation: Operation,
) -> Result<T, MongoReliabilityError>
where
    T: Send,
    Operation: Future<Output = Result<T, MongoReliabilityError>> + Send,
{
    let execution = async {
        tokio::select! {
            biased;
            _ = context.force.cancelled() => {
                operation_cancellation.cancel();
                Err(MongoReliabilityError::TransactionCancelled)
            }
            _ = context.delivery_cancellation.cancelled() => {
                operation_cancellation.cancel();
                Err(MongoReliabilityError::TransactionCancelled)
            }
            result = operation => result,
        }
    };
    let result = if let Some(deadline) = deadline {
        tokio::time::timeout_at(deadline, execution)
            .await
            .map_err(|_| {
                operation_cancellation.cancel();
                MongoReliabilityError::TransactionDeadlineExceeded
            })?
    } else {
        execution.await
    };
    if let Err(error) = &result {
        context.ledger.record_failure(error.code());
    }
    result
}

fn transaction_operation_context<'a>(
    context: &'a TransactionExecutionContext,
    cancellation: &CancellationToken,
) -> Result<MongoOperationContext<'a>, MongoReliabilityError> {
    let operation = context
        .database
        .operation_context(cancellation.child_token())?;
    match context.work_deadline {
        Some(deadline) => {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(MongoReliabilityError::TransactionDeadlineExceeded);
            }
            operation.with_deadline(remaining).map_err(Into::into)
        }
        None => Ok(operation),
    }
}

fn transaction_commit_timeout(
    context: &TransactionExecutionContext,
) -> Result<Duration, MongoReliabilityError> {
    let configured = Duration::from_millis(context.policy.commit_retry_timeout_millis);
    match context.work_deadline {
        Some(deadline) => {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                Err(MongoReliabilityError::TransactionDeadlineExceeded)
            } else {
                Ok(configured.min(remaining))
            }
        }
        None => Ok(configured),
    }
}

fn recorded_handler_failure(
    context: &TransactionExecutionContext,
    error: MongoReliabilityError,
) -> QueueHandlerError {
    context.ledger.record_failure(error.code());
    lily_trace::tracing::warn!(
        lily.error_code = error.code(),
        lily.storage.backend = "mongodb",
        lily.transaction.phase = "execution",
        lily.outcome = "failed",
        "transactional inbox storage execution failed"
    );
    reliability_as_handler(error)
}

fn transaction_retry_exhausted(context: &TransactionExecutionContext) -> QueueHandlerError {
    context
        .ledger
        .record(&context.ledger.transaction_retry_exhausted_total);
    recorded_handler_failure(context, MongoReliabilityError::TransactionRetryExhausted)
}

fn trace_transaction_retry(phase: &'static str, code: &'static str, attempt: u32) {
    lily_trace::tracing::warn!(
        lily.error_code = code,
        lily.storage.backend = "mongodb",
        lily.transaction.phase = phase,
        lily.transaction.attempt = u64::from(attempt),
        lily.outcome = "retry",
        "transactional inbox storage operation requested a bounded retry"
    );
}

async fn execute_attempts<T, Operation, OperationFuture>(
    context: &TransactionExecutionContext,
    lease_lost: &AtomicBool,
    operation: Operation,
) -> Result<TransactionalExecution<T>, TransactionAttemptError>
where
    T: Send + 'static,
    Operation: Fn(MongoTransaction, Option<Instant>) -> OperationFuture + Send + Sync + 'static,
    OperationFuture: Future<Output = Result<T, QueueHandlerError>> + Send + 'static,
{
    for attempt in 1..=context.policy.max_transaction_attempts {
        if lease_lost.load(Ordering::Acquire) {
            return Err(
                recorded_handler_failure(context, MongoReliabilityError::InboxLeaseLost).into(),
            );
        }
        if context.force.is_cancelled() || context.delivery_cancellation.is_cancelled() {
            return Err(recorded_handler_failure(
                context,
                MongoReliabilityError::TransactionCancelled,
            )
            .into());
        }
        let attempt_cancellation = context.delivery_cancellation.child_token();
        let base_operation = transaction_operation_context(context, &attempt_cancellation)
            .map_err(|error| recorded_handler_failure(context, error))?;
        let commit_timeout = transaction_commit_timeout(context)
            .map_err(|error| recorded_handler_failure(context, error))?;
        let begin = context
            .database
            .begin_transaction_with_commit_timeout(&base_operation, commit_timeout);
        let base_transaction = match run_storage_before(context, &attempt_cancellation, async {
            begin.await.map_err(MongoReliabilityError::from)
        })
        .await
        {
            Ok(transaction) => Arc::new(transaction),
            Err(error) => {
                if !error.is_transient_transaction() {
                    return Err(recorded_handler_failure(context, error).into());
                }
                if attempt == context.policy.max_transaction_attempts {
                    return Err(transaction_retry_exhausted(context).into());
                }
                trace_transaction_retry("begin", error.code(), attempt);
                wait_for_transaction_retry(context, attempt).await?;
                continue;
            }
        };
        let body_cancellation = attempt_cancellation.child_token();
        let transaction = MongoTransaction {
            database: Arc::clone(&context.database),
            transaction: Arc::clone(&base_transaction),
            cancellation: body_cancellation,
            source_handler: Arc::clone(&context.logical_handler),
            source_event_id: context.event_id,
            max_outbox_bytes: context
                .config
                .relay_max_in_flight_bytes
                .min(MAX_OUTBOX_MESSAGE_BYTES),
        };

        context.ledger.record(&context.ledger.body_attempts_total);
        lily_trace::tracing::debug!(
            lily.storage.backend = "mongodb",
            lily.transaction.phase = "body",
            lily.transaction.attempt = u64::from(attempt),
            lily.outcome = "attempt",
            "transactional inbox body attempt started"
        );
        let body = execute_transaction_body(
            TransactionBodyContext {
                database: &context.database,
                base_transaction: &base_transaction,
                config: &context.config,
                logical_handler: &context.logical_handler,
                event_id: context.event_id,
                transaction,
                framework_cancellation: attempt_cancellation.child_token(),
                body_deadline: context.body_deadline,
            },
            &operation,
        );
        // The queue-owned body includes middleware unwind and DI scope
        // disposal. Never race it with an outer storage timeout/cancellation
        // select: dropping this future could allow transaction finalization or
        // broker settlement to overtake scope cleanup. Cooperative shutdown is
        // observed through the attempt-local DeliveryInput token; force drain
        // uses the tracked owner abort/join path.
        let attempt_result = body.await;
        let observation = base_transaction.error_observation();

        if lease_lost.load(Ordering::Acquire) {
            abort_best_effort(context, &base_transaction).await;
            return Err(
                recorded_handler_failure(context, MongoReliabilityError::InboxLeaseLost).into(),
            );
        }
        if context.force.is_cancelled() || context.delivery_cancellation.is_cancelled() {
            abort_best_effort(context, &base_transaction).await;
            return Err(recorded_handler_failure(
                context,
                MongoReliabilityError::TransactionCancelled,
            )
            .into());
        }

        let transient_body_retry = match attempt_result {
            Ok(value) if !observation.transient_transaction() => {
                match commit_with_retry(context, &base_transaction, &attempt_cancellation).await {
                    Ok(()) => return Ok(value),
                    Err(error) if error.is_transient_transaction() => true,
                    Err(MongoReliabilityError::CommitOutcomeUnknown) => {
                        return Err(TransactionAttemptError::CommitOutcomeUnknown);
                    }
                    Err(error) => return Err(recorded_handler_failure(context, error).into()),
                }
            }
            Ok(_) => {
                abort_best_effort(context, &base_transaction).await;
                true
            }
            Err(AttemptFailure::Handler(error)) if !observation.transient_transaction() => {
                abort_best_effort(context, &base_transaction).await;
                return Err(error.into());
            }
            Err(AttemptFailure::Storage(error))
                if !error.is_transient_transaction() && !observation.transient_transaction() =>
            {
                abort_best_effort(context, &base_transaction).await;
                return Err(recorded_handler_failure(context, error).into());
            }
            Err(_) => {
                abort_best_effort(context, &base_transaction).await;
                true
            }
        };

        if attempt == context.policy.max_transaction_attempts {
            return Err(transaction_retry_exhausted(context).into());
        }
        if transient_body_retry {
            context
                .ledger
                .record(&context.ledger.transient_body_retries_total);
            trace_transaction_retry(
                "body",
                MongoReliabilityError::TransientTransaction.code(),
                attempt,
            );
        }
        wait_for_transaction_retry(context, attempt).await?;
    }
    Err(transaction_retry_exhausted(context).into())
}

async fn wait_for_transaction_retry(
    context: &TransactionExecutionContext,
    attempt: u32,
) -> Result<(), QueueHandlerError> {
    let wait = async {
        tokio::select! {
            biased;
            _ = context.force.cancelled() => {
                Err(MongoReliabilityError::TransactionCancelled)
            }
            _ = context.delivery_cancellation.cancelled() => {
                Err(MongoReliabilityError::TransactionCancelled)
            }
            _ = tokio::time::sleep(transaction_backoff(&context.policy, attempt)) => Ok(()),
        }
    };
    let result = if let Some(deadline) = context.work_deadline {
        tokio::time::timeout_at(deadline, wait)
            .await
            .map_err(|_| MongoReliabilityError::TransactionDeadlineExceeded)?
    } else {
        wait.await
    };
    match result {
        Ok(()) => Ok(()),
        Err(error) => Err(recorded_handler_failure(context, error)),
    }
}

enum AttemptFailure {
    Handler(QueueHandlerError),
    Storage(MongoReliabilityError),
}

enum TransactionAttemptError {
    Handler(QueueHandlerError),
    CommitOutcomeUnknown,
}

impl From<QueueHandlerError> for TransactionAttemptError {
    fn from(error: QueueHandlerError) -> Self {
        Self::Handler(error)
    }
}

struct TransactionBodyContext<'a> {
    database: &'a DatabaseService,
    base_transaction: &'a lily_mongo_repository::MongoTransaction,
    config: &'a TransactionalInboxConfig,
    logical_handler: &'a str,
    event_id: Uuid,
    transaction: MongoTransaction,
    framework_cancellation: CancellationToken,
    body_deadline: Option<Instant>,
}

struct BodyLifetimeGuard(CancellationToken);

impl BodyLifetimeGuard {
    fn new(cancellation: CancellationToken) -> Self {
        Self(cancellation)
    }

    fn cancel(&self) {
        self.0.cancel();
    }
}

impl Drop for BodyLifetimeGuard {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

async fn execute_transaction_body<T, Operation, OperationFuture>(
    context: TransactionBodyContext<'_>,
    operation: &Operation,
) -> Result<TransactionalExecution<T>, AttemptFailure>
where
    Operation: Fn(MongoTransaction, Option<Instant>) -> OperationFuture + Send + Sync,
    OperationFuture: Future<Output = Result<T, QueueHandlerError>> + Send,
{
    // The public MongoTransaction handle has a strictly body-scoped
    // cancellation lifetime. A clone escaping into a detached task therefore
    // cannot mutate business data or enqueue outbox rows after the handler
    // body has returned. Framework inbox completion remains on the independent
    // attempt token so invalidating public handles does not cancel commit work.
    let body_lifetime = BodyLifetimeGuard::new(context.transaction.cancellation.clone());
    let operation_context = context
        .database
        .operation_context(context.framework_cancellation.child_token())
        .map(|operation| operation.with_transaction(context.base_transaction))
        .map_err(MongoReliabilityError::from)
        .map_err(AttemptFailure::Storage)?;
    match claim_inbox(
        context.database,
        context.base_transaction,
        &operation_context,
        context.logical_handler,
        context.event_id,
        context.config.inbox_lock_timeout_millis,
    )
    .await
    .map_err(AttemptFailure::Storage)?
    {
        InboxClaimState::AlreadyCompleted => Ok(TransactionalExecution::AlreadyCompleted),
        InboxClaimState::InProgress => Ok(TransactionalExecution::InProgress),
        InboxClaimState::Acquired(lock_token) => {
            let value = operation(context.transaction.clone(), context.body_deadline).await;
            body_lifetime.cancel();
            let value = value.map_err(AttemptFailure::Handler)?;
            complete_inbox(
                context.database,
                context.base_transaction,
                &operation_context,
                context.logical_handler,
                context.event_id,
                &lock_token,
            )
            .await
            .map_err(AttemptFailure::Storage)?;
            Ok(TransactionalExecution::Applied(value))
        }
    }
}

async fn commit_with_retry(
    context: &TransactionExecutionContext,
    transaction: &lily_mongo_repository::MongoTransaction,
    cancellation: &CancellationToken,
) -> Result<(), MongoReliabilityError> {
    let configured_deadline =
        Instant::now() + Duration::from_millis(context.policy.commit_retry_timeout_millis);
    let deadline = context
        .work_deadline
        .map_or(configured_deadline, |delivery| {
            delivery.min(configured_deadline)
        });
    let mut commit_attempt = 0_u32;
    loop {
        if Instant::now() >= deadline {
            return Err(commit_outcome_unknown(context));
        }
        transaction.clear_error_observation();
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(commit_outcome_unknown(context));
        }
        let operation = context
            .database
            .operation_context(cancellation.child_token())?
            .with_deadline(remaining)?
            .with_transaction_observation(transaction.observation_token());
        context.ledger.record(&context.ledger.commit_attempts_total);
        commit_attempt = commit_attempt.saturating_add(1);
        let attempt = transaction
            .commit(&operation)
            .map(|result| result.map_err(map_commit_operation_error));
        match run_storage_until(context, cancellation, Some(deadline), attempt).await {
            Ok(()) => return Ok(()),
            Err(error) => {
                if error.is_unknown_commit_result()
                    || transaction.error_observation().unknown_commit_result()
                {
                    context
                        .ledger
                        .record(&context.ledger.unknown_commit_retries_total);
                    trace_transaction_retry("commit", error.code(), commit_attempt);
                    tokio::task::yield_now().await;
                    continue;
                }
                if matches!(
                    error,
                    MongoReliabilityError::CommitOutcomeUnknown
                        | MongoReliabilityError::TransactionDeadlineExceeded
                        | MongoReliabilityError::TransactionCancelled
                ) {
                    return Err(commit_outcome_unknown(context));
                }
                return Err(error);
            }
        }
    }
}

fn map_commit_operation_error(error: MongoDbError) -> MongoReliabilityError {
    match error {
        MongoDbError::OperationTimedOut | MongoDbError::OperationCancelled => {
            MongoReliabilityError::CommitOutcomeUnknown
        }
        error => error.into(),
    }
}

fn commit_outcome_unknown(context: &TransactionExecutionContext) -> MongoReliabilityError {
    let error = MongoReliabilityError::CommitOutcomeUnknown;
    context
        .ledger
        .record(&context.ledger.commit_outcome_unknown_total);
    context.ledger.record_failure(error.code());
    lily_trace::tracing::warn!(
        lily.error_code = error.code(),
        lily.storage.backend = "mongodb",
        lily.transaction.phase = "commit",
        lily.outcome = "unknown",
        "transactional inbox commit outcome remained unknown at its deadline"
    );
    error
}

async fn abort_best_effort(
    context: &TransactionExecutionContext,
    transaction: &lily_mongo_repository::MongoTransaction,
) {
    let cancellation = CancellationToken::new();
    let Ok(mut operation) = context
        .database
        .operation_context(cancellation.child_token())
    else {
        return;
    };
    if let Some(deadline) = context.cleanup_deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return;
        }
        let Ok(bounded) = operation.with_deadline(remaining) else {
            return;
        };
        operation = bounded;
        let abort = transaction.abort(&operation);
        if tokio::time::timeout_at(deadline, abort).await.is_err() {
            cancellation.cancel();
        }
    } else {
        let _ = transaction.abort(&operation).await;
    }
}

async fn claim_inbox(
    database: &DatabaseService,
    transaction: &lily_mongo_repository::MongoTransaction,
    operation: &MongoOperationContext<'_>,
    logical_handler: &str,
    event_id: Uuid,
    lock_timeout_millis: u64,
) -> Result<InboxClaimState, MongoReliabilityError> {
    let collection = database.collection::<Document>(INBOX_COLLECTION)?;
    let key = inbox_key(logical_handler, event_id);
    let lock_timeout_millis = i64::try_from(lock_timeout_millis)
        .map_err(|_| invalid("QUEUE_MONGODB_DURATION_INVALID"))?;
    let lock_token: Arc<str> = Arc::from(Uuid::new_v4().to_string());

    let existing = operation
        .execute(async {
            let mut session = transaction.lock_session().await;
            collection
                .find_one(doc! { "_id": &key })
                .session(&mut *session)
                .await
                .map_err(|error| operation.map_driver_error(error))
        })
        .await?;
    match existing {
        Some(document) if document.get_str("state") == Ok(INBOX_COMPLETED) => {
            Ok(InboxClaimState::AlreadyCompleted)
        }
        Some(_) => {
            let result = operation
                .execute(async {
                    let mut session = transaction.lock_session().await;
                    collection
                        .update_one(
                            doc! {
                                "_id": &key,
                                "state": INBOX_PROCESSING,
                                "$expr": { "$lte": ["$locked_until", "$$NOW"] },
                            },
                            vec![doc! { "$set": {
                                "lock_token": { "$literal": lock_token.as_ref() },
                                "locked_until": { "$add": ["$$NOW", lock_timeout_millis] },
                                "updated_at": "$$NOW",
                            }}],
                        )
                        .session(&mut *session)
                        .await
                        .map_err(|error| operation.map_driver_error(error))
                })
                .await?;
            if result.modified_count == 1 {
                Ok(InboxClaimState::Acquired(lock_token))
            } else {
                Ok(InboxClaimState::InProgress)
            }
        }
        None => {
            let document = doc! {
                "_id": &key,
                "handler_identity": logical_handler,
                "event_id": event_id.to_string(),
                "state": INBOX_PROCESSING,
                "lock_token": lock_token.as_ref(),
                "locked_until": DateTime::from_millis(0),
                "created_at": DateTime::from_millis(0),
                "updated_at": DateTime::from_millis(0),
                "completed_at": Bson::Null,
            };
            let inserted = operation
                .execute(async {
                    let mut session = transaction.lock_session().await;
                    collection
                        .insert_one(document)
                        .session(&mut *session)
                        .await
                        .map(|_| ())
                        .map_err(|error| operation.map_driver_error(error))
                })
                .await;
            match inserted {
                Ok(()) => {
                    let result = operation
                        .execute(async {
                            let mut session = transaction.lock_session().await;
                            collection
                                .update_one(
                                    doc! { "_id": &key, "lock_token": lock_token.as_ref() },
                                    vec![doc! { "$set": {
                                        "locked_until": {
                                            "$add": ["$$NOW", lock_timeout_millis]
                                        },
                                        "created_at": "$$NOW",
                                        "updated_at": "$$NOW",
                                    }}],
                                )
                                .session(&mut *session)
                                .await
                                .map_err(|error| operation.map_driver_error(error))
                        })
                        .await?;
                    if result.modified_count != 1 {
                        return Err(MongoReliabilityError::InboxLeaseLost);
                    }
                    Ok(InboxClaimState::Acquired(lock_token))
                }
                Err(MongoDbError::DuplicateKey(_)) => Ok(InboxClaimState::InProgress),
                Err(error) => Err(error.into()),
            }
        }
    }
}

async fn complete_inbox(
    database: &DatabaseService,
    transaction: &lily_mongo_repository::MongoTransaction,
    operation: &MongoOperationContext<'_>,
    logical_handler: &str,
    event_id: Uuid,
    lock_token: &str,
) -> Result<(), MongoReliabilityError> {
    let collection = database.collection::<Document>(INBOX_COLLECTION)?;
    let result = operation
        .execute(async {
            let mut session = transaction.lock_session().await;
            collection
                .update_one(
                    doc! {
                        "_id": inbox_key(logical_handler, event_id),
                        "state": INBOX_PROCESSING,
                        "lock_token": lock_token,
                    },
                    doc! {
                        "$set": {
                            "state": INBOX_COMPLETED,
                        },
                        "$currentDate": { "completed_at": true, "updated_at": true },
                        "$unset": { "lock_token": "", "locked_until": "" },
                    },
                )
                .session(&mut *session)
                .await
                .map_err(|error| operation.map_driver_error(error))
        })
        .await?;
    if result.modified_count == 1 {
        Ok(())
    } else {
        Err(MongoReliabilityError::InboxLeaseLost)
    }
}

async fn acquire_external_lease(
    database: &DatabaseService,
    collection: &Collection<Document>,
    key: &str,
    owner: Uuid,
    lease_millis: u64,
    cancellation: CancellationToken,
) -> Result<bool, MongoReliabilityError> {
    let lease_millis =
        i64::try_from(lease_millis).map_err(|_| invalid("QUEUE_MONGODB_DURATION_INVALID"))?;
    let owner = owner.to_string();
    let can_acquire = doc! { "$or": [
        { "$eq": [{ "$type": "$expires_at" }, "missing"] },
        { "$lte": ["$expires_at", "$$NOW"] },
        { "$eq": ["$owner", { "$literal": &owner }] },
    ] };
    let operation = database.operation_context(cancellation)?;
    let result = operation
        .execute(async {
            collection
                .find_one_and_update(
                    doc! { "_id": key },
                    vec![doc! { "$set": {
                        "owner": { "$cond": [
                            can_acquire.clone(),
                            { "$literal": &owner },
                            "$owner",
                        ] },
                        "expires_at": { "$cond": [
                            can_acquire.clone(),
                            { "$add": ["$$NOW", lease_millis] },
                            "$expires_at",
                        ] },
                        "updated_at": { "$cond": [
                            can_acquire,
                            "$$NOW",
                            "$updated_at",
                        ] },
                    }}],
                )
                .upsert(true)
                .return_document(ReturnDocument::After)
                .await
                .map_err(MongoDbError::from)
        })
        .await;
    match result {
        Ok(Some(document)) if document.get_str("owner") == Ok(owner.as_str()) => Ok(true),
        Ok(Some(_)) => Ok(false),
        Ok(None) => Ok(false),
        Err(error) => match error {
            MongoDbError::DuplicateKey(_) => Ok(false),
            error => Err(error.into()),
        },
    }
}

async fn heartbeat_external_lease(context: ExternalLeaseHeartbeatContext) {
    let interval = Duration::from_millis((context.lease_millis / 3).max(1));
    let lease_millis = match i64::try_from(context.lease_millis) {
        Ok(value) => value,
        Err(_) => {
            context.lost.store(true, Ordering::Release);
            context.force.cancel();
            return;
        }
    };
    let owner = context.owner.to_string();
    loop {
        let wait = async {
            tokio::select! {
                biased;
                _ = context.stop.cancelled() => false,
                _ = context.force.cancelled() => false,
                _ = tokio::time::sleep(interval) => true,
            }
        };
        let continue_heartbeat = if let Some(deadline) = context.deadline {
            tokio::time::timeout_at(deadline, wait)
                .await
                .unwrap_or(false)
        } else {
            wait.await
        };
        if !continue_heartbeat {
            return;
        }
        let operation_cancellation = CancellationToken::new();
        let renew = bounded_database_operation(
            &context.database,
            operation_cancellation.child_token(),
            async {
                context
                    .collection
                    .update_one(
                        doc! {
                            "_id": &context.key,
                            "owner": &owner,
                            "$expr": { "$gt": ["$expires_at", "$$NOW"] },
                        },
                        vec![doc! { "$set": {
                            "expires_at": { "$add": ["$$NOW", lease_millis] },
                            "updated_at": "$$NOW",
                        }}],
                    )
                    .await
                    .map_err(MongoDbError::from)
            },
        );
        tokio::pin!(renew);
        let operation = async {
            tokio::select! {
                biased;
                _ = context.stop.cancelled() => {
                    operation_cancellation.cancel();
                    None
                }
                _ = context.force.cancelled() => {
                    operation_cancellation.cancel();
                    None
                }
                result = &mut renew => Some(result),
            }
        };
        let result = if let Some(deadline) = context.deadline {
            match tokio::time::timeout_at(deadline, operation).await {
                Ok(Some(result)) => result,
                Ok(None) => return,
                Err(_) => {
                    operation_cancellation.cancel();
                    return;
                }
            }
        } else {
            let Some(result) = operation.await else {
                return;
            };
            result
        };
        match result {
            Ok(result) if result.modified_count == 1 || result.matched_count == 1 => {}
            _ => {
                context.lost.store(true, Ordering::Release);
                context.force.cancel();
                return;
            }
        }
    }
}

async fn release_external_lease_before(
    context: &TransactionExecutionContext,
    collection: &Collection<Document>,
    key: &str,
    owner: Uuid,
) -> Result<(), MongoReliabilityError> {
    let cancellation = CancellationToken::new();
    let release = release_external_lease(
        &context.database,
        collection,
        key,
        owner,
        cancellation.child_token(),
    );
    if let Some(deadline) = context.cleanup_deadline {
        match tokio::time::timeout_at(deadline, release).await {
            Ok(result) => result,
            Err(_) => {
                cancellation.cancel();
                Err(MongoReliabilityError::TransactionDeadlineExceeded)
            }
        }
    } else {
        release.await
    }
}

async fn release_external_lease(
    database: &DatabaseService,
    collection: &Collection<Document>,
    key: &str,
    owner: Uuid,
    cancellation: CancellationToken,
) -> Result<(), MongoReliabilityError> {
    let result = bounded_database_operation(database, cancellation, async {
        collection
            .delete_one(doc! { "_id": key, "owner": owner.to_string() })
            .await
            .map_err(MongoDbError::from)
    })
    .await?;
    if result.deleted_count == 1 {
        Ok(())
    } else {
        Err(MongoReliabilityError::InboxLeaseLost)
    }
}

async fn release_outbox_claim(
    database: &DatabaseService,
    collection: &Collection<Document>,
    record_id: Uuid,
    claim_token: Uuid,
) -> Result<(), MongoReliabilityError> {
    let result = bounded_database_operation(database, CancellationToken::new(), async {
        collection
            .update_one(
                doc! { "_id": record_id.to_string(), "claim_token": claim_token.to_string() },
                doc! { "$set": {
                    "claim_owner": Bson::Null,
                    "claim_token": Bson::Null,
                    "claimed_until": Bson::Null,
                } },
            )
            .await
            .map_err(MongoDbError::from)
    })
    .await?;
    exactly_one(result.modified_count)
}

async fn ensure_schema_ready(database: &DatabaseService) -> Result<(), MongoReliabilityError> {
    let names = bounded_database_operation(
        database,
        CancellationToken::new(),
        database.list_collection_names(Some(required_collection_filter())),
    )
    .await?;
    if !required_collection_names_are_complete(&names) {
        return Err(MongoReliabilityError::SchemaMissing);
    }

    let schema = database.collection::<Document>(SCHEMA_COLLECTION)?;
    let marker = bounded_database_operation(database, CancellationToken::new(), async {
        schema
            .find_one(doc! { "_id": MIGRATION_COMPONENT })
            .await
            .map_err(MongoDbError::from)
    })
    .await?
    .ok_or(MongoReliabilityError::SchemaMissing)?;
    let installed = marker
        .get_i64("version")
        .or_else(|_| marker.get_i32("version").map(i64::from))
        .map_err(|_| MongoReliabilityError::SchemaDrift)?;
    if installed < SCHEMA_VERSION {
        return Err(MongoReliabilityError::SchemaOutdated {
            installed,
            required: SCHEMA_VERSION,
        });
    }
    if installed > SCHEMA_VERSION {
        return Err(MongoReliabilityError::SchemaTooNew {
            installed,
            supported: SCHEMA_VERSION,
        });
    }
    if marker.get_str("fingerprint") != Ok(SCHEMA_FINGERPRINT) {
        return Err(MongoReliabilityError::SchemaDrift);
    }

    let specifications = bounded_database_operation(
        database,
        CancellationToken::new(),
        database.list_collection_specifications(Some(doc! {
            "name": { "$in": [INBOX_COLLECTION, LEASE_COLLECTION, OUTBOX_COLLECTION] }
        })),
    )
    .await?;
    for (name, validator) in [
        (INBOX_COLLECTION, inbox_validator()),
        (LEASE_COLLECTION, lease_validator()),
        (OUTBOX_COLLECTION, outbox_validator()),
    ] {
        let Some(specification) = specifications.iter().find(|item| item.name == name) else {
            return Err(MongoReliabilityError::SchemaDrift);
        };
        if !collection_specification_matches(specification, &validator) {
            return Err(MongoReliabilityError::SchemaDrift);
        }
    }

    verify_indexes(
        database,
        &database.collection::<Document>(INBOX_COLLECTION)?,
        &[
            (
                INBOX_IDENTITY_INDEX,
                doc! { "handler_identity": 1, "event_id": 1 },
                true,
            ),
            (INBOX_COMPLETED_INDEX, doc! { "completed_at": 1 }, false),
        ],
    )
    .await?;
    verify_indexes(
        database,
        &database.collection::<Document>(LEASE_COLLECTION)?,
        &[(LEASE_EXPIRY_INDEX, doc! { "expires_at": 1 }, false)],
    )
    .await?;
    verify_indexes(
        database,
        &database.collection::<Document>(OUTBOX_COLLECTION)?,
        &[
            (OUTBOX_EVENT_INDEX, doc! { "event_id": 1 }, true),
            (
                OUTBOX_READY_INDEX,
                doc! {
                    "delivered_at": 1, "available_at": 1, "claimed_until": 1,
                    "created_at": 1, "_id": 1,
                },
                false,
            ),
            (OUTBOX_DELIVERED_INDEX, doc! { "delivered_at": 1 }, false),
        ],
    )
    .await
}

fn required_collection_filter() -> Document {
    doc! { "name": { "$in": REQUIRED_COLLECTIONS.to_vec() } }
}

fn required_collection_names_are_complete(names: &[String]) -> bool {
    names.len() == REQUIRED_COLLECTIONS.len()
        && REQUIRED_COLLECTIONS
            .iter()
            .all(|required| names.iter().any(|name| name == required))
}

fn collection_specification_matches(
    specification: &CollectionSpecification,
    validator: &Document,
) -> bool {
    let options = &specification.options;
    specification.collection_type == CollectionType::Collection
        && !specification.info.read_only
        && !options.capped.unwrap_or(false)
        && options.size.is_none()
        && options.max.is_none()
        && options.storage_engine.is_none()
        && options.validator.as_ref() == Some(validator)
        && options.validation_level == Some(ValidationLevel::Strict)
        && options.validation_action == Some(ValidationAction::Error)
        && options.view_on.is_none()
        && options.pipeline.is_none()
        && options.collation.is_none()
        && options.write_concern.is_none()
        && options.index_option_defaults.is_none()
        && options.timeseries.is_none()
        && options.expire_after_seconds.is_none()
        && options.change_stream_pre_and_post_images.is_none()
        && options.clustered_index.is_none()
        && options.comment.is_none()
}

async fn verify_indexes(
    database: &DatabaseService,
    collection: &Collection<Document>,
    expected: &[(&str, Document, bool)],
) -> Result<(), MongoReliabilityError> {
    let indexes = bounded_database_operation(database, CancellationToken::new(), async {
        collection
            .list_indexes()
            .await
            .map_err(MongoDbError::from)?
            .try_collect::<Vec<_>>()
            .await
            .map_err(MongoDbError::from)
    })
    .await?;
    if !indexes_match_expected(&indexes, expected) {
        return Err(MongoReliabilityError::SchemaDrift);
    }
    Ok(())
}

fn indexes_match_expected(
    indexes: &[mongodb::IndexModel],
    expected: &[(&str, Document, bool)],
) -> bool {
    if indexes.len() != expected.len().saturating_add(1) {
        return false;
    }
    let mut id_indexes = 0_usize;
    for index in indexes {
        let Some(name) = index
            .options
            .as_ref()
            .and_then(|options| options.name.as_deref())
        else {
            return false;
        };
        if name == "_id_" {
            id_indexes = id_indexes.saturating_add(1);
            if index.keys != doc! { "_id": 1 } {
                return false;
            }
            continue;
        }
        if !expected
            .iter()
            .any(|(expected_name, _, _)| name == *expected_name)
        {
            return false;
        }
    }
    if id_indexes != 1 {
        return false;
    }
    for (name, keys, unique) in expected {
        let Some(index) = indexes.iter().find(|index| {
            index
                .options
                .as_ref()
                .and_then(|options| options.name.as_deref())
                == Some(*name)
        }) else {
            return false;
        };
        let actual_unique = index
            .options
            .as_ref()
            .and_then(|options| options.unique)
            .unwrap_or(false);
        let incompatible_options = index.options.as_ref().is_some_and(|options| {
            options.sparse.unwrap_or(false)
                || options.partial_filter_expression.is_some()
                || options.expire_after.is_some()
                || options.hidden.unwrap_or(false)
                || options.collation.is_some()
        });
        if &index.keys != keys || actual_unique != *unique || incompatible_options {
            return false;
        }
    }
    true
}

async fn bounded_ids(
    database: &DatabaseService,
    collection: &Collection<Document>,
    filter: Document,
    limit: i64,
) -> Result<Vec<String>, MongoReliabilityError> {
    let rows = bounded_database_operation(database, CancellationToken::new(), async {
        collection
            .find(filter)
            .sort(doc! { "_id": 1 })
            .limit(limit)
            .projection(doc! { "_id": 1 })
            .await
            .map_err(MongoDbError::from)?
            .try_collect::<Vec<_>>()
            .await
            .map_err(MongoDbError::from)
    })
    .await?;
    rows.into_iter()
        .map(|row| {
            row.get_str("_id")
                .map(str::to_owned)
                .map_err(|_| MongoReliabilityError::SchemaDrift)
        })
        .collect()
}

async fn delete_ids(
    database: &DatabaseService,
    collection: &Collection<Document>,
    ids: Vec<String>,
) -> Result<u64, MongoReliabilityError> {
    if ids.is_empty() {
        return Ok(0);
    }
    bounded_database_operation(database, CancellationToken::new(), async {
        collection
            .delete_many(doc! { "_id": { "$in": ids } })
            .await
            .map(|result| result.deleted_count)
            .map_err(MongoDbError::from)
    })
    .await
}

async fn delete_expired_leases(
    database: &DatabaseService,
    collection: &Collection<Document>,
    ids: Vec<String>,
) -> Result<u64, MongoReliabilityError> {
    if ids.is_empty() {
        return Ok(0);
    }
    bounded_database_operation(database, CancellationToken::new(), async {
        collection
            .delete_many(doc! {
                "_id": { "$in": ids },
                "$expr": { "$lt": ["$expires_at", "$$NOW"] },
            })
            .await
            .map(|result| result.deleted_count)
            .map_err(MongoDbError::from)
    })
    .await
}

fn map_claimed_outbox(
    document: Document,
) -> Result<ClaimedMongoOutboxMessage, MongoReliabilityError> {
    let record_id = parse_uuid(&document, "_id")?;
    let event_id = parse_uuid(&document, "event_id")?;
    let claim_token = parse_uuid(&document, "claim_token")?;
    let schema_version = document
        .get_i32("schema_version")
        .ok()
        .and_then(|value| u16::try_from(value).ok())
        .filter(|value| *value > 0)
        .ok_or_else(|| invalid("QUEUE_OUTBOX_STORED_SCHEMA_VERSION_INVALID"))?;
    let body = document
        .get_binary_generic("body")
        .map(|bytes| Bytes::copy_from_slice(bytes.as_slice()))
        .map_err(|_| invalid("QUEUE_OUTBOX_STORED_BODY_INVALID"))?;
    if body.len() > MAX_OUTBOX_MESSAGE_BYTES {
        return Err(MongoReliabilityError::OutboxRecordTooLarge {
            bytes: body.len(),
            maximum: MAX_OUTBOX_MESSAGE_BYTES,
        });
    }
    let traceparent = match document.get("traceparent") {
        Some(Bson::String(value))
            if lily_trace::W3CTraceContext::from_traceparent(value).is_ok() =>
        {
            Some(Arc::from(value.as_str()))
        }
        Some(Bson::Null) | None => None,
        _ => return Err(invalid("QUEUE_OUTBOX_STORED_TRACEPARENT_INVALID")),
    };
    Ok(ClaimedMongoOutboxMessage {
        record_id,
        event_id,
        exchange: bounded_stored_string(
            &document,
            "exchange_name",
            MAX_OUTBOX_TRANSPORT_IDENTITY_BYTES,
        )?,
        routing_key: bounded_stored_string(
            &document,
            "routing_key",
            MAX_OUTBOX_TRANSPORT_IDENTITY_BYTES,
        )?,
        schema_version,
        content_kind: bounded_stored_string(
            &document,
            "content_kind",
            lily_queue_client::MAX_PUBLISH_CONTENT_KIND_BYTES,
        )?,
        content_type: bounded_stored_string(
            &document,
            "content_type",
            lily_queue_client::MAX_PUBLISH_CONTENT_TYPE_BYTES,
        )?,
        body,
        traceparent,
        claim_token,
        publish_attempts: nonnegative_u64(&document, "publish_attempts")?,
    })
}

fn map_outbox_write_error(error: MongoDbError) -> MongoReliabilityError {
    match error {
        MongoDbError::DuplicateKey(_) => invalid("QUEUE_OUTBOX_CONSTRAINT_VIOLATION"),
        error => error.into(),
    }
}

fn validate_policy(config: &TransactionalInboxConfig) -> Result<(), MongoReliabilityError> {
    config
        .validate_contract()
        .map_err(|_| invalid("QUEUE_MONGODB_TRANSACTION_CONFIG_INVALID"))?;
    if config.backend != TransactionalInboxBackend::MongoDb {
        return Err(invalid("QUEUE_TRANSACTION_BACKEND_MISMATCH"));
    }
    Ok(())
}

fn map_topology_error(error: MongoDbError) -> MongoReliabilityError {
    match error {
        MongoDbError::InvalidConfiguration(_) => {
            MongoReliabilityError::TransactionTopologyUnsupported
        }
        error => error.into(),
    }
}

fn reliability_as_handler(error: MongoReliabilityError) -> QueueHandlerError {
    let code = error.code();
    match error {
        MongoReliabilityError::InvalidContract { .. }
        | MongoReliabilityError::SchemaMissing
        | MongoReliabilityError::SchemaOutdated { .. }
        | MongoReliabilityError::SchemaTooNew { .. }
        | MongoReliabilityError::SchemaDrift
        | MongoReliabilityError::TransactionTopologyUnsupported => {
            QueueHandlerError::permanent_with_source(code, error)
        }
        _ => QueueHandlerError::retryable_with_source(code, error),
    }
}

async fn bounded_database_operation<T, Operation>(
    database: &DatabaseService,
    cancellation: CancellationToken,
    operation: Operation,
) -> Result<T, MongoReliabilityError>
where
    T: Send,
    Operation: Future<Output = Result<T, MongoDbError>> + Send,
{
    database
        .operation_context(cancellation)?
        .execute(operation)
        .await
        .map_err(Into::into)
}

async fn drain_tracker(
    tracker: &TransactionTracker,
    timeout: Duration,
) -> Result<(), MongoReliabilityError> {
    let deadline = tracker.owners.budget.cap(Instant::now() + timeout);
    let drain = async {
        loop {
            let changed = tracker.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if !tracker
                .owners
                .tasks
                .drain_before(&tracker.owners.budget, deadline)
                .await
            {
                return false;
            }
            if tracker.active.load(Ordering::Acquire) == 0 {
                return true;
            }
            changed.await;
        }
    };
    if !(tracker.active.load(Ordering::Acquire) == 0 && tracker.owners.tasks.reconciled())
        && tracker.owners.budget.run_until(deadline, drain).await != Ok(true)
    {
        return Err(MongoReliabilityError::TransactionDrainTimeout {
            remaining: tracker.active.load(Ordering::Acquire),
        });
    }
    if tracker.owners.tasks.panicked() || tracker.owners.interrupted.load(Ordering::Acquire) {
        return Err(MongoReliabilityError::TransactionTerminationIncomplete);
    }
    Ok(())
}

fn signal_force_drain(tracker: &TransactionTracker) {
    tracker.accepting.store(false, Ordering::Release);
    // Full pipeline cooperation and transaction finalization retain ownership
    // until this fixed cutoff. No immediate abort of the execution future.
    tracker.owners.request_force();
    tracker.changed.notify_waiters();
}

fn transaction_backoff(policy: &MongoTransactionalInboxConfig, attempt: u32) -> Duration {
    let shift = attempt.saturating_sub(1).min(63);
    let multiplier = 1_u64.checked_shl(shift).unwrap_or(u64::MAX);
    Duration::from_millis(
        policy
            .retry_initial_backoff_millis
            .saturating_mul(multiplier)
            .min(policy.retry_max_backoff_millis),
    )
}

fn inbox_key(handler: &str, event_id: Uuid) -> String {
    let mut digest = Sha256::new();
    digest.update(b"lily.queue.mongodb.inbox.v1\0");
    digest.update(handler.as_bytes());
    digest.update([0]);
    digest.update(event_id.as_bytes());
    format!("{:x}", digest.finalize())
}

fn duration_seconds_as_i64_millis(seconds: u64) -> Result<i64, MongoReliabilityError> {
    seconds
        .checked_mul(1_000)
        .and_then(|value| i64::try_from(value).ok())
        .ok_or_else(|| invalid("QUEUE_MONGODB_DURATION_INVALID"))
}

fn parse_uuid(document: &Document, field: &str) -> Result<Uuid, MongoReliabilityError> {
    document
        .get_str(field)
        .ok()
        .and_then(|value| Uuid::parse_str(value).ok())
        .filter(|value| !value.is_nil())
        .ok_or_else(|| invalid("QUEUE_OUTBOX_STORED_IDENTITY_INVALID"))
}

fn bounded_stored_string(
    document: &Document,
    field: &str,
    maximum: usize,
) -> Result<Arc<str>, MongoReliabilityError> {
    let value = document
        .get_str(field)
        .map_err(|_| invalid("QUEUE_OUTBOX_STORED_STRING_INVALID"))?;
    if value.is_empty()
        || value.len() > maximum
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        return Err(invalid("QUEUE_OUTBOX_STORED_STRING_INVALID"));
    }
    Ok(Arc::from(value))
}

fn nonnegative_u64(document: &Document, field: &str) -> Result<u64, MongoReliabilityError> {
    let value = document
        .get_i64(field)
        .map_err(|_| invalid("QUEUE_OUTBOX_STORED_ATTEMPT_INVALID"))?;
    u64::try_from(value).map_err(|_| invalid("QUEUE_OUTBOX_STORED_ATTEMPT_INVALID"))
}

fn positive_u64(document: &Document, field: &str) -> Result<u64, MongoReliabilityError> {
    nonnegative_u64(document, field).and_then(|value| {
        (value > 0)
            .then_some(value)
            .ok_or_else(|| invalid("QUEUE_OUTBOX_STORED_ATTEMPT_INVALID"))
    })
}

fn exactly_one(modified: u64) -> Result<(), MongoReliabilityError> {
    if modified == 1 {
        Ok(())
    } else {
        Err(MongoReliabilityError::OutboxClaimLost)
    }
}

fn failure_code_is_valid(code: &str) -> bool {
    (1..=MAX_FAILURE_CODE_BYTES).contains(&code.len())
        && code
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}

const fn invalid(code: &'static str) -> MongoReliabilityError {
    MongoReliabilityError::InvalidContract { code }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::pending;

    fn index(name: &str, keys: Document, unique: bool) -> mongodb::IndexModel {
        mongodb::IndexModel::builder()
            .keys(keys)
            .options(
                mongodb::options::IndexOptions::builder()
                    .name(name.to_owned())
                    .unique(unique)
                    .build(),
            )
            .build()
    }

    #[test]
    fn inbox_identity_is_stable_and_handler_specific() {
        let event = Uuid::parse_str("7e0dbb62-bb31-467b-8566-98326814a4ab").unwrap();
        assert_eq!(
            inbox_key("orders.created@1/json", event),
            inbox_key("orders.created@1/json", event)
        );
        assert_ne!(
            inbox_key("orders.created@1/json", event),
            inbox_key("orders.created@2/json", event)
        );
    }

    #[test]
    fn retry_backoff_is_bounded() {
        let policy = MongoTransactionalInboxConfig {
            max_transaction_attempts: 100,
            retry_initial_backoff_millis: 10,
            retry_max_backoff_millis: 80,
            commit_retry_timeout_millis: 100,
        };
        assert_eq!(transaction_backoff(&policy, 1), Duration::from_millis(10));
        assert_eq!(transaction_backoff(&policy, 4), Duration::from_millis(80));
        assert_eq!(transaction_backoff(&policy, 100), Duration::from_millis(80));
    }

    #[tokio::test(start_paused = true)]
    async fn heartbeat_abort_without_join_cannot_authorize_external_lease_release() {
        let tasks = crate::owned_tasks::OwnedTasks::default();
        let task = tasks.spawn(pending());
        let heartbeat = LeaseHeartbeat::new(CancellationToken::new(), task.clone());
        let now = Instant::now();
        assert!(!heartbeat.stop_before(Some(now), Some(now)).await);
        assert!(task.join().await.unwrap_err().is_cancelled());
        assert!(tasks.reconciled());
    }

    #[tokio::test(start_paused = true)]
    async fn force_retains_transaction_owner_until_its_fixed_cutoff_and_confirms_join() {
        let tracker = Arc::new(TransactionTracker::default());
        tracker.open();
        let started = Instant::now();
        tracker
            .owners
            .budget
            .install(crate::shutdown_budget::QueueShutdownDeadlines::before(
                started,
                started + Duration::from_secs(2),
            ));
        let lease = tracker.begin(Uuid::new_v4()).unwrap();
        let owners = tracker.owners.clone();
        let task = tracker.owners.tasks.spawn(async move {
            let _lease = lease;
            assert!(owners.run(None, pending::<()>()).await.is_none());
        });
        signal_force_drain(&tracker);
        assert!(!task.terminal(), "request is not a join");
        assert_eq!(tracker.active.load(Ordering::Acquire), 1);
        assert_eq!(
            drain_tracker(&tracker, Duration::from_secs(30)).await,
            Err(MongoReliabilityError::TransactionTerminationIncomplete)
        );
        assert_eq!(Instant::now() - started, Duration::from_secs(1));
        assert!(
            !tracker.force.is_cancelled(),
            "notification must not revoke driver authority during cooperation"
        );
        assert!(!tracker.accepting.load(Ordering::Acquire));
        assert_eq!(tracker.active.load(Ordering::Acquire), 0);
        task.join().await.unwrap();
        assert!(tracker.owners.tasks.reconciled());
    }

    #[test]
    fn delivery_body_deadline_preserves_bounded_transaction_finalization_time() {
        let now = Instant::now();
        let aggregate = now + Duration::from_secs(2);
        let (body, work, cleanup) = transaction_execution_deadlines(aggregate, now);
        assert_eq!(work, aggregate);
        assert_eq!(cleanup, aggregate);
        assert_eq!(aggregate.duration_since(body), Duration::from_millis(100));

        let short_aggregate = now + Duration::from_millis(40);
        let (short_body, short_work, short_cleanup) =
            transaction_execution_deadlines(short_aggregate, now);
        assert_eq!(short_work, short_aggregate);
        assert_eq!(short_cleanup, short_aggregate);
        assert_eq!(
            short_aggregate.duration_since(short_body),
            Duration::from_millis(10)
        );
    }

    #[test]
    fn failure_codes_are_bounded_tokens() {
        assert!(failure_code_is_valid("QUEUE_RELAY_FAILED"));
        assert!(!failure_code_is_valid(""));
        assert!(!failure_code_is_valid("queue.failed"));
        assert!(!failure_code_is_valid(
            &"A".repeat(MAX_FAILURE_CODE_BYTES + 1)
        ));
    }

    #[test]
    fn retention_duration_conversion_is_exact_and_overflow_safe() {
        assert_eq!(duration_seconds_as_i64_millis(0), Ok(0));
        assert_eq!(duration_seconds_as_i64_millis(60), Ok(60_000));
        assert!(duration_seconds_as_i64_millis(u64::MAX).is_err());
    }

    #[test]
    fn schema_index_contract_accepts_only_id_plus_the_exact_expected_set() {
        let expected = [("lily_idx_state", doc! { "state": 1 }, false)];
        let canonical = vec![
            index("_id_", doc! { "_id": 1 }, true),
            index("lily_idx_state", doc! { "state": 1 }, false),
        ];
        assert!(indexes_match_expected(&canonical, &expected));

        let mut unexpected = canonical.clone();
        unexpected.push(index("application_index", doc! { "tenant": 1 }, false));
        assert!(!indexes_match_expected(&unexpected, &expected));
        assert!(!indexes_match_expected(&canonical[1..], &expected));

        let wrong_keys = vec![
            index("_id_", doc! { "_id": 1 }, true),
            index("lily_idx_state", doc! { "state": -1 }, false),
        ];
        assert!(!indexes_match_expected(&wrong_keys, &expected));
    }

    #[test]
    fn schema_collection_contract_rejects_destructive_or_non_collection_shapes() {
        let validator = doc! { "$jsonSchema": { "bsonType": "object" } };
        let mut canonical = CollectionSpecification::default();
        canonical.name = "_lily_queue_inbox".to_owned();
        canonical.options.validator = Some(validator.clone());
        canonical.options.validation_level = Some(ValidationLevel::Strict);
        canonical.options.validation_action = Some(ValidationAction::Error);
        assert!(collection_specification_matches(&canonical, &validator));

        let mut capped = canonical.clone();
        capped.options.capped = Some(true);
        capped.options.size = Some(1_024);
        assert!(!collection_specification_matches(&capped, &validator));

        let mut read_only = canonical.clone();
        read_only.info.read_only = true;
        assert!(!collection_specification_matches(&read_only, &validator));

        let mut view = canonical.clone();
        view.collection_type = CollectionType::View;
        view.options.view_on = Some("application_data".to_owned());
        assert!(!collection_specification_matches(&view, &validator));

        let mut expiring = canonical;
        expiring.options.expire_after_seconds = Some(Duration::from_secs(60));
        assert!(!collection_specification_matches(&expiring, &validator));
    }

    #[test]
    fn readiness_queries_only_the_exact_framework_collection_names() {
        assert_eq!(
            required_collection_filter(),
            doc! { "name": { "$in": [
                SCHEMA_COLLECTION,
                INBOX_COLLECTION,
                LEASE_COLLECTION,
                OUTBOX_COLLECTION,
            ] } }
        );
        let exact = REQUIRED_COLLECTIONS.map(str::to_owned);
        assert!(required_collection_names_are_complete(&exact));
        assert!(!required_collection_names_are_complete(&exact[..3]));

        let mut replaced = exact;
        replaced[3] = "application_unrelated".to_owned();
        assert!(!required_collection_names_are_complete(&replaced));
    }

    #[test]
    fn external_lease_release_is_advisory_after_commit_and_never_replaces_primary_failure() {
        let ledger = TransactionExecutionLedger::default();
        let committed = retain_primary_transaction_result(
            &ledger,
            Ok::<_, QueueHandlerError>(TransactionalExecution::<()>::AlreadyCompleted),
            Err(MongoReliabilityError::InboxLeaseLost),
        )
        .unwrap();
        assert_eq!(committed, TransactionalExecution::AlreadyCompleted);
        assert_eq!(ledger.snapshot(0).post_commit_release_failures_total, 1);
        assert_eq!(
            ledger.snapshot(0).last_failure_code,
            Some("QUEUE_MONGODB_INBOX_LEASE_LOST")
        );

        let primary = QueueHandlerError::permanent("APPLICATION_PRIMARY_FAILURE");
        let error = retain_primary_transaction_result::<(), QueueHandlerError>(
            &ledger,
            Err(primary),
            Err(MongoReliabilityError::TransactionDeadlineExceeded),
        )
        .unwrap_err();
        assert_eq!(error.code(), "APPLICATION_PRIMARY_FAILURE");
        assert_eq!(ledger.snapshot(0).post_commit_release_failures_total, 1);
    }

    #[test]
    fn transactional_in_progress_is_deferred_only_for_broker_deliveries() {
        assert!(matches!(
            classify_delivery_execution::<()>(TransactionalExecution::InProgress, true),
            MongoDeliveryExecution::ContentionDeferred
        ));
        assert!(matches!(
            classify_delivery_execution::<()>(TransactionalExecution::InProgress, false),
            MongoDeliveryExecution::Completed(TransactionalExecution::InProgress)
        ));
    }

    #[test]
    fn application_error_code_cannot_spoof_unknown_commit_control_flow() {
        let internal =
            Err::<TransactionalExecution<()>, _>(TransactionAttemptError::CommitOutcomeUnknown);
        assert!(retain_external_lease_after(&internal));

        let spoofed = Err::<TransactionalExecution<()>, _>(TransactionAttemptError::Handler(
            QueueHandlerError::retryable("QUEUE_MONGODB_COMMIT_OUTCOME_UNKNOWN"),
        ));
        assert!(!retain_external_lease_after(&spoofed));
    }

    #[test]
    fn transaction_counters_saturate_instead_of_wrapping() {
        let counter = AtomicU64::new(u64::MAX - 1);
        saturating_increment(&counter);
        saturating_increment(&counter);
        assert_eq!(counter.load(Ordering::Acquire), u64::MAX);
    }

    #[test]
    fn commit_operation_timeout_and_cancellation_are_unknown_outcomes() {
        assert_eq!(
            map_commit_operation_error(MongoDbError::OperationTimedOut),
            MongoReliabilityError::CommitOutcomeUnknown
        );
        assert_eq!(
            map_commit_operation_error(MongoDbError::OperationCancelled),
            MongoReliabilityError::CommitOutcomeUnknown
        );
        assert_eq!(
            map_commit_operation_error(MongoDbError::TransientTransaction),
            MongoReliabilityError::TransientTransaction
        );
    }

    #[test]
    fn non_commit_operation_timeout_and_cancellation_keep_typed_lifecycle_meaning() {
        assert_eq!(
            MongoReliabilityError::from(MongoDbError::OperationCancelled),
            MongoReliabilityError::TransactionCancelled
        );
        assert_eq!(
            MongoReliabilityError::from(MongoDbError::OperationTimedOut),
            MongoReliabilityError::TransactionDeadlineExceeded
        );
    }
}
