use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use lily_config::TransactionalInboxConfig;
use lily_error::application::QueueHandlerError;
use lily_injection::{ApplicationContainer, Extensions};
use lily_postgresql::diesel::OptionalExtension as _;
use lily_postgresql::{PgDatabaseService, PgTransaction, diesel, diesel_async};
use sha2::{Digest, Sha256};
use tokio::sync::Notify;
use uuid::Uuid;

use super::PostgresReliabilityError;
use super::migration::ensure_schema_ready;
use crate::transactional::{
    CleanupReport, MAX_OUTBOX_MESSAGE_BYTES, TransactionalExecution, TransactionalOutboxMessage,
    handler_identity_is_valid,
};

const INBOX_PROCESSING: i32 = 0;
const INBOX_COMPLETED: i32 = 1;
const INBOX_IN_PROGRESS: i32 = 2;
const INBOX_ADVISORY_LOCK_DOMAIN: &[u8] = b"lily.queue.inbox.v1\0";

#[derive(diesel::QueryableByName)]
struct AdvisoryLockRow {
    #[diesel(sql_type = diesel::sql_types::Bool)]
    acquired: bool,
}

#[derive(diesel::QueryableByName)]
struct ClaimStateRow {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    state: i32,
}

#[derive(diesel::QueryableByName)]
struct ClaimedOutboxRow {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    record_id: Uuid,
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    event_id: Uuid,
    #[diesel(sql_type = diesel::sql_types::Text)]
    exchange_name: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    routing_key: String,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    schema_version: i32,
    #[diesel(sql_type = diesel::sql_types::Text)]
    content_kind: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    content_type: String,
    #[diesel(sql_type = diesel::sql_types::Binary)]
    body: Vec<u8>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    traceparent: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    claim_token: Uuid,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    publish_attempts: i64,
}

#[derive(diesel::QueryableByName)]
struct CountRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    count: i64,
}

#[derive(diesel::QueryableByName)]
struct PublishAttemptRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    publish_attempts: i64,
}

#[derive(diesel::QueryableByName)]
struct OutboxBodyBytesRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    body_bytes: i64,
}

enum ClaimOutboxBatchResult {
    Rows(Vec<ClaimedOutboxRow>),
    Oversized { body_bytes: i64 },
}

enum InboxClaimState {
    Acquired(Uuid),
    AlreadyCompleted,
    InProgress,
}

/// One durable outbox row exclusively leased to a relay iteration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaimedOutboxMessage {
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

impl ClaimedOutboxMessage {
    /// Lily-owned database record identity.
    #[must_use]
    pub const fn record_id(&self) -> Uuid {
        self.record_id
    }

    /// Stable event identity reused for every broker attempt.
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

    /// Canonical AMQP content type paired with the content kind.
    #[must_use]
    pub fn content_type(&self) -> &str {
        &self.content_type
    }

    /// Serialized message body.
    #[must_use]
    pub fn body(&self) -> &[u8] {
        &self.body
    }

    /// W3C parent captured when the outbox row was created.
    #[must_use]
    pub fn traceparent(&self) -> Option<&str> {
        self.traceparent.as_deref()
    }

    /// Opaque ownership token required for terminal storage mutations.
    #[must_use]
    pub const fn claim_token(&self) -> Uuid {
        self.claim_token
    }

    /// Persisted broker attempt count before this lease enters publish I/O.
    ///
    /// Leasing a batch does not consume publish attempts. The relay advances
    /// this value for one claim-token-fenced record immediately before it
    /// starts that record's broker operation.
    #[must_use]
    pub const fn publish_attempts(&self) -> u64 {
        self.publish_attempts
    }
}

/// Handler-side access to the exact transaction owning inbox completion.
///
/// This type is a parts extractor and does not consume the delivery body. It
/// exposes ordinary Diesel operations and durable outbox insertion, but never
/// commit, rollback, inbox ownership, or broker settlement.
///
/// Lily inserts it only for a handler whose `#[queue]` contract selects
/// `delivery_guarantee = "transactional_inbox"`. The attribute alone cannot
/// make an independently injected repository or pool connection atomic: every
/// covered business mutation must run through [`Self::with_connection`], and
/// every covered outgoing event through [`Self::enqueue`]. Extracting this
/// type from an at-least-once handler fails closed before application code.
#[derive(Clone)]
pub struct PostgresTransaction {
    transaction: PgTransaction,
    source_handler: Arc<str>,
    source_event_id: Uuid,
    max_outbox_bytes: usize,
}

impl PostgresTransaction {
    /// Runs one Diesel operation on the transaction's exact connection.
    pub async fn with_connection<T, Operation>(
        &self,
        operation: Operation,
    ) -> Result<T, PostgresReliabilityError>
    where
        T: Send + 'static,
        Operation: for<'connection> FnOnce(
                &'connection mut lily_postgresql::diesel_async::AsyncPgConnection,
            )
                -> lily_postgresql::PgConnectionFuture<'connection, T>
            + Send
            + 'static,
    {
        self.transaction
            .with_connection(operation)
            .await
            .map_err(PostgresReliabilityError::from)
    }

    /// Inserts one durable event through the active business transaction.
    pub async fn enqueue(
        &self,
        message: TransactionalOutboxMessage,
    ) -> Result<(), PostgresReliabilityError> {
        if message.body.len() > self.max_outbox_bytes {
            return Err(PostgresReliabilityError::InvalidContract {
                code: "QUEUE_OUTBOX_POLICY_BYTES_EXCEEDED",
            });
        }
        let source_handler = Arc::clone(&self.source_handler);
        let source_event_id = self.source_event_id;
        let record_id = Uuid::new_v4();
        self.transaction
            .with_connection(move |connection| {
                Box::pin(async move {
                    use diesel_async::RunQueryDsl as _;

                    diesel::sql_query(
                        r#"INSERT INTO lily_queue.outbox (
                            record_id, source_handler_identity, source_event_id, event_id,
                            exchange_name, routing_key, schema_version, content_kind,
                            content_type, body, traceparent
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)"#,
                    )
                    .bind::<diesel::sql_types::Uuid, _>(record_id)
                    .bind::<diesel::sql_types::Text, _>(source_handler.as_ref())
                    .bind::<diesel::sql_types::Uuid, _>(source_event_id)
                    .bind::<diesel::sql_types::Uuid, _>(message.event_id)
                    .bind::<diesel::sql_types::Text, _>(message.exchange.as_ref())
                    .bind::<diesel::sql_types::Text, _>(message.routing_key.as_ref())
                    .bind::<diesel::sql_types::Integer, _>(i32::from(message.schema_version))
                    .bind::<diesel::sql_types::Text, _>(message.content_kind.as_ref())
                    .bind::<diesel::sql_types::Text, _>(message.content_type.as_ref())
                    .bind::<diesel::sql_types::Binary, _>(message.body.as_ref())
                    .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(
                        message.traceparent.as_deref(),
                    )
                    .execute(connection)
                    .await?;
                    Ok(())
                })
            })
            .await
            .map_err(|error| match error {
                lily_postgresql::PgError::Query {
                    kind: lily_postgresql::PgQueryErrorKind::ConstraintViolation,
                } => PostgresReliabilityError::InvalidContract {
                    code: "QUEUE_OUTBOX_CONSTRAINT_VIOLATION",
                },
                error => PostgresReliabilityError::from(error),
            })
    }
}

impl crate::FromDeliveryParts for PostgresTransaction {
    type Rejection = QueueHandlerError;

    async fn from_delivery_parts(
        invocation: &mut crate::DeliveryInvocation,
    ) -> Result<Self, Self::Rejection> {
        invocation.remove_local::<Self>().ok_or_else(|| {
            QueueHandlerError::permanent("QUEUE_POSTGRES_TRANSACTION_CONTEXT_UNAVAILABLE")
        })
    }
}

#[derive(Default)]
struct TransactionTracker {
    accepting: AtomicBool,
    active: AtomicUsize,
    changed: Notify,
    owners: Arc<crate::owned_tasks::TransactionTasks>,
}

impl TransactionTracker {
    fn open(&self) {
        self.accepting.store(true, Ordering::Release);
    }

    fn begin(self: &Arc<Self>) -> Result<TransactionLease, PostgresReliabilityError> {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(PostgresReliabilityError::TransactionAdmissionClosed);
        }
        self.active.fetch_add(1, Ordering::AcqRel);
        if !self.accepting.load(Ordering::Acquire) {
            self.finish();
            return Err(PostgresReliabilityError::TransactionAdmissionClosed);
        }
        Ok(TransactionLease {
            tracker: Arc::clone(self),
        })
    }

    fn finish(&self) {
        if self.active.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.changed.notify_waiters();
        }
    }
}

struct TransactionWaiter(Option<tokio::sync::oneshot::Sender<()>>);
impl Drop for TransactionWaiter {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

struct TransactionLease {
    tracker: Arc<TransactionTracker>,
}

impl Drop for TransactionLease {
    fn drop(&mut self) {
        self.tracker.finish();
    }
}

/// Prepared, schema-qualified PostgreSQL transactional inbox/outbox runtime.
///
/// This is cross-crate framework ABI. Applications interact with
/// [`PostgresTransaction`] inside typed handlers and with the explicit
/// [`super::PostgresInboxOutboxMigrator`] deployment boundary.
pub struct PostgresTransactionalRuntime {
    database: Arc<PgDatabaseService>,
    config: TransactionalInboxConfig,
    queue_identity: Arc<str>,
    database_cell: Option<Arc<str>>,
    transactions: Arc<TransactionTracker>,
}

impl PostgresTransactionalRuntime {
    /// Resolves the configured PostgreSQL authority from one application.
    pub async fn resolve(
        container: Arc<ApplicationContainer>,
        config: TransactionalInboxConfig,
    ) -> Result<Self, PostgresReliabilityError> {
        let services = container.services();
        Self::resolve_extensions(services, config).await
    }

    async fn resolve_extensions(
        extensions: Arc<Extensions>,
        config: TransactionalInboxConfig,
    ) -> Result<Self, PostgresReliabilityError> {
        #[cfg(feature = "transactional-inbox-postgresql")]
        let database = {
            if config.database_cell.is_some() {
                return Err(PostgresReliabilityError::InvalidContract {
                    code: "QUEUE_POSTGRES_SINGLE_CELL_FORBIDDEN",
                });
            }
            extensions
                .get_service::<PgDatabaseService>(None)
                .await
                .map_err(|_| PostgresReliabilityError::DependencyUnavailable)?
        };

        #[cfg(feature = "transactional-inbox-postgresql-factory")]
        let database = {
            let cell = config.database_cell.as_deref().ok_or(
                PostgresReliabilityError::InvalidContract {
                    code: "QUEUE_POSTGRES_FACTORY_CELL_REQUIRED",
                },
            )?;
            let factory = extensions
                .get_service::<lily_postgresql::PgFactory>(None)
                .await
                .map_err(|_| PostgresReliabilityError::DependencyUnavailable)?;
            factory
                .get(cell)
                .map_err(|_| PostgresReliabilityError::DependencyUnavailable)?
        };

        Self::from_database(database, config).await
    }

    /// Prepares a runtime from an already selected application-owned service.
    pub async fn from_database(
        database: Arc<PgDatabaseService>,
        config: TransactionalInboxConfig,
    ) -> Result<Self, PostgresReliabilityError> {
        validate_policy(&config)?;
        ensure_schema_ready(&database).await?;
        let transactions = Arc::new(TransactionTracker::default());
        transactions.open();
        let database_cell = config.database_cell.as_deref().map(Arc::from);
        Ok(Self {
            database,
            config,
            queue_identity: Arc::from("manual"),
            database_cell,
            transactions,
        })
    }

    pub(crate) fn bind_queue_identity(&mut self, queue: &str) {
        self.queue_identity = Arc::from(queue);
    }

    /// Performs the same read-only schema check used before broker admission.
    pub async fn ensure_schema_ready(&self) -> Result<(), PostgresReliabilityError> {
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

    /// Validated bounded policy used by this exact queue binding.
    #[must_use]
    pub(crate) const fn config(&self) -> &TransactionalInboxConfig {
        &self.config
    }

    /// Number of transactions still owned by PostgreSQL finalization.
    #[must_use]
    pub fn active_transactions(&self) -> usize {
        self.transactions.active.load(Ordering::Acquire)
    }

    /// Rejects new transactional deliveries without cancelling admitted work.
    pub fn stop_transaction_admission(&self) {
        self.transactions.accepting.store(false, Ordering::Release);
        self.transactions.changed.notify_waiters();
    }

    /// Waits until every admitted transaction commits or rolls back.
    pub async fn drain_transactions(
        &self,
        timeout: Duration,
    ) -> Result<(), PostgresReliabilityError> {
        let tracker = &self.transactions;
        let deadline = tracker
            .owners
            .budget
            .cap(tokio::time::Instant::now() + timeout);
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
                if self.active_transactions() == 0 {
                    return true;
                }
                changed.await;
            }
        };
        if !self.transactions_reconciled()
            && tracker.owners.budget.run_until(deadline, drain).await != Ok(true)
        {
            return Err(PostgresReliabilityError::TransactionDrainTimeout {
                remaining: self.active_transactions(),
            });
        }
        if tracker.owners.tasks.panicked() || tracker.owners.interrupted.load(Ordering::Acquire) {
            return Err(PostgresReliabilityError::TransactionTerminationIncomplete);
        }
        Ok(())
    }

    pub(crate) fn set_shutdown_deadlines(
        &self,
        deadlines: crate::shutdown_budget::QueueShutdownDeadlines,
    ) {
        self.transactions.owners.budget.install(deadlines);
    }

    pub(crate) fn request_force(&self) {
        self.stop_transaction_admission();
        self.transactions.owners.request_force();
    }

    pub(crate) fn transactions_reconciled(&self) -> bool {
        self.active_transactions() == 0 && self.transactions.owners.tasks.reconciled()
    }

    /// Claims one inbox identity, executes application work, and commits it
    /// with inbox completion and any enqueued outbox rows.
    ///
    /// The transaction lease is moved into PostgreSQL's owner task. If the
    /// awaiting delivery task is cancelled, active evidence remains non-zero
    /// until that owner finishes rollback.
    pub async fn execute<T, Operation, OperationFuture>(
        &self,
        logical_handler: impl AsRef<str>,
        event_id: Uuid,
        operation: Operation,
    ) -> Result<TransactionalExecution<T>, QueueHandlerError>
    where
        T: Send + 'static,
        Operation: FnOnce(PostgresTransaction) -> OperationFuture + Send + 'static,
        OperationFuture: Future<Output = Result<T, QueueHandlerError>> + Send + 'static,
    {
        self.execute_before(logical_handler, event_id, None, operation)
            .await
    }

    pub(crate) async fn execute_delivery<T, Operation, OperationFuture>(
        &self,
        logical_handler: impl AsRef<str>,
        event_id: Uuid,
        deadline: tokio::time::Instant,
        operation: Operation,
    ) -> Result<TransactionalExecution<T>, QueueHandlerError>
    where
        T: Send + 'static,
        Operation: FnOnce(PostgresTransaction) -> OperationFuture + Send + 'static,
        OperationFuture: Future<Output = Result<T, QueueHandlerError>> + Send + 'static,
    {
        self.execute_before(logical_handler, event_id, Some(deadline), operation)
            .await
    }

    async fn execute_before<T, Operation, OperationFuture>(
        &self,
        logical_handler: impl AsRef<str>,
        event_id: Uuid,
        deadline: Option<tokio::time::Instant>,
        operation: Operation,
    ) -> Result<TransactionalExecution<T>, QueueHandlerError>
    where
        T: Send + 'static,
        Operation: FnOnce(PostgresTransaction) -> OperationFuture + Send + 'static,
        OperationFuture: Future<Output = Result<T, QueueHandlerError>> + Send + 'static,
    {
        let logical_handler = logical_handler.as_ref();
        if !handler_identity_is_valid(logical_handler) || event_id.is_nil() {
            return Err(QueueHandlerError::permanent(
                "QUEUE_POSTGRES_INBOX_IDENTITY_INVALID",
            ));
        }
        let lease = self.transactions.begin().map_err(reliability_as_handler)?;
        let logical_handler: Arc<str> = Arc::from(logical_handler);
        let lock_timeout = self.config.inbox_lock_timeout_millis;
        let max_outbox_bytes = self
            .config
            .relay_max_in_flight_bytes
            .min(MAX_OUTBOX_MESSAGE_BYTES);
        let retained_handler_error = Arc::new(Mutex::new(None));
        let retained_for_owner = Arc::clone(&retained_handler_error);

        let database = Arc::clone(&self.database);
        let owners = Arc::clone(&self.transactions.owners);
        let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel();
        let _cancel_on_drop = TransactionWaiter(Some(cancel_tx));
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let owner = self.transactions.owners.tasks.spawn(async move {
            let _lease = lease;
            let transaction = database.transaction_with_handle_owned(
                move |transaction| async move {
                    set_local_lock_timeout(&transaction, lock_timeout).await?;
                    match claim_inbox(&transaction, &logical_handler, event_id, lock_timeout)
                        .await?
                    {
                        InboxClaimState::AlreadyCompleted => {
                            Ok(TransactionalExecution::AlreadyCompleted)
                        }
                        InboxClaimState::InProgress => Ok(TransactionalExecution::InProgress),
                        InboxClaimState::Acquired(lock_token) => {
                            let context = PostgresTransaction {
                                transaction: transaction.clone(),
                                source_handler: Arc::clone(&logical_handler),
                                source_event_id: event_id,
                                max_outbox_bytes,
                            };
                            let value = match operation(context).await {
                                Ok(value) => value,
                                Err(error) => {
                                    *retained_for_owner
                                        .lock()
                                        .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                                        Some(error);
                                    return Err(lily_postgresql::PgError::TransactionFinalization);
                                }
                            };
                            complete_inbox(&transaction, &logical_handler, event_id, lock_token)
                                .await?;
                            Ok(TransactionalExecution::Applied(value))
                        }
                    }
                },
                cancel_rx,
            );
            let result = owners
                .run(deadline, transaction)
                .await
                .unwrap_or(Err(lily_postgresql::PgError::TransactionFinalization));
            let _ = result_tx.send(result);
        });
        let result = result_rx
            .await
            .unwrap_or(Err(lily_postgresql::PgError::TransactionFinalization));
        // Returning a value cannot outrun the actual owner task's final drop.
        if owner.join().await.is_err() {
            return Err(QueueHandlerError::retryable(
                "QUEUE_POSTGRES_TRANSACTION_OWNER_JOIN_FAILED",
            ));
        }

        match result {
            Ok(result) => Ok(result),
            Err(lily_postgresql::PgError::TransactionFinalization) => Err(retained_handler_error
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take()
                .unwrap_or_else(|| {
                    reliability_as_handler(lily_postgresql::PgError::TransactionFinalization.into())
                })),
            Err(error) => Err(reliability_as_handler(error.into())),
        }
    }

    /// Atomically leases a bounded outbox batch using `FOR UPDATE SKIP LOCKED`.
    pub async fn claim_outbox_batch(
        &self,
        claim_owner: Uuid,
    ) -> Result<Vec<ClaimedOutboxMessage>, PostgresReliabilityError> {
        if claim_owner.is_nil() {
            return Err(PostgresReliabilityError::InvalidContract {
                code: "QUEUE_OUTBOX_CLAIM_TOKEN_INVALID",
            });
        }
        let batch_size = i64::try_from(self.config.relay_batch_size).map_err(|_| {
            PostgresReliabilityError::InvalidContract {
                code: "QUEUE_OUTBOX_BATCH_SIZE_INVALID",
            }
        })?;
        let max_bytes = i64::try_from(self.config.relay_max_in_flight_bytes).map_err(|_| {
            PostgresReliabilityError::InvalidContract {
                code: "QUEUE_OUTBOX_BATCH_BYTES_INVALID",
            }
        })?;
        let claim_millis = i64::try_from(self.config.outbox_claim_lease_millis).map_err(|_| {
            PostgresReliabilityError::InvalidContract {
                code: "QUEUE_OUTBOX_CLAIM_LEASE_INVALID",
            }
        })?;
        let max_attempts = i64::from(self.config.relay_max_publish_attempts);
        let result = self
            .database
            .transaction(None, |connection, _| {
                Box::pin(async move {
                    use diesel_async::RunQueryDsl as _;

                    let oldest = diesel::sql_query(
                        r#"SELECT OCTET_LENGTH(body)::BIGINT AS body_bytes
                           FROM lily_queue.outbox
                           WHERE delivered_at IS NULL
                             AND available_at <= NOW()
                             AND (claimed_until IS NULL OR claimed_until <= NOW())
                             AND publish_attempts < $1
                           ORDER BY created_at, record_id
                           FOR UPDATE SKIP LOCKED
                           LIMIT 1"#,
                    )
                    .bind::<diesel::sql_types::BigInt, _>(max_attempts)
                    .load::<OutboxBodyBytesRow>(connection)
                    .await?
                    .into_iter()
                    .next();
                    if let Some(oldest) = oldest
                        && oldest.body_bytes > max_bytes
                    {
                        return Ok(ClaimOutboxBatchResult::Oversized {
                            body_bytes: oldest.body_bytes,
                        });
                    }

                    let rows = diesel::sql_query(
                        r#"WITH candidates AS (
                            SELECT record_id, created_at, body
                            FROM lily_queue.outbox
                            WHERE delivered_at IS NULL
                              AND available_at <= NOW()
                              AND (claimed_until IS NULL OR claimed_until <= NOW())
                              AND publish_attempts < $4
                            ORDER BY created_at, record_id
                            FOR UPDATE SKIP LOCKED
                            LIMIT $1
                        ), ranked AS (
                            SELECT record_id,
                                   SUM(OCTET_LENGTH(body)) OVER (
                                       ORDER BY created_at, record_id
                                   ) AS retained_bytes
                            FROM candidates
                        ), bounded AS (
                            SELECT record_id FROM ranked WHERE retained_bytes <= $2
                        ), updated AS (
                            UPDATE lily_queue.outbox AS outbox
                            SET claim_token = $3,
                                claimed_until = NOW() + ($5 * INTERVAL '1 millisecond')
                            FROM bounded
                            WHERE outbox.record_id = bounded.record_id
                            RETURNING outbox.*
                        )
                        SELECT record_id, event_id, exchange_name, routing_key,
                               schema_version, content_kind, content_type, body,
                               traceparent, claim_token, publish_attempts
                        FROM updated ORDER BY created_at, record_id"#,
                    )
                    .bind::<diesel::sql_types::BigInt, _>(batch_size)
                    .bind::<diesel::sql_types::BigInt, _>(max_bytes)
                    .bind::<diesel::sql_types::Uuid, _>(claim_owner)
                    .bind::<diesel::sql_types::BigInt, _>(max_attempts)
                    .bind::<diesel::sql_types::BigInt, _>(claim_millis)
                    .load::<ClaimedOutboxRow>(connection)
                    .await?;
                    Ok(ClaimOutboxBatchResult::Rows(rows))
                })
            })
            .await?;

        match result {
            ClaimOutboxBatchResult::Rows(rows) => {
                rows.into_iter().map(map_claimed_outbox).collect()
            }
            ClaimOutboxBatchResult::Oversized { body_bytes } => {
                let bytes = usize::try_from(body_bytes).map_err(|_| {
                    PostgresReliabilityError::InvalidContract {
                        code: "QUEUE_OUTBOX_STORED_BODY_LENGTH_INVALID",
                    }
                })?;
                Err(PostgresReliabilityError::OutboxRecordTooLarge {
                    bytes,
                    maximum: self.config.relay_max_in_flight_bytes,
                })
            }
        }
    }

    /// Admits one leased row to broker publish and persists its attempt.
    ///
    /// Batch leasing deliberately does not advance this counter: a forced
    /// shutdown may abandon the unvisited tail of a leased batch. Advancing
    /// here ensures only the exact record entering broker I/O consumes the
    /// bounded publish budget. The same fenced update renews the current
    /// record's lease. An expired lease remains renewable only while its exact
    /// claim token is still authoritative; a competing reclaimer replaces the
    /// token and makes this update fail closed.
    pub async fn begin_outbox_publish(
        &self,
        record_id: Uuid,
        claim_token: Uuid,
    ) -> Result<u64, PostgresReliabilityError> {
        let maximum = i64::from(self.config.relay_max_publish_attempts);
        let claim_millis = i64::try_from(self.config.outbox_claim_lease_millis).map_err(|_| {
            PostgresReliabilityError::InvalidContract {
                code: "QUEUE_OUTBOX_CLAIM_LEASE_INVALID",
            }
        })?;
        use diesel_async::RunQueryDsl as _;

        let row = self
            .database
            .with_connection(
                move |connection, _| {
                    Box::pin(async move {
                        diesel::sql_query(
                            r#"UPDATE lily_queue.outbox
                           SET publish_attempts = publish_attempts + 1,
                               claimed_until = NOW() + ($4 * INTERVAL '1 millisecond')
                           WHERE record_id = $1 AND claim_token = $2
                             AND delivered_at IS NULL
                             AND publish_attempts < $3
                           RETURNING publish_attempts"#,
                        )
                        .bind::<diesel::sql_types::Uuid, _>(record_id)
                        .bind::<diesel::sql_types::Uuid, _>(claim_token)
                        .bind::<diesel::sql_types::BigInt, _>(maximum)
                        .bind::<diesel::sql_types::BigInt, _>(claim_millis)
                        .get_result::<PublishAttemptRow>(connection)
                        .await
                        .optional()
                        .map_err(Into::into)
                    })
                },
                None,
            )
            .await?
            .ok_or(PostgresReliabilityError::OutboxClaimLost)?;

        u64::try_from(row.publish_attempts).map_err(|_| PostgresReliabilityError::InvalidContract {
            code: "QUEUE_OUTBOX_STORED_ATTEMPT_INVALID",
        })
    }

    /// Marks a leased row delivered after Ack-without-Return confirmation.
    pub async fn mark_outbox_delivered(
        &self,
        record_id: Uuid,
        claim_token: Uuid,
    ) -> Result<(), PostgresReliabilityError> {
        use diesel_async::RunQueryDsl as _;

        let affected = self
            .database
            .with_connection(
                move |connection, _| {
                    Box::pin(async move {
                        diesel::sql_query(
                            r#"UPDATE lily_queue.outbox
                           SET delivered_at = NOW(), claim_token = NULL,
                               claimed_until = NULL, last_failure_code = NULL
                           WHERE record_id = $1 AND claim_token = $2
                             AND delivered_at IS NULL AND publish_attempts > 0"#,
                        )
                        .bind::<diesel::sql_types::Uuid, _>(record_id)
                        .bind::<diesel::sql_types::Uuid, _>(claim_token)
                        .execute(connection)
                        .await
                        .map_err(Into::into)
                    })
                },
                None,
            )
            .await?;
        if affected == 1 {
            Ok(())
        } else {
            Err(PostgresReliabilityError::OutboxClaimLost)
        }
    }

    /// Releases a failed publish for bounded delayed retry.
    ///
    /// The row is retained even when its persisted attempt count reaches the
    /// configured maximum. Such rows are excluded from future claims and are
    /// surfaced through [`Self::exhausted_outbox_count`].
    pub async fn record_outbox_failure(
        &self,
        record_id: Uuid,
        claim_token: Uuid,
        retry_after: Duration,
        failure_code: &'static str,
    ) -> Result<(), PostgresReliabilityError> {
        if !failure_code_is_valid(failure_code) {
            return Err(PostgresReliabilityError::InvalidContract {
                code: "QUEUE_OUTBOX_FAILURE_CODE_INVALID",
            });
        }
        let retry_millis = i64::try_from(retry_after.as_millis()).map_err(|_| {
            PostgresReliabilityError::InvalidContract {
                code: "QUEUE_OUTBOX_RETRY_DELAY_INVALID",
            }
        })?;
        use diesel_async::RunQueryDsl as _;
        let affected = self
            .database
            .with_connection(
                move |connection, _| {
                    Box::pin(async move {
                        diesel::sql_query(
                            r#"UPDATE lily_queue.outbox
                           SET available_at = NOW() + ($3 * INTERVAL '1 millisecond'),
                               claim_token = NULL, claimed_until = NULL,
                               last_failure_code = $4
                           WHERE record_id = $1 AND claim_token = $2
                             AND delivered_at IS NULL AND publish_attempts > 0"#,
                        )
                        .bind::<diesel::sql_types::Uuid, _>(record_id)
                        .bind::<diesel::sql_types::Uuid, _>(claim_token)
                        .bind::<diesel::sql_types::BigInt, _>(retry_millis)
                        .bind::<diesel::sql_types::Text, _>(failure_code)
                        .execute(connection)
                        .await
                        .map_err(Into::into)
                    })
                },
                None,
            )
            .await?;
        if affected == 1 {
            Ok(())
        } else {
            Err(PostgresReliabilityError::OutboxClaimLost)
        }
    }

    /// Counts retained undelivered rows that exhausted their publish budget.
    pub async fn exhausted_outbox_count(&self) -> Result<u64, PostgresReliabilityError> {
        use diesel_async::RunQueryDsl as _;
        let maximum = i64::from(self.config.relay_max_publish_attempts);
        let count = self
            .database
            .with_connection(
                move |connection, _| {
                    Box::pin(async move {
                        diesel::sql_query(
                            r#"SELECT COUNT(*)::BIGINT AS count FROM lily_queue.outbox
                           WHERE delivered_at IS NULL AND publish_attempts >= $1"#,
                        )
                        .bind::<diesel::sql_types::BigInt, _>(maximum)
                        .get_result::<CountRow>(connection)
                        .await
                        .map(|row| row.count)
                        .map_err(Into::into)
                    })
                },
                None,
            )
            .await?;
        u64::try_from(count).map_err(|_| PostgresReliabilityError::InvalidContract {
            code: "QUEUE_OUTBOX_EXHAUSTED_COUNT_INVALID",
        })
    }

    /// Deletes at most one configured batch from each completed ledger.
    pub async fn cleanup(&self) -> Result<CleanupReport, PostgresReliabilityError> {
        let limit = i64::try_from(self.config.relay_batch_size).map_err(|_| {
            PostgresReliabilityError::InvalidContract {
                code: "QUEUE_POSTGRES_CLEANUP_BATCH_INVALID",
            }
        })?;
        let inbox_retention = i64::try_from(self.config.inbox_retention_secs).map_err(|_| {
            PostgresReliabilityError::InvalidContract {
                code: "QUEUE_POSTGRES_INBOX_RETENTION_INVALID",
            }
        })?;
        let outbox_retention = i64::try_from(self.config.outbox_retention_secs).map_err(|_| {
            PostgresReliabilityError::InvalidContract {
                code: "QUEUE_POSTGRES_OUTBOX_RETENTION_INVALID",
            }
        })?;
        self.database
            .transaction(None, |connection, _| {
                Box::pin(async move {
                    use diesel_async::RunQueryDsl as _;

                    let inbox = diesel::sql_query(
                        r#"WITH expired AS (
                            SELECT handler_identity, event_id FROM lily_queue.inbox
                            WHERE state = 1
                              AND completed_at < NOW() - ($1 * INTERVAL '1 second')
                            ORDER BY completed_at, handler_identity, event_id
                            LIMIT $2 FOR UPDATE SKIP LOCKED
                        ) DELETE FROM lily_queue.inbox AS inbox USING expired
                          WHERE inbox.handler_identity = expired.handler_identity
                            AND inbox.event_id = expired.event_id"#,
                    )
                    .bind::<diesel::sql_types::BigInt, _>(inbox_retention)
                    .bind::<diesel::sql_types::BigInt, _>(limit)
                    .execute(connection)
                    .await?;
                    let outbox = diesel::sql_query(
                        r#"WITH expired AS (
                            SELECT record_id FROM lily_queue.outbox
                            WHERE delivered_at IS NOT NULL
                              AND delivered_at < NOW() - ($1 * INTERVAL '1 second')
                            ORDER BY delivered_at, record_id
                            LIMIT $2 FOR UPDATE SKIP LOCKED
                        ) DELETE FROM lily_queue.outbox AS outbox USING expired
                          WHERE outbox.record_id = expired.record_id"#,
                    )
                    .bind::<diesel::sql_types::BigInt, _>(outbox_retention)
                    .bind::<diesel::sql_types::BigInt, _>(limit)
                    .execute(connection)
                    .await?;
                    Ok(CleanupReport {
                        inbox_rows: inbox as u64,
                        outbox_rows: outbox as u64,
                    })
                })
            })
            .await
            .map_err(PostgresReliabilityError::from)
    }
}

async fn set_local_lock_timeout(
    transaction: &PgTransaction,
    milliseconds: u64,
) -> lily_postgresql::PgResult<()> {
    let timeout = format!("{milliseconds}ms");
    transaction
        .with_connection(move |connection| {
            Box::pin(async move {
                use diesel_async::RunQueryDsl as _;
                diesel::sql_query("SELECT set_config('lock_timeout', $1, true)")
                    .bind::<diesel::sql_types::Text, _>(timeout)
                    .execute(connection)
                    .await?;
                Ok(())
            })
        })
        .await
}

async fn claim_inbox(
    transaction: &PgTransaction,
    logical_handler: &str,
    event_id: Uuid,
    lock_timeout_millis: u64,
) -> lily_postgresql::PgResult<InboxClaimState> {
    // PostgreSQL's unique-index conflict arbitration waits for an uncommitted
    // conflicting row even when INSERT uses ON CONFLICT DO NOTHING. Acquire a
    // transaction-owned, non-blocking lock first so a concurrent duplicate is
    // classified as InProgress without waiting for the first handler's
    // transaction. The lock is only an admission gate: the exact table key is
    // still the sole deduplication authority.
    let advisory_key = inbox_advisory_lock_key(logical_handler, event_id);
    let acquired = transaction
        .with_connection(move |connection| {
            Box::pin(async move {
                use diesel_async::RunQueryDsl as _;
                diesel::sql_query("SELECT pg_try_advisory_xact_lock($1) AS acquired")
                    .bind::<diesel::sql_types::BigInt, _>(advisory_key)
                    .get_result::<AdvisoryLockRow>(connection)
                    .await
                    .map(|row| row.acquired)
                    .map_err(Into::into)
            })
        })
        .await?;
    if !acquired {
        // A 64-bit advisory-key collision can only cause a transient retry. It
        // cannot produce AlreadyCompleted, commit application work, or ACK a
        // delivery because the exact (handler_identity, event_id) row remains
        // the durable authority below.
        return Ok(InboxClaimState::InProgress);
    }

    let logical_handler = logical_handler.to_owned();
    let lock_timeout = i64::try_from(lock_timeout_millis)
        .map_err(|_| lily_postgresql::PgError::TransactionFinalization)?;
    let lock_token = Uuid::new_v4();
    let row = transaction
        .with_connection(move |connection| {
            Box::pin(async move {
                use diesel_async::RunQueryDsl as _;
                diesel::sql_query(
                    r#"WITH inserted AS (
                        INSERT INTO lily_queue.inbox (
                            handler_identity, event_id, state, lock_token, locked_until
                        ) VALUES ($1, $2, 0, $3,
                            NOW() + ($4 * INTERVAL '1 millisecond'))
                        ON CONFLICT (handler_identity, event_id) DO NOTHING
                        RETURNING 1
                    ), reclaimed AS (
                        UPDATE lily_queue.inbox
                        SET lock_token = $3,
                            locked_until = NOW() + ($4 * INTERVAL '1 millisecond'),
                            updated_at = NOW()
                        WHERE handler_identity = $1 AND event_id = $2
                          AND state = 0 AND locked_until <= NOW()
                          AND NOT EXISTS (SELECT 1 FROM inserted)
                        RETURNING 1
                    )
                    SELECT CASE
                        WHEN EXISTS (SELECT 1 FROM inserted)
                          OR EXISTS (SELECT 1 FROM reclaimed) THEN 0
                        WHEN EXISTS (
                            SELECT 1 FROM lily_queue.inbox
                            WHERE handler_identity = $1 AND event_id = $2 AND state = 1
                        ) THEN 1
                        ELSE 2
                    END AS state"#,
                )
                .bind::<diesel::sql_types::Text, _>(logical_handler)
                .bind::<diesel::sql_types::Uuid, _>(event_id)
                .bind::<diesel::sql_types::Uuid, _>(lock_token)
                .bind::<diesel::sql_types::BigInt, _>(lock_timeout)
                .get_result::<ClaimStateRow>(connection)
                .await
                .map_err(Into::into)
            })
        })
        .await?;
    match row.state {
        INBOX_PROCESSING => Ok(InboxClaimState::Acquired(lock_token)),
        INBOX_COMPLETED => Ok(InboxClaimState::AlreadyCompleted),
        INBOX_IN_PROGRESS => Ok(InboxClaimState::InProgress),
        _ => Err(lily_postgresql::PgError::TransactionFinalization),
    }
}

fn inbox_advisory_lock_key(logical_handler: &str, event_id: Uuid) -> i64 {
    let mut digest = Sha256::new();
    digest.update(INBOX_ADVISORY_LOCK_DOMAIN);
    digest.update(
        u32::try_from(logical_handler.len())
            .expect("validated handler identities fit in u32")
            .to_be_bytes(),
    );
    digest.update(logical_handler.as_bytes());
    digest.update(event_id.as_bytes());
    let bytes: [u8; 8] = digest.finalize()[..8]
        .try_into()
        .expect("SHA-256 output contains eight bytes");
    i64::from_be_bytes(bytes)
}

async fn complete_inbox(
    transaction: &PgTransaction,
    logical_handler: &str,
    event_id: Uuid,
    lock_token: Uuid,
) -> lily_postgresql::PgResult<()> {
    let logical_handler = logical_handler.to_owned();
    let affected = transaction
        .with_connection(move |connection| {
            Box::pin(async move {
                use diesel_async::RunQueryDsl as _;
                diesel::sql_query(
                    r#"UPDATE lily_queue.inbox
                       SET state = 1, completed_at = NOW(), updated_at = NOW()
                       WHERE handler_identity = $1 AND event_id = $2
                         AND state = 0 AND lock_token = $3"#,
                )
                .bind::<diesel::sql_types::Text, _>(logical_handler)
                .bind::<diesel::sql_types::Uuid, _>(event_id)
                .bind::<diesel::sql_types::Uuid, _>(lock_token)
                .execute(connection)
                .await
                .map_err(Into::into)
            })
        })
        .await?;
    if affected == 1 {
        Ok(())
    } else {
        Err(lily_postgresql::PgError::TransactionFinalization)
    }
}

fn map_claimed_outbox(
    row: ClaimedOutboxRow,
) -> Result<ClaimedOutboxMessage, PostgresReliabilityError> {
    let schema_version = u16::try_from(row.schema_version).map_err(|_| {
        PostgresReliabilityError::InvalidContract {
            code: "QUEUE_OUTBOX_STORED_SCHEMA_INVALID",
        }
    })?;
    let publish_attempts = u64::try_from(row.publish_attempts).map_err(|_| {
        PostgresReliabilityError::InvalidContract {
            code: "QUEUE_OUTBOX_STORED_ATTEMPT_INVALID",
        }
    })?;
    Ok(ClaimedOutboxMessage {
        record_id: row.record_id,
        event_id: row.event_id,
        exchange: Arc::from(row.exchange_name),
        routing_key: Arc::from(row.routing_key),
        schema_version,
        content_kind: Arc::from(row.content_kind),
        content_type: Arc::from(row.content_type),
        body: Bytes::from(row.body),
        traceparent: row.traceparent.map(Arc::from),
        claim_token: row.claim_token,
        publish_attempts,
    })
}

fn reliability_as_handler(error: PostgresReliabilityError) -> QueueHandlerError {
    error.into()
}

fn failure_code_is_valid(code: &str) -> bool {
    let bytes = code.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 64
        && bytes[0].is_ascii_alphanumeric()
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

fn validate_policy(config: &TransactionalInboxConfig) -> Result<(), PostgresReliabilityError> {
    config
        .validate_contract()
        .map_err(|_| PostgresReliabilityError::InvalidContract {
            code: "QUEUE_POSTGRES_POLICY_INVALID",
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failure_codes_are_bounded_and_canonical() {
        assert!(failure_code_is_valid("BROKER_UNAVAILABLE"));
        assert!(!failure_code_is_valid(""));
        assert!(!failure_code_is_valid("contains secret"));
        assert!(!failure_code_is_valid(&"X".repeat(65)));
    }

    #[test]
    fn transaction_tracker_closes_admission_and_drains_exactly() {
        let tracker = Arc::new(TransactionTracker::default());
        tracker.open();
        let lease = tracker.begin().unwrap();
        assert_eq!(tracker.active.load(Ordering::Acquire), 1);
        tracker.accepting.store(false, Ordering::Release);
        assert!(tracker.begin().is_err());
        drop(lease);
        assert_eq!(tracker.active.load(Ordering::Acquire), 0);
    }

    #[test]
    fn policy_rejects_unbounded_or_relationally_invalid_values() {
        let defaults = TransactionalInboxConfig::default();
        assert!(validate_policy(&defaults).is_ok());
        assert!(
            validate_policy(&TransactionalInboxConfig {
                relay_batch_size: 0,
                ..defaults.clone()
            })
            .is_err()
        );
        assert!(
            validate_policy(&TransactionalInboxConfig {
                outbox_claim_lease_millis: defaults.relay_publish_timeout_millis - 1,
                ..defaults.clone()
            })
            .is_err()
        );
        assert!(
            validate_policy(&TransactionalInboxConfig {
                relay_poll_interval_millis: 0,
                ..defaults.clone()
            })
            .is_err()
        );
        assert!(
            validate_policy(&TransactionalInboxConfig {
                relay_retry_max_backoff_millis: defaults.relay_retry_initial_backoff_millis - 1,
                ..defaults.clone()
            })
            .is_err()
        );
        assert!(
            validate_policy(&TransactionalInboxConfig {
                cleanup_interval_secs: defaults.outbox_retention_secs + 1,
                ..defaults.clone()
            })
            .is_err()
        );
        assert!(
            validate_policy(&TransactionalInboxConfig {
                database_cell: Some(" invalid".to_owned()),
                ..defaults
            })
            .is_err()
        );
    }

    #[test]
    fn inbox_advisory_key_is_stable_and_covers_the_exact_identity() {
        let event = Uuid::parse_str("00112233-4455-6677-8899-aabbccddeeff").unwrap();
        let key = inbox_advisory_lock_key("orders.created", event);
        assert_eq!(
            key,
            i64::from_be_bytes([0x81, 0x39, 0x93, 0x93, 0xb6, 0x13, 0x8e, 0xe9])
        );
        assert_eq!(key, inbox_advisory_lock_key("orders.created", event));
        assert_ne!(key, inbox_advisory_lock_key("orders.updated", event));
        assert_ne!(
            key,
            inbox_advisory_lock_key("orders.created", Uuid::new_v4())
        );
    }
}
