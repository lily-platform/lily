//! Backend-neutral transactional inbox/outbox contracts.

use std::{fmt, sync::Arc};

use bytes::Bytes;
use lily_queue_client::PublishContentKind;
use uuid::Uuid;

/// Sampling-independent execution evidence for transactional inbox owners.
///
/// The snapshot is deliberately payload-free and contains no event, handler,
/// database-cell or credential identity. Counters saturate at `u64::MAX` and
/// are therefore safe to retain for the complete process lifetime.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Serialize)]
pub struct TransactionalInboxSnapshot {
    /// Transaction owners which have been admitted but not yet finalized.
    pub active_transactions: u64,
    /// Complete MongoDB transaction bodies entered, including safe replays.
    pub body_attempts_total: u64,
    /// Complete transaction-body replays requested by MongoDB.
    pub transient_body_retries_total: u64,
    /// MongoDB commit commands attempted, including result reconciliation.
    pub commit_attempts_total: u64,
    /// Commit commands replayed after an unknown commit result.
    pub unknown_commit_retries_total: u64,
    /// Deliveries which exhausted the bounded whole-transaction retry policy.
    pub transaction_retry_exhausted_total: u64,
    /// Commits whose result remained unknown at the absolute delivery deadline.
    pub commit_outcome_unknown_total: u64,
    /// Deliveries which observed an external owner for the same inbox identity.
    pub lease_contentions_total: u64,
    /// Committed results whose best-effort external lease release failed.
    ///
    /// This is advisory historical evidence: the committed delivery result is
    /// never rewritten into an application failure by lease cleanup.
    pub post_commit_release_failures_total: u64,
    /// Most recent stable, secret-safe transaction failure code.
    pub last_failure_code: Option<&'static str>,
}

impl TransactionalInboxSnapshot {
    pub(crate) fn saturating_add_assign(&mut self, other: Self) {
        self.active_transactions = self
            .active_transactions
            .saturating_add(other.active_transactions);
        self.body_attempts_total = self
            .body_attempts_total
            .saturating_add(other.body_attempts_total);
        self.transient_body_retries_total = self
            .transient_body_retries_total
            .saturating_add(other.transient_body_retries_total);
        self.commit_attempts_total = self
            .commit_attempts_total
            .saturating_add(other.commit_attempts_total);
        self.unknown_commit_retries_total = self
            .unknown_commit_retries_total
            .saturating_add(other.unknown_commit_retries_total);
        self.transaction_retry_exhausted_total = self
            .transaction_retry_exhausted_total
            .saturating_add(other.transaction_retry_exhausted_total);
        self.commit_outcome_unknown_total = self
            .commit_outcome_unknown_total
            .saturating_add(other.commit_outcome_unknown_total);
        self.lease_contentions_total = self
            .lease_contentions_total
            .saturating_add(other.lease_contentions_total);
        self.post_commit_release_failures_total = self
            .post_commit_release_failures_total
            .saturating_add(other.post_commit_release_failures_total);
        if other.last_failure_code.is_some() {
            self.last_failure_code = other.last_failure_code;
        }
    }
}

/// One prepared storage authority bound to a physical transactional queue.
///
/// This is hidden cross-crate framework ABI used by `lily_consumer`. A queue
/// owns exactly one variant; handler compilation only clones the same `Arc`
/// into every version/content entry selected for that physical queue.
#[doc(hidden)]
#[derive(Clone)]
pub enum PreparedTransactionalRuntime {
    /// PostgreSQL-backed inbox/outbox authority.
    #[cfg(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory"
    ))]
    PostgreSql(Arc<crate::transactional_postgresql::PostgresTransactionalRuntime>),
    /// MongoDB-backed inbox/outbox authority.
    #[cfg(any(
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-mongodb-factory"
    ))]
    MongoDb(Arc<crate::transactional_mongodb::MongoTransactionalRuntime>),
}

/// Maximum encoded bytes accepted for an exchange or routing identity.
pub const MAX_OUTBOX_TRANSPORT_IDENTITY_BYTES: usize = 200;
/// Absolute per-record payload ceiling enforced before database work.
pub const MAX_OUTBOX_MESSAGE_BYTES: usize = 16 * 1024 * 1024;
/// Maximum stable handler identity retained by an inbox adapter.
pub const MAX_TRANSACTIONAL_HANDLER_IDENTITY_BYTES: usize = 512;

/// A secret-safe violation of the durable outbox message contract.
///
/// This error is independent of the database selected by a transactional
/// queue binding. Storage adapters convert it into their own typed error only
/// when an operation enters that backend.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct TransactionalOutboxContractError {
    code: &'static str,
}

impl TransactionalOutboxContractError {
    pub(crate) const fn new(code: &'static str) -> Self {
        Self { code }
    }

    /// Returns the bounded stable failure code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        self.code
    }
}

impl fmt::Display for TransactionalOutboxContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code)
    }
}

impl std::error::Error for TransactionalOutboxContractError {}

impl From<TransactionalOutboxContractError> for lily_error::application::QueueHandlerError {
    fn from(error: TransactionalOutboxContractError) -> Self {
        Self::permanent_with_source(error.code(), error)
    }
}

/// Result of one storage-backed transactional delivery attempt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransactionalExecution<T> {
    /// This delivery acquired the inbox claim and committed application work.
    Applied(T),
    /// This exact logical handler already completed this event.
    AlreadyCompleted,
    /// Another transaction currently owns this logical handler/event pair.
    ///
    /// This is always retryable and never classifies the event as completed.
    InProgress,
}

/// Counts deleted by one bounded transactional retention cleanup pass.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CleanupReport {
    /// Completed inbox rows or documents removed.
    pub inbox_rows: u64,
    /// Delivered outbox rows or documents removed.
    pub outbox_rows: u64,
}

/// Durable application event inserted through the active business transaction.
///
/// The event ID is supplied by the application and remains identical across
/// every relay attempt. Construction validates all retained strings and the
/// absolute payload ceiling before any database operation begins.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransactionalOutboxMessage {
    pub(crate) event_id: Uuid,
    pub(crate) exchange: Arc<str>,
    pub(crate) routing_key: Arc<str>,
    pub(crate) schema_version: u16,
    pub(crate) content_kind: Arc<str>,
    pub(crate) content_type: Arc<str>,
    pub(crate) body: Bytes,
    pub(crate) traceparent: Option<Arc<str>>,
}

impl TransactionalOutboxMessage {
    /// Builds a validated durable event with an optional captured W3C parent.
    pub fn try_new(
        event_id: Uuid,
        exchange: impl AsRef<str>,
        routing_key: impl AsRef<str>,
        schema_version: u16,
        content: PublishContentKind,
        body: impl Into<Bytes>,
    ) -> Result<Self, TransactionalOutboxContractError> {
        if event_id.is_nil() {
            return Err(invalid("QUEUE_OUTBOX_EVENT_ID_INVALID"));
        }
        let exchange = canonical_identity(
            exchange.as_ref(),
            MAX_OUTBOX_TRANSPORT_IDENTITY_BYTES,
            "QUEUE_OUTBOX_EXCHANGE_INVALID",
        )?;
        let routing_key = canonical_identity(
            routing_key.as_ref(),
            MAX_OUTBOX_TRANSPORT_IDENTITY_BYTES,
            "QUEUE_OUTBOX_ROUTING_KEY_INVALID",
        )?;
        if schema_version == 0 {
            return Err(invalid("QUEUE_OUTBOX_SCHEMA_VERSION_INVALID"));
        }
        // PublishContentKind owns the canonical built-in MIME mapping and its
        // custom variant can only be built through CustomPublishContent's
        // strict token/MIME parser.
        let content_kind: Arc<str> = Arc::from(content.header_value());
        let content_type: Arc<str> = Arc::from(content.content_type());
        let body = body.into();
        if body.len() > MAX_OUTBOX_MESSAGE_BYTES {
            return Err(invalid("QUEUE_OUTBOX_BODY_TOO_LARGE"));
        }
        let traceparent = lily_trace::W3CTraceContext::from_current_span()
            .map(|context| Arc::<str>::from(context.to_traceparent()));

        Ok(Self {
            event_id,
            exchange,
            routing_key,
            schema_version,
            content_kind,
            content_type,
            body,
            traceparent,
        })
    }

    /// Stable event identity reused by every relay attempt.
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

    /// Bounded AMQP content type.
    #[must_use]
    pub fn content_type(&self) -> &str {
        &self.content_type
    }

    /// Serialized durable payload.
    #[must_use]
    pub fn body(&self) -> &[u8] {
        &self.body
    }
}

pub(crate) fn handler_identity_is_valid(value: &str) -> bool {
    (1..=MAX_TRANSACTIONAL_HANDLER_IDENTITY_BYTES).contains(&value.len())
        && value.trim() == value
        && !value.chars().any(char::is_control)
}

const fn invalid(code: &'static str) -> TransactionalOutboxContractError {
    TransactionalOutboxContractError::new(code)
}

fn canonical_identity(
    value: &str,
    maximum: usize,
    code: &'static str,
) -> Result<Arc<str>, TransactionalOutboxContractError> {
    ((1..=maximum).contains(&value.len())
        && value.trim() == value
        && !value.chars().any(char::is_control))
    .then(|| Arc::from(value))
    .ok_or_else(|| invalid(code))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_message(
        body: Vec<u8>,
    ) -> Result<TransactionalOutboxMessage, TransactionalOutboxContractError> {
        TransactionalOutboxMessage::try_new(
            Uuid::new_v4(),
            "events",
            "orders.created",
            1,
            PublishContentKind::Json,
            body,
        )
    }

    #[test]
    fn outbox_contract_accepts_exact_bounds_and_rejects_plus_one() {
        assert!(
            TransactionalOutboxMessage::try_new(
                Uuid::new_v4(),
                "e".repeat(MAX_OUTBOX_TRANSPORT_IDENTITY_BYTES),
                "r".repeat(MAX_OUTBOX_TRANSPORT_IDENTITY_BYTES),
                u16::MAX,
                PublishContentKind::Binary,
                vec![0; MAX_OUTBOX_MESSAGE_BYTES],
            )
            .is_ok()
        );
        assert!(
            TransactionalOutboxMessage::try_new(
                Uuid::new_v4(),
                "e".repeat(MAX_OUTBOX_TRANSPORT_IDENTITY_BYTES + 1),
                "routing",
                1,
                PublishContentKind::Json,
                Vec::new(),
            )
            .is_err()
        );
        assert_eq!(
            valid_message(vec![0; MAX_OUTBOX_MESSAGE_BYTES + 1])
                .unwrap_err()
                .code(),
            "QUEUE_OUTBOX_BODY_TOO_LARGE"
        );
    }

    #[test]
    fn nil_zero_and_noncanonical_values_fail_closed() {
        let result = TransactionalOutboxMessage::try_new(
            Uuid::nil(),
            "events",
            "orders",
            1,
            PublishContentKind::Json,
            Vec::new(),
        );
        assert_eq!(result.unwrap_err().code(), "QUEUE_OUTBOX_EVENT_ID_INVALID");
        let result = TransactionalOutboxMessage::try_new(
            Uuid::new_v4(),
            "events",
            "orders",
            0,
            PublishContentKind::Json,
            Vec::new(),
        );
        assert_eq!(
            result.unwrap_err().code(),
            "QUEUE_OUTBOX_SCHEMA_VERSION_INVALID"
        );
    }
}
