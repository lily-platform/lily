use std::{collections::BTreeMap, fmt, sync::Arc, time::Duration};

use bytes::Bytes;
use lily_error::application::QueueHandlerError;
#[cfg(test)]
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// Maximum bytes retained for one canonical queue content-kind token.
pub const MAX_QUEUE_CONTENT_KIND_BYTES: usize = 64;

/// Maximum bytes retained for a queue, exchange or routing-key identity.
pub(crate) const MAX_QUEUE_TRANSPORT_IDENTITY_BYTES: usize = 200;

/// Validates an identity before it is retained by config or delivery state.
pub(crate) fn queue_transport_identity_is_valid(value: &str) -> bool {
    (1..=MAX_QUEUE_TRANSPORT_IDENTITY_BYTES).contains(&value.len())
        && value.trim() == value
        && !value.chars().any(char::is_control)
}

/// Stable event identifier carried by Lily's queue envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EventId(pub Uuid);

impl EventId {
    /// Returns the validated UUID.
    #[must_use]
    pub const fn into_inner(self) -> Uuid {
        self.0
    }
}

/// Positive schema version carried by Lily's queue envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SchemaVersion(u16);

impl SchemaVersion {
    /// Creates a validated, non-zero schema version.
    ///
    /// Zero is not a valid Lily queue schema version and is rejected with a
    /// permanent typed failure before handler execution.
    pub fn try_new(value: u16) -> Result<Self, QueueHandlerError> {
        (value != 0)
            .then_some(Self(value))
            .ok_or_else(|| QueueHandlerError::permanent("QUEUE_SCHEMA_VERSION_INVALID"))
    }

    /// Returns the numeric schema version.
    #[must_use]
    pub const fn into_inner(self) -> u16 {
        self.0
    }
}

/// Exact, bounded payload content-kind token.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ContentKind(Arc<str>);

impl ContentKind {
    /// Validates a content-kind obtained from the transport envelope.
    pub(crate) fn try_new(value: impl Into<String>) -> Result<Self, QueueHandlerError> {
        let value = value.into();
        let valid = !value.is_empty()
            && value.len() <= MAX_QUEUE_CONTENT_KIND_BYTES
            && value.bytes().enumerate().all(|(index, byte)| {
                if index == 0 {
                    byte.is_ascii_lowercase() || byte.is_ascii_digit()
                } else {
                    byte.is_ascii_lowercase()
                        || byte.is_ascii_digit()
                        || matches!(byte, b'.' | b'_' | b'+' | b'-')
                }
            });
        valid
            .then(|| Self(Arc::from(value)))
            .ok_or_else(|| QueueHandlerError::permanent("QUEUE_CONTENT_KIND_INVALID"))
    }

    /// Returns the validated token.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for ContentKind {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

/// Number of Lily retry handoffs preceding the current delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RetryCount(pub u32);

impl RetryCount {
    /// Returns the handoff count.
    #[must_use]
    pub const fn into_inner(self) -> u32 {
        self.0
    }
}

/// Whether RabbitMQ marked the current delivery as redelivered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Redelivered(pub bool);

impl Redelivered {
    /// Returns the broker flag.
    #[must_use]
    pub const fn into_inner(self) -> bool {
        self.0
    }
}

/// Immutable metadata for one admitted queue delivery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryContext {
    pub(crate) event_id: EventId,
    pub(crate) schema_version: SchemaVersion,
    pub(crate) content_kind: ContentKind,
    pub(crate) retry_count: RetryCount,
    pub(crate) redelivered: Redelivered,
    pub(crate) queue: Arc<str>,
    pub(crate) exchange: Arc<str>,
    pub(crate) routing_key: Arc<str>,
}

impl DeliveryContext {
    /// Stable event identity from the canonical envelope.
    #[must_use]
    pub const fn event_id(&self) -> EventId {
        self.event_id
    }

    /// Canonical envelope schema version.
    #[must_use]
    pub const fn schema_version(&self) -> SchemaVersion {
        self.schema_version
    }

    /// Exact canonical envelope content kind.
    #[must_use]
    pub const fn content_kind(&self) -> &ContentKind {
        &self.content_kind
    }

    /// Lily retry handoff count.
    #[must_use]
    pub const fn retry_count(&self) -> RetryCount {
        self.retry_count
    }

    /// RabbitMQ redelivery flag.
    #[must_use]
    pub const fn redelivered(&self) -> Redelivered {
        self.redelivered
    }

    /// Physical RabbitMQ queue name.
    #[must_use]
    pub fn queue(&self) -> &str {
        &self.queue
    }

    /// Exchange that routed the delivery.
    #[must_use]
    pub fn exchange(&self) -> &str {
        &self.exchange
    }

    /// Exact routing key used by the broker delivery.
    #[must_use]
    pub fn routing_key(&self) -> &str {
        &self.routing_key
    }
}

/// Bounded, provider-neutral value retained from one AMQP application header.
#[derive(Clone, PartialEq)]
#[non_exhaustive]
pub enum DeliveryHeaderValue {
    /// Boolean scalar.
    Boolean(bool),
    /// Signed integral scalar normalized to 64 bits.
    Signed(i64),
    /// Unsigned integral scalar normalized to 64 bits.
    Unsigned(u64),
    /// Floating-point scalar normalized to 64 bits.
    Float(f64),
    /// AMQP decimal scalar.
    Decimal {
        /// Decimal scale.
        scale: u8,
        /// Unscaled decimal value.
        value: u32,
    },
    /// Valid UTF-8 application text.
    Text(Arc<str>),
    /// Bounded opaque application bytes.
    Binary(Bytes),
    /// AMQP timestamp.
    Timestamp(u64),
    /// Bounded nested array.
    Array(Arc<[DeliveryHeaderValue]>),
    /// Bounded nested table.
    Table(Arc<BTreeMap<String, DeliveryHeaderValue>>),
    /// Explicit AMQP void.
    Void,
}

impl DeliveryHeaderValue {
    fn kind(&self) -> &'static str {
        match self {
            Self::Boolean(_) => "boolean",
            Self::Signed(_) => "signed",
            Self::Unsigned(_) => "unsigned",
            Self::Float(_) => "float",
            Self::Decimal { .. } => "decimal",
            Self::Text(_) => "text",
            Self::Binary(_) => "binary",
            Self::Timestamp(_) => "timestamp",
            Self::Array(_) => "array",
            Self::Table(_) => "table",
            Self::Void => "void",
        }
    }
}

impl fmt::Debug for DeliveryHeaderValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeliveryHeaderValue")
            .field("kind", &self.kind())
            .finish_non_exhaustive()
    }
}

/// Cheaply cloned, immutable view of bounded AMQP application headers.
#[derive(Clone, Default, PartialEq)]
pub struct DeliveryHeaders(Arc<BTreeMap<String, DeliveryHeaderValue>>);

impl DeliveryHeaders {
    pub(crate) fn from_entries(entries: BTreeMap<String, DeliveryHeaderValue>) -> Self {
        Self(Arc::new(entries))
    }

    /// Returns one exact case-sensitive header value.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&DeliveryHeaderValue> {
        self.0.get(name)
    }

    /// Iterates over header names and immutable values in lexical order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&str, &DeliveryHeaderValue)> {
        self.0.iter().map(|(name, value)| (name.as_str(), value))
    }

    /// Number of top-level application headers.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` when no application header was retained.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for DeliveryHeaders {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeliveryHeaders")
            .field("entry_count", &self.len())
            .finish()
    }
}

/// Immutable allowlisted AMQP basic properties for a raw delivery extractor.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct DeliveryProperties {
    pub(crate) content_type: Option<Arc<str>>,
    pub(crate) content_encoding: Option<Arc<str>>,
    pub(crate) correlation_id: Option<Arc<str>>,
    pub(crate) message_id: Option<Arc<str>>,
    pub(crate) message_type: Option<Arc<str>>,
    pub(crate) reply_to: Option<Arc<str>>,
    pub(crate) app_id: Option<Arc<str>>,
    pub(crate) user_id: Option<Arc<str>>,
    pub(crate) expiration: Option<Arc<str>>,
    pub(crate) delivery_mode: Option<u8>,
    pub(crate) priority: Option<u8>,
    pub(crate) timestamp: Option<u64>,
}

macro_rules! optional_property {
    ($name:ident) => {
        #[doc = concat!("Returns the bounded `", stringify!($name), "` property.")]
        #[must_use]
        pub fn $name(&self) -> Option<&str> {
            self.$name.as_deref()
        }
    };
}

impl DeliveryProperties {
    optional_property!(content_type);
    optional_property!(content_encoding);
    optional_property!(correlation_id);
    optional_property!(message_id);
    optional_property!(message_type);
    optional_property!(reply_to);
    optional_property!(app_id);
    optional_property!(user_id);
    optional_property!(expiration);

    /// Returns the AMQP delivery-mode property.
    #[must_use]
    pub const fn delivery_mode(&self) -> Option<u8> {
        self.delivery_mode
    }

    /// Returns the AMQP priority property.
    #[must_use]
    pub const fn priority(&self) -> Option<u8> {
        self.priority
    }

    /// Returns the AMQP timestamp property.
    #[must_use]
    pub const fn timestamp(&self) -> Option<u64> {
        self.timestamp
    }
}

impl fmt::Debug for DeliveryProperties {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text_property_count = [
            self.content_type.as_ref(),
            self.content_encoding.as_ref(),
            self.correlation_id.as_ref(),
            self.message_id.as_ref(),
            self.message_type.as_ref(),
            self.reply_to.as_ref(),
            self.app_id.as_ref(),
            self.user_id.as_ref(),
            self.expiration.as_ref(),
        ]
        .into_iter()
        .flatten()
        .count();
        formatter
            .debug_struct("DeliveryProperties")
            .field("text_property_count", &text_property_count)
            .field("has_delivery_mode", &self.delivery_mode.is_some())
            .field("has_priority", &self.priority.is_some())
            .field("has_timestamp", &self.timestamp.is_some())
            .finish()
    }
}

/// Read-only cooperative cancellation signal for one admitted delivery.
///
/// The framework owns the underlying cancellation authority. A handler may
/// observe timeout or shutdown through [`Self::cancelled`] or [`Self::is_cancelled`], but
/// it cannot cancel Lily's lifecycle token. Application-owned cancellation is
/// expressed by returning an explicit retryable or permanent
/// [`crate::QueueHandlerError`].
#[derive(Debug, Clone)]
pub struct DeliveryCancellation(pub(crate) crate::cancellation::DeliveryCancellationSource);

impl DeliveryCancellation {
    /// Wait for delivery timeout or framework execution cancellation.
    pub async fn cancelled(&self) {
        self.0.cancelled().await;
    }

    /// Whether delivery timeout or framework shutdown requested cancellation.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.0.is_cancelled()
    }

    /// First recorded cancellation reason. Admission closure alone has no reason.
    ///
    /// A reason proves only that cancellation was requested, not that execution,
    /// cleanup or broker settlement has completed.
    #[must_use]
    pub fn reason(&self) -> Option<crate::DeliveryCancellationReason> {
        self.0.reason()
    }
}

/// Common absolute deadline for the complete normal delivery pipeline.
///
/// The queue's configured delivery timeout is an aggregate execution and
/// cleanup budget. Lily reserves a bounded tail of that budget for mandatory
/// cooperative cancellation, abnormal middleware unwind and per-delivery DI
/// cleanup. Before/guard/extraction/handler/normal-after share this cutoff;
/// cancellation notifies execution here and allows bounded continued polling.
/// A pipeline which completes in that window retains its real result. This
/// deadline is not a new budget for each callback or a settlement guarantee.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeliveryDeadline(pub tokio::time::Instant);

impl DeliveryDeadline {
    /// Returns the absolute Tokio instant.
    #[must_use]
    pub const fn instant(self) -> tokio::time::Instant {
        self.0
    }

    /// Returns the remaining budget, saturating at zero.
    #[must_use]
    pub fn remaining(self) -> Duration {
        self.0
            .saturating_duration_since(tokio::time::Instant::now())
    }
}

/// Crate-owned transport input passed from RabbitMQ admission to typed dispatch.
#[derive(Clone)]
pub(crate) struct DeliveryInput {
    pub(crate) body: Bytes,
    pub(crate) context: DeliveryContext,
    pub(crate) headers: DeliveryHeaders,
    pub(crate) properties: DeliveryProperties,
    pub(crate) cancellation: crate::cancellation::DeliveryCancellationSource,
    pub(crate) deadline: tokio::time::Instant,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_kind_uses_the_frozen_bounded_token_grammar() {
        for valid in [
            "json",
            "1json",
            "application.json",
            "vendor+v1",
            "binary_v2",
        ] {
            assert_eq!(ContentKind::try_new(valid).unwrap().as_str(), valid);
        }
        for invalid in ["", "Json", "+json", "has space", "a/b"] {
            assert_eq!(
                ContentKind::try_new(invalid).unwrap_err().code(),
                "QUEUE_CONTENT_KIND_INVALID"
            );
        }
        assert!(ContentKind::try_new("x".repeat(MAX_QUEUE_CONTENT_KIND_BYTES)).is_ok());
        assert!(ContentKind::try_new("x".repeat(MAX_QUEUE_CONTENT_KIND_BYTES + 1)).is_err());
    }

    #[test]
    fn queue_transport_identity_bounds_are_exact_and_control_free() {
        assert!(queue_transport_identity_is_valid("q"));
        assert!(queue_transport_identity_is_valid(
            &"q".repeat(MAX_QUEUE_TRANSPORT_IDENTITY_BYTES)
        ));

        for invalid in [
            "".to_string(),
            " queue".to_string(),
            "queue ".to_string(),
            "queue\nforged".to_string(),
            "q".repeat(MAX_QUEUE_TRANSPORT_IDENTITY_BYTES + 1),
        ] {
            assert!(!queue_transport_identity_is_valid(&invalid), "{invalid:?}");
        }
    }

    #[test]
    fn schema_version_cannot_represent_zero_through_the_public_constructor() {
        assert_eq!(SchemaVersion::try_new(1).unwrap().into_inner(), 1);
        assert_eq!(
            SchemaVersion::try_new(u16::MAX).unwrap().into_inner(),
            u16::MAX
        );
        assert_eq!(
            SchemaVersion::try_new(0).unwrap_err().code(),
            "QUEUE_SCHEMA_VERSION_INVALID"
        );
    }

    #[tokio::test]
    async fn delivery_cancellation_is_a_read_only_framework_signal() {
        let authority = CancellationToken::new();
        let cancellation = DeliveryCancellation(authority.child_token().into());
        assert!(!cancellation.is_cancelled());

        authority.cancel();
        cancellation.cancelled().await;

        assert!(cancellation.is_cancelled());
    }

    #[test]
    fn metadata_debug_output_does_not_expose_remote_values() {
        let secret = "DELIVERY-METADATA-SECRET";
        let mut entries = BTreeMap::new();
        entries.insert(
            "secret".to_string(),
            DeliveryHeaderValue::Text(Arc::from(secret)),
        );
        let headers = DeliveryHeaders::from_entries(entries);
        let properties = DeliveryProperties {
            correlation_id: Some(Arc::from(secret)),
            ..DeliveryProperties::default()
        };

        assert!(!format!("{headers:?}").contains(secret));
        assert!(!format!("{properties:?}").contains(secret));
        assert!(!format!("{:?}", headers.get("secret").unwrap()).contains(secret));
    }

    #[test]
    fn deadline_remaining_saturates_at_zero() {
        let expired = DeliveryDeadline(tokio::time::Instant::now() - Duration::from_secs(1));
        assert_eq!(expired.remaining(), Duration::ZERO);
    }
}
