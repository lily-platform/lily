use std::{fmt, sync::Arc};

/// Maximum encoded bytes accepted for one custom Lily content-kind token.
pub const MAX_PUBLISH_CONTENT_KIND_BYTES: usize = 64;

/// Maximum encoded bytes accepted for one AMQP content-type value.
///
/// AMQP 0-9-1 represents `content_type` as a short string, whose encoded
/// payload cannot exceed 255 bytes.
pub const MAX_PUBLISH_CONTENT_TYPE_BYTES: usize = 255;

/// A validation failure in caller-owned publish metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishContractError {
    /// Schema version zero was supplied.
    InvalidSchemaVersion,
    /// The all-zero UUID was supplied as an event identity.
    NilEventId,
    /// A custom content-kind token was empty, oversized, or non-canonical.
    InvalidContentKind,
    /// A built-in content-kind name was supplied through the custom escape hatch.
    ReservedContentKind,
    /// The supplied AMQP content type was empty, oversized, or not a valid MIME value.
    InvalidContentType,
}

impl PublishContractError {
    /// Returns a stable, secret-safe error code.
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidSchemaVersion => "QUEUE_PUBLISH_SCHEMA_VERSION_INVALID",
            Self::NilEventId => "QUEUE_PUBLISH_EVENT_ID_INVALID",
            Self::InvalidContentKind => "QUEUE_PUBLISH_CONTENT_KIND_INVALID",
            Self::ReservedContentKind => "QUEUE_PUBLISH_CONTENT_KIND_RESERVED",
            Self::InvalidContentType => "QUEUE_PUBLISH_CONTENT_TYPE_INVALID",
        }
    }
}

impl fmt::Display for PublishContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidSchemaVersion => "schema version must be a positive u16",
            Self::NilEventId => "event ID must not be the nil UUID",
            Self::InvalidContentKind => "custom content kind is not a canonical bounded token",
            Self::ReservedContentKind => {
                "built-in content kinds must use their typed publisher method"
            }
            Self::InvalidContentType => "content type is not a valid bounded MIME value",
        })
    }
}

impl std::error::Error for PublishContractError {}

/// Positive schema version recorded in Lily's RabbitMQ envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PublishSchemaVersion(u16);

impl PublishSchemaVersion {
    /// Canonical schema version used by the compatibility publisher methods.
    pub const V1: Self = Self(1);

    /// Creates a non-zero schema version.
    pub const fn try_new(value: u16) -> Result<Self, PublishContractError> {
        if value == 0 {
            Err(PublishContractError::InvalidSchemaVersion)
        } else {
            Ok(Self(value))
        }
    }

    /// Returns the underlying positive integer.
    pub const fn get(self) -> u16 {
        self.0
    }
}

impl TryFrom<u16> for PublishSchemaVersion {
    type Error = PublishContractError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}

impl From<PublishSchemaVersion> for u16 {
    fn from(value: PublishSchemaVersion) -> Self {
        value.get()
    }
}

impl fmt::Display for PublishSchemaVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Stable event identity and schema version owned by a publisher or outbox.
///
/// Fresh application publishes use [`Self::fresh`]. Transactional outbox
/// relays persist this metadata and reuse it for every handoff attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PublishMetadata {
    event_id: uuid::Uuid,
    schema_version: PublishSchemaVersion,
}

impl PublishMetadata {
    /// Validates caller-owned event identity and schema version.
    pub fn try_new(
        event_id: uuid::Uuid,
        schema_version: PublishSchemaVersion,
    ) -> Result<Self, PublishContractError> {
        if event_id.is_nil() {
            return Err(PublishContractError::NilEventId);
        }
        Ok(Self {
            event_id,
            schema_version,
        })
    }

    /// Creates metadata with a fresh random event ID.
    pub fn fresh(schema_version: PublishSchemaVersion) -> Self {
        Self {
            event_id: uuid::Uuid::new_v4(),
            schema_version,
        }
    }

    /// Returns the stable event identity.
    pub const fn event_id(self) -> uuid::Uuid {
        self.event_id
    }

    /// Returns the payload schema version.
    pub const fn schema_version(self) -> PublishSchemaVersion {
        self.schema_version
    }
}

/// Validated custom content-kind token paired with its AMQP MIME type.
///
/// Built-in `json`, `text`, and `binary` tokens are deliberately rejected;
/// their typed publisher methods own the only valid MIME mapping.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CustomPublishContent {
    kind: Arc<str>,
    content_type: Arc<str>,
}

impl CustomPublishContent {
    /// Validates and canonicalizes a custom content token and MIME type.
    pub fn try_new(
        kind: impl AsRef<str>,
        content_type: impl AsRef<str>,
    ) -> Result<Self, PublishContractError> {
        let kind = kind.as_ref();
        if !content_kind_is_valid(kind) {
            return Err(PublishContractError::InvalidContentKind);
        }
        if matches!(kind, "json" | "text" | "binary") {
            return Err(PublishContractError::ReservedContentKind);
        }

        let content_type = content_type.as_ref();
        if content_type.is_empty()
            || content_type.len() > MAX_PUBLISH_CONTENT_TYPE_BYTES
            || content_type.trim() != content_type
            || content_type.chars().any(char::is_control)
        {
            return Err(PublishContractError::InvalidContentType);
        }
        let parsed = content_type
            .parse::<mime::Mime>()
            .map_err(|_| PublishContractError::InvalidContentType)?;
        let content_type = parsed.to_string();
        if content_type.len() > MAX_PUBLISH_CONTENT_TYPE_BYTES {
            return Err(PublishContractError::InvalidContentType);
        }

        Ok(Self {
            kind: Arc::from(kind),
            content_type: Arc::from(content_type),
        })
    }

    /// Returns the canonical `x-lily-content-kind` token.
    pub fn kind(&self) -> &str {
        &self.kind
    }

    /// Returns the validated AMQP content type.
    pub fn content_type(&self) -> &str {
        &self.content_type
    }
}

fn content_kind_is_valid(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_PUBLISH_CONTENT_KIND_BYTES
        && value.bytes().enumerate().all(|(index, byte)| {
            if index == 0 {
                byte.is_ascii_lowercase() || byte.is_ascii_digit()
            } else {
                byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || matches!(byte, b'.' | b'_' | b'+' | b'-')
            }
        })
}

/// Content representation recorded in Lily's versioned RabbitMQ envelope.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PublishContentKind {
    /// UTF-8 JSON payload with `application/json` content type.
    Json,
    /// UTF-8 text payload with `text/plain; charset=utf-8` content type.
    Text,
    /// Opaque bytes with `application/octet-stream` content type.
    Binary,
    /// Application-owned codec with a validated token and MIME type.
    Custom(CustomPublishContent),
}

impl PublishContentKind {
    /// Returns the stable `x-lily-content-kind` header value.
    pub fn header_value(&self) -> &str {
        match self {
            Self::Json => "json",
            Self::Text => "text",
            Self::Binary => "binary",
            Self::Custom(custom) => custom.kind(),
        }
    }

    /// Returns the AMQP content type corresponding to this representation.
    pub fn content_type(&self) -> &str {
        match self {
            Self::Json => "application/json",
            Self::Text => "text/plain; charset=utf-8",
            Self::Binary => "application/octet-stream",
            Self::Custom(custom) => custom.content_type(),
        }
    }
}

/// Versioned identity and content metadata attached to a RabbitMQ publish.
///
/// End-user code normally selects one of `QueueClientService`'s
/// typed publish methods. This type remains available for Lily's low-level
/// transport qualification and retry/outbox integration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishEnvelope {
    metadata: PublishMetadata,
    content_kind: PublishContentKind,
}

impl PublishEnvelope {
    /// Creates a V1 envelope with caller-owned event identity.
    ///
    /// The low-level transport validates the identity before broker I/O.
    /// Application outboxes should prefer [`PublishMetadata::try_new`] and the
    /// typed service methods so invalid identity is rejected at construction.
    #[doc(hidden)]
    pub const fn new(event_id: uuid::Uuid, content_kind: PublishContentKind) -> Self {
        Self {
            metadata: PublishMetadata {
                event_id,
                schema_version: PublishSchemaVersion::V1,
            },
            content_kind,
        }
    }

    /// Creates an envelope from validated caller-owned metadata.
    pub fn with_metadata(metadata: PublishMetadata, content_kind: PublishContentKind) -> Self {
        Self {
            metadata,
            content_kind,
        }
    }

    /// Creates a V1 JSON envelope with a fresh random event ID.
    pub fn fresh_json() -> Self {
        Self::with_metadata(
            PublishMetadata::fresh(PublishSchemaVersion::V1),
            PublishContentKind::Json,
        )
    }

    /// Creates a V1 binary envelope with a fresh random event ID.
    pub fn fresh_binary() -> Self {
        Self::with_metadata(
            PublishMetadata::fresh(PublishSchemaVersion::V1),
            PublishContentKind::Binary,
        )
    }

    /// Returns the stable event identity.
    pub const fn event_id(&self) -> uuid::Uuid {
        self.metadata.event_id()
    }

    /// Returns the payload schema version.
    pub const fn schema_version(&self) -> PublishSchemaVersion {
        self.metadata.schema_version()
    }

    /// Returns the payload content representation.
    pub const fn content_kind(&self) -> &PublishContentKind {
        &self.content_kind
    }

    pub(crate) fn validate(&self) -> Result<(), PublishContractError> {
        if self.event_id().is_nil() {
            Err(PublishContractError::NilEventId)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_version_and_event_identity_are_fail_closed() {
        assert_eq!(
            PublishSchemaVersion::try_new(0),
            Err(PublishContractError::InvalidSchemaVersion)
        );
        assert_eq!(
            PublishMetadata::try_new(uuid::Uuid::nil(), PublishSchemaVersion::V1),
            Err(PublishContractError::NilEventId)
        );
        assert_eq!(
            PublishSchemaVersion::try_new(u16::MAX).unwrap().get(),
            u16::MAX
        );
        assert_eq!(
            PublishEnvelope::new(uuid::Uuid::nil(), PublishContentKind::Json).validate(),
            Err(PublishContractError::NilEventId)
        );
    }

    #[test]
    fn caller_can_preserve_event_id_and_version_for_outbox_redelivery() {
        let event_id = uuid::Uuid::new_v4();
        let version = PublishSchemaVersion::try_new(3).unwrap();
        let metadata = PublishMetadata::try_new(event_id, version).unwrap();
        let envelope = PublishEnvelope::with_metadata(metadata, PublishContentKind::Json);

        assert_eq!(envelope.event_id(), event_id);
        assert_eq!(envelope.schema_version(), version);
        assert_eq!(envelope.content_kind(), &PublishContentKind::Json);
    }

    #[test]
    fn custom_content_is_bounded_canonical_and_cannot_shadow_builtins() {
        assert!(CustomPublishContent::try_new("protobuf", "application/protobuf").is_ok());
        assert_eq!(
            CustomPublishContent::try_new("json", "application/custom").unwrap_err(),
            PublishContractError::ReservedContentKind
        );
        for invalid in ["", "UPPER", " leading", "two words", ".prefix"] {
            assert_eq!(
                CustomPublishContent::try_new(invalid, "application/custom").unwrap_err(),
                PublishContractError::InvalidContentKind
            );
        }
        assert_eq!(
            CustomPublishContent::try_new(
                "x".repeat(MAX_PUBLISH_CONTENT_KIND_BYTES + 1),
                "application/custom"
            )
            .unwrap_err(),
            PublishContractError::InvalidContentKind
        );
        assert!(CustomPublishContent::try_new(
            "x".repeat(MAX_PUBLISH_CONTENT_KIND_BYTES),
            "application/custom"
        )
        .is_ok());
    }

    #[test]
    fn custom_mime_is_validated_and_canonicalized() {
        let custom = CustomPublishContent::try_new(
            "protobuf",
            "application/vnd.example.order+protobuf; charset=utf-8",
        )
        .unwrap();
        assert_eq!(custom.kind(), "protobuf");
        assert_eq!(
            custom.content_type(),
            "application/vnd.example.order+protobuf; charset=utf-8"
        );

        for invalid in ["", "not a mime", " application/json", "application/json\n"] {
            assert_eq!(
                CustomPublishContent::try_new("custom", invalid).unwrap_err(),
                PublishContractError::InvalidContentType
            );
        }

        let exact_maximum = format!(
            "application/{}",
            "x".repeat(MAX_PUBLISH_CONTENT_TYPE_BYTES - "application/".len())
        );
        assert_eq!(exact_maximum.len(), MAX_PUBLISH_CONTENT_TYPE_BYTES);
        assert!(CustomPublishContent::try_new("custom", &exact_maximum).is_ok());

        let over_maximum = format!("{exact_maximum}x");
        assert_eq!(
            CustomPublishContent::try_new("custom", over_maximum).unwrap_err(),
            PublishContractError::InvalidContentType
        );
    }

    #[test]
    fn built_in_content_kinds_have_fixed_mime_mappings() {
        assert_eq!(PublishContentKind::Json.header_value(), "json");
        assert_eq!(PublishContentKind::Json.content_type(), "application/json");
        assert_eq!(PublishContentKind::Text.header_value(), "text");
        assert_eq!(
            PublishContentKind::Text.content_type(),
            "text/plain; charset=utf-8"
        );
        assert_eq!(PublishContentKind::Binary.header_value(), "binary");
        assert_eq!(
            PublishContentKind::Binary.content_type(),
            "application/octet-stream"
        );
    }
}
