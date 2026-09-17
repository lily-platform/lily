use std::{fmt, future::Future};

use bytes::Bytes;
use lily_error::application::QueueHandlerError;
use lily_queue_registry::QueuePayloadKind;
use serde::de::DeserializeOwned;

use crate::{ContentKind, DeliveryContext, DeliveryHeaders, DeliveryProperties};

use super::FromDelivery;

/// Mutable single-consumer view of one bounded delivery body.
pub struct DeliveryPayloadInput<'a> {
    body: &'a mut Option<Bytes>,
    context: &'a DeliveryContext,
    headers: &'a DeliveryHeaders,
    properties: &'a DeliveryProperties,
}

impl<'a> DeliveryPayloadInput<'a> {
    pub(super) fn new(
        body: &'a mut Option<Bytes>,
        context: &'a DeliveryContext,
        headers: &'a DeliveryHeaders,
        properties: &'a DeliveryProperties,
    ) -> Self {
        Self {
            body,
            context,
            headers,
            properties,
        }
    }

    /// Takes the bounded body exactly once.
    pub fn take_body(&mut self) -> Result<Bytes, QueueHandlerError> {
        self.body
            .take()
            .ok_or_else(|| QueueHandlerError::permanent("QUEUE_PAYLOAD_ALREADY_CONSUMED"))
    }

    /// Immutable canonical delivery metadata.
    #[must_use]
    pub const fn context(&self) -> &DeliveryContext {
        self.context
    }

    /// Exact content-kind declared by the canonical envelope.
    #[must_use]
    pub fn content_kind(&self) -> &ContentKind {
        self.context.content_kind()
    }

    /// Immutable bounded application headers.
    #[must_use]
    pub const fn headers(&self) -> &DeliveryHeaders {
        self.headers
    }

    /// Immutable bounded allowlisted AMQP properties.
    #[must_use]
    pub const fn properties(&self) -> &DeliveryProperties {
        self.properties
    }
}

impl fmt::Debug for DeliveryPayloadInput<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeliveryPayloadInput")
            .field("body_available", &self.body.is_some())
            .field("body_bytes", &self.body.as_ref().map_or(0, Bytes::len))
            .finish_non_exhaustive()
    }
}

/// JSON payload deserialized into `T` exactly once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Json<T>(pub T);

impl<T> Json<T> {
    /// Returns the deserialized application value.
    #[must_use]
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> FromDelivery for Json<T>
where
    T: DeserializeOwned + Send,
{
    const PAYLOAD_KIND: QueuePayloadKind = QueuePayloadKind::Json;
    type Rejection = QueueHandlerError;

    fn from_delivery(
        mut input: DeliveryPayloadInput<'_>,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        let result = require_content_kind(&input, "json")
            .and_then(|()| input.take_body())
            .and_then(|body| {
                serde_json::from_slice(&body).map_err(|error| {
                    QueueHandlerError::permanent_with_source("QUEUE_JSON_INVALID", error)
                })
            })
            .map(Self);
        std::future::ready(result)
    }
}

/// Owned, codec-validated UTF-8 text payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextPayload(pub String);

impl TextPayload {
    /// Returns the owned text.
    #[must_use]
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl FromDelivery for TextPayload {
    const PAYLOAD_KIND: QueuePayloadKind = QueuePayloadKind::Text;
    type Rejection = QueueHandlerError;

    fn from_delivery(
        mut input: DeliveryPayloadInput<'_>,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        let result = require_content_kind(&input, "text")
            .and_then(|()| input.take_body())
            .and_then(|body| {
                String::from_utf8(body.to_vec()).map_err(|error| {
                    QueueHandlerError::permanent_with_source("QUEUE_TEXT_INVALID", error)
                })
            })
            .map(Self);
        std::future::ready(result)
    }
}

/// Owned bounded binary payload.
#[derive(Clone, PartialEq, Eq)]
pub struct BinaryPayload(pub Bytes);

impl BinaryPayload {
    /// Borrows the exact application bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_ref()
    }

    /// Transfers the bounded bytes without another copy.
    #[must_use]
    pub fn into_inner(self) -> Bytes {
        self.0
    }
}

impl fmt::Debug for BinaryPayload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BinaryPayload")
            .field("byte_length", &self.0.len())
            .finish()
    }
}

impl FromDelivery for BinaryPayload {
    const PAYLOAD_KIND: QueuePayloadKind = QueuePayloadKind::Binary;
    type Rejection = QueueHandlerError;

    fn from_delivery(
        mut input: DeliveryPayloadInput<'_>,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        let result = require_content_kind(&input, "binary")
            .and_then(|()| input.take_body())
            .map(Self);
        std::future::ready(result)
    }
}

/// Bounded read-only raw delivery without broker settlement authority.
#[derive(Clone, PartialEq)]
pub struct RawDelivery {
    body: Bytes,
    context: DeliveryContext,
    headers: DeliveryHeaders,
    properties: DeliveryProperties,
}

impl RawDelivery {
    /// Borrows the exact bounded body.
    #[must_use]
    pub fn body(&self) -> &[u8] {
        self.body.as_ref()
    }

    /// Transfers the exact body without another copy.
    #[must_use]
    pub fn into_body(self) -> Bytes {
        self.body
    }

    /// Immutable canonical delivery metadata.
    #[must_use]
    pub const fn context(&self) -> &DeliveryContext {
        &self.context
    }

    /// Immutable bounded application headers.
    #[must_use]
    pub const fn headers(&self) -> &DeliveryHeaders {
        &self.headers
    }

    /// Immutable bounded allowlisted AMQP properties.
    #[must_use]
    pub const fn properties(&self) -> &DeliveryProperties {
        &self.properties
    }
}

impl fmt::Debug for RawDelivery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RawDelivery")
            .field("body_bytes", &self.body.len())
            .field("header_count", &self.headers.len())
            .finish_non_exhaustive()
    }
}

impl FromDelivery for RawDelivery {
    const PAYLOAD_KIND: QueuePayloadKind = QueuePayloadKind::Raw;
    type Rejection = QueueHandlerError;

    fn from_delivery(
        mut input: DeliveryPayloadInput<'_>,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        let context = input.context().clone();
        let headers = input.headers().clone();
        let properties = input.properties().clone();
        let result = input.take_body().map(|body| Self {
            body,
            context,
            headers,
            properties,
        });
        std::future::ready(result)
    }
}

fn require_content_kind(
    input: &DeliveryPayloadInput<'_>,
    expected: &str,
) -> Result<(), QueueHandlerError> {
    if input.content_kind().as_str() == expected {
        Ok(())
    } else {
        Err(QueueHandlerError::permanent("QUEUE_CONTENT_KIND_MISMATCH"))
    }
}

#[cfg(test)]
mod tests {
    use std::{future::ready, sync::Arc};

    use lily_queue_registry::QueuePayloadKind;

    use super::*;
    use crate::{EventId, Redelivered, RetryCount, SchemaVersion};

    fn context(kind: &str) -> DeliveryContext {
        DeliveryContext {
            event_id: EventId(uuid::Uuid::new_v4()),
            schema_version: SchemaVersion::try_new(1).unwrap(),
            content_kind: ContentKind::try_new(kind).unwrap(),
            retry_count: RetryCount(0),
            redelivered: Redelivered(false),
            queue: Arc::from("orders"),
            exchange: Arc::from("events"),
            routing_key: Arc::from("orders.created"),
        }
    }

    fn input<'a>(
        body: &'a mut Option<Bytes>,
        context: &'a DeliveryContext,
        headers: &'a DeliveryHeaders,
        properties: &'a DeliveryProperties,
    ) -> DeliveryPayloadInput<'a> {
        DeliveryPayloadInput::new(body, context, headers, properties)
    }

    #[derive(Debug, serde::Deserialize, PartialEq, Eq)]
    struct Order {
        id: u32,
    }

    #[tokio::test]
    async fn built_in_payloads_consume_one_exact_authority() {
        let headers = DeliveryHeaders::default();
        let properties = DeliveryProperties::default();

        let json_context = context("json");
        let mut json_body = Some(Bytes::from_static(br#"{"id":7}"#));
        let Json(order) = Json::<Order>::from_delivery(input(
            &mut json_body,
            &json_context,
            &headers,
            &properties,
        ))
        .await
        .unwrap();
        assert_eq!(order, Order { id: 7 });
        assert!(json_body.is_none());

        let text_context = context("text");
        let mut text_body = Some(Bytes::from_static(b"hello"));
        let text =
            TextPayload::from_delivery(input(&mut text_body, &text_context, &headers, &properties))
                .await
                .unwrap();
        assert_eq!(text.into_inner(), "hello");

        let binary_context = context("binary");
        let mut binary_body = Some(Bytes::from_static(&[1, 2, 3]));
        let binary = BinaryPayload::from_delivery(input(
            &mut binary_body,
            &binary_context,
            &headers,
            &properties,
        ))
        .await
        .unwrap();
        assert_eq!(binary.as_bytes(), &[1, 2, 3]);
    }

    #[tokio::test]
    async fn malformed_or_mismatched_payload_is_permanent_and_value_redacting() {
        let headers = DeliveryHeaders::default();
        let properties = DeliveryProperties::default();

        let json_context = context("json");
        let mut malformed = Some(Bytes::from_static(b"PRIVATE-BODY-SENTINEL"));
        let error = Json::<Order>::from_delivery(input(
            &mut malformed,
            &json_context,
            &headers,
            &properties,
        ))
        .await
        .unwrap_err();
        assert_eq!(error.code(), "QUEUE_JSON_INVALID");
        assert_eq!(
            error.class(),
            lily_error::application::QueueHandlerFailureClass::Permanent
        );
        assert!(!format!("{error:?}").contains("PRIVATE-BODY-SENTINEL"));
        assert!(malformed.is_none());

        let text_context = context("text");
        let mut invalid_utf8 = Some(Bytes::from_static(&[0xff]));
        let error = TextPayload::from_delivery(input(
            &mut invalid_utf8,
            &text_context,
            &headers,
            &properties,
        ))
        .await
        .unwrap_err();
        assert_eq!(error.code(), "QUEUE_TEXT_INVALID");

        let mut wrong_kind = Some(Bytes::from_static(b"text"));
        let error = BinaryPayload::from_delivery(input(
            &mut wrong_kind,
            &text_context,
            &headers,
            &properties,
        ))
        .await
        .unwrap_err();
        assert_eq!(error.code(), "QUEUE_CONTENT_KIND_MISMATCH");
        assert!(wrong_kind.is_some());
    }

    struct CustomPayload(Bytes);

    impl FromDelivery for CustomPayload {
        const PAYLOAD_KIND: QueuePayloadKind = QueuePayloadKind::Custom;
        type Rejection = QueueHandlerError;

        fn from_delivery(
            mut input: DeliveryPayloadInput<'_>,
        ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
            let body = input.take_body().map(Self);
            ready(body)
        }
    }

    #[tokio::test]
    async fn custom_and_raw_extractors_receive_bounded_owned_views_only() {
        let context = context("vendor.v1");
        let headers = DeliveryHeaders::default();
        let properties = DeliveryProperties::default();

        let mut custom_body = Some(Bytes::from_static(b"custom"));
        let custom =
            CustomPayload::from_delivery(input(&mut custom_body, &context, &headers, &properties))
                .await
                .unwrap();
        assert_eq!(custom.0.as_ref(), b"custom");
        assert!(custom_body.is_none());

        let mut raw_body = Some(Bytes::from_static(b"raw"));
        let raw = RawDelivery::from_delivery(input(&mut raw_body, &context, &headers, &properties))
            .await
            .unwrap();
        assert_eq!(raw.body(), b"raw");
        assert_eq!(raw.context().queue(), "orders");
        assert!(raw_body.is_none());
    }
}
