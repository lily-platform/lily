use std::fmt;
use std::future::{Future, ready};

use serde::de::DeserializeOwned;

use crate::codec::{
    DecodedWebSocketPayload, EncodedWebSocketPayload, RawEnvelope, WebSocketPayloadCodec,
};

use super::{FromWebSocketPayload, WebSocketExtractionError};

/// Mutable single-consumer view of decoded payload and original envelope authority.
pub struct WebSocketPayloadInput<'a> {
    payload: &'a mut Option<EncodedWebSocketPayload>,
    payload_codec: &'a dyn WebSocketPayloadCodec,
    raw_envelope: &'a mut Option<RawEnvelope>,
    consumed: bool,
}

impl<'a> WebSocketPayloadInput<'a> {
    pub(super) fn new(
        payload: &'a mut Option<EncodedWebSocketPayload>,
        payload_codec: &'a dyn WebSocketPayloadCodec,
        raw_envelope: &'a mut Option<RawEnvelope>,
    ) -> Self {
        Self {
            payload,
            payload_codec,
            raw_envelope,
            consumed: false,
        }
    }

    fn claim_authority(&mut self) -> Result<(), WebSocketExtractionError> {
        if self.consumed {
            return Err(WebSocketExtractionError::payload_consumed());
        }
        self.consumed = true;
        Ok(())
    }

    /// Takes the codec-normalized payload exactly once.
    pub fn take_payload(&mut self) -> Result<DecodedWebSocketPayload, WebSocketExtractionError> {
        self.claim_authority()?;
        // The normalized payload and original envelope are two views of one
        // authority, not two independently consumable bodies. Once either
        // view wins, discard the other so a custom extractor cannot retry
        // with a different representation after a decode rejection.
        self.raw_envelope.take();
        let payload = self
            .payload
            .take()
            .ok_or_else(WebSocketExtractionError::payload_consumed)?;
        self.payload_codec
            .decode_payload(payload)
            .map_err(WebSocketExtractionError::payload_codec)
    }

    /// Takes the exact bounded inbound envelope exactly once.
    pub fn take_raw_envelope(&mut self) -> Result<RawEnvelope, WebSocketExtractionError> {
        self.claim_authority()?;
        self.payload.take();
        self.raw_envelope
            .take()
            .ok_or_else(WebSocketExtractionError::payload_consumed)
    }

    /// Takes one JSON syntax tree or rejects a content-kind mismatch.
    pub fn take_json(&mut self) -> Result<serde_json::Value, WebSocketExtractionError> {
        match self.take_payload()? {
            DecodedWebSocketPayload::Json(value) => Ok(value),
            _ => Err(WebSocketExtractionError::payload_kind_mismatch()),
        }
    }

    /// Takes one UTF-8 text payload or rejects a content-kind mismatch.
    pub fn take_text(&mut self) -> Result<String, WebSocketExtractionError> {
        match self.take_payload()? {
            DecodedWebSocketPayload::Text(value) => Ok(value),
            _ => Err(WebSocketExtractionError::payload_kind_mismatch()),
        }
    }

    /// Takes one binary payload or rejects a content-kind mismatch.
    pub fn take_binary(&mut self) -> Result<Vec<u8>, WebSocketExtractionError> {
        match self.take_payload()? {
            DecodedWebSocketPayload::Binary(value) => Ok(value),
            _ => Err(WebSocketExtractionError::payload_kind_mismatch()),
        }
    }
}

impl fmt::Debug for WebSocketPayloadInput<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WebSocketPayloadInput")
            .field("payload_available", &self.payload.is_some())
            .field("raw_envelope_available", &self.raw_envelope.is_some())
            .finish()
    }
}

/// JSON payload deserialized into `T` exactly once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Payload<T>(pub T);

impl<T> Payload<T> {
    /// Returns the deserialized application value.
    #[must_use]
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> FromWebSocketPayload for Payload<T>
where
    T: DeserializeOwned + Send,
{
    type Rejection = WebSocketExtractionError;

    fn from_websocket_payload(
        mut input: WebSocketPayloadInput<'_>,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(
            input
                .take_json()
                .and_then(|value| {
                    serde_json::from_value(value).map_err(WebSocketExtractionError::invalid_json)
                })
                .map(Self),
        )
    }
}

/// Owned codec-validated UTF-8 text payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextPayload(pub String);

impl TextPayload {
    /// Returns the owned text.
    #[must_use]
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl FromWebSocketPayload for TextPayload {
    type Rejection = WebSocketExtractionError;

    fn from_websocket_payload(
        mut input: WebSocketPayloadInput<'_>,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(input.take_text().map(Self))
    }
}

/// Owned codec-decoded binary application payload.
#[derive(Clone, PartialEq, Eq)]
pub struct BinaryPayload(pub Vec<u8>);

impl BinaryPayload {
    /// Returns the owned application bytes.
    #[must_use]
    pub fn into_inner(self) -> Vec<u8> {
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

impl FromWebSocketPayload for BinaryPayload {
    type Rejection = WebSocketExtractionError;

    fn from_websocket_payload(
        mut input: WebSocketPayloadInput<'_>,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(input.take_binary().map(Self))
    }
}

/// Owned codec-normalized representation before DTO deserialization.
#[derive(Clone, PartialEq)]
pub struct RawPayload(pub DecodedWebSocketPayload);

impl RawPayload {
    /// Returns the owned normalized representation.
    #[must_use]
    pub fn into_inner(self) -> DecodedWebSocketPayload {
        self.0
    }
}

impl fmt::Debug for RawPayload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RawPayload")
            .field("kind", &self.0.kind())
            .finish()
    }
}

impl FromWebSocketPayload for RawPayload {
    type Rejection = WebSocketExtractionError;

    fn from_websocket_payload(
        mut input: WebSocketPayloadInput<'_>,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(input.take_payload().map(Self))
    }
}

impl FromWebSocketPayload for RawEnvelope {
    type Rejection = WebSocketExtractionError;

    fn from_websocket_payload(
        mut input: WebSocketPayloadInput<'_>,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(input.take_raw_envelope())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::codec::{
        EncodedWebSocketPayloadData, WebSocketCodecError, WebSocketCodecFactory,
        WebSocketCodecInitError, WebSocketContentKind, WebSocketFrameKind,
    };
    use lily_injection::Extensions;

    static DECODE_CALLS: AtomicUsize = AtomicUsize::new(0);

    struct CountingCodec;

    #[async_trait::async_trait]
    impl WebSocketCodecFactory for CountingCodec {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketCodecInitError> {
            Ok(Self)
        }
    }

    impl WebSocketPayloadCodec for CountingCodec {
        fn decode_payload(
            &self,
            _payload: EncodedWebSocketPayload,
        ) -> Result<DecodedWebSocketPayload, WebSocketCodecError> {
            DECODE_CALLS.fetch_add(1, Ordering::SeqCst);
            Err(WebSocketCodecError::invalid_payload())
        }

        fn encode_payload(
            &self,
            _payload: DecodedWebSocketPayload,
        ) -> Result<EncodedWebSocketPayload, WebSocketCodecError> {
            Err(WebSocketCodecError::unsupported_content())
        }
    }

    #[test]
    fn debug_output_redacts_binary_and_raw_payload_values() {
        let secret = b"LILY_BINARY_SECRET".to_vec();
        let binary = BinaryPayload(secret.clone());
        assert!(!format!("{binary:?}").contains("LILY_BINARY_SECRET"));

        let raw = RawPayload(DecodedWebSocketPayload::Raw(secret));
        assert!(!format!("{raw:?}").contains("LILY_BINARY_SECRET"));

        let envelope =
            RawEnvelope::try_new(WebSocketFrameKind::Binary, b"LILY_ENVELOPE_SECRET", 128).unwrap();
        assert!(!format!("{envelope:?}").contains("LILY_ENVELOPE_SECRET"));
    }

    #[tokio::test]
    async fn raw_envelope_does_not_trigger_or_consume_payload_decode() {
        DECODE_CALLS.store(0, Ordering::SeqCst);
        let mut payload = Some(
            EncodedWebSocketPayload::try_new(
                WebSocketContentKind::Raw,
                "application/octet-stream",
                "identity",
                EncodedWebSocketPayloadData::Bytes(vec![1, 2, 3]),
            )
            .unwrap(),
        );
        let mut envelope =
            Some(RawEnvelope::try_new(WebSocketFrameKind::Binary, [4, 5, 6], 3).unwrap());
        let input = WebSocketPayloadInput::new(&mut payload, &CountingCodec, &mut envelope);

        let extracted = RawEnvelope::from_websocket_payload(input).await.unwrap();

        assert_eq!(extracted.as_bytes(), &[4, 5, 6]);
        assert_eq!(DECODE_CALLS.load(Ordering::SeqCst), 0);
        assert!(payload.is_none());
        assert!(envelope.is_none());
    }

    #[test]
    fn either_payload_view_invalidates_the_other_authority() {
        DECODE_CALLS.store(0, Ordering::SeqCst);
        let mut payload = Some(
            EncodedWebSocketPayload::try_new(
                WebSocketContentKind::Raw,
                "application/octet-stream",
                "identity",
                EncodedWebSocketPayloadData::Bytes(vec![1, 2, 3]),
            )
            .unwrap(),
        );
        let mut envelope =
            Some(RawEnvelope::try_new(WebSocketFrameKind::Binary, [4, 5, 6], 3).unwrap());
        let mut input = WebSocketPayloadInput::new(&mut payload, &CountingCodec, &mut envelope);

        assert!(input.take_payload().is_err());
        assert!(input.take_raw_envelope().is_err());
        assert!(payload.is_none());
        assert!(envelope.is_none());
        assert_eq!(DECODE_CALLS.load(Ordering::SeqCst), 1);
    }
}
