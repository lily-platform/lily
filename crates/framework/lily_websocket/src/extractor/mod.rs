//! Typed WebSocket action and lifecycle extraction contracts.

mod cancellation;
mod message_cancellation;
pub(crate) use message_cancellation::{
    MESSAGE_COOPERATIVE_CAP, MessageCancellationCause, MessageCancellationFacts,
};
mod lifecycle;
mod parts;
mod payload;
mod service;

#[allow(deprecated)]
pub use cancellation::{Cancellation, CleanupCancellation, ExecutionCancellation};
pub use lifecycle::{
    Connected, DisconnectReason, Disconnected, FromWebSocketLifecycleParts,
    OptionalFromWebSocketLifecycleParts, WebSocketLifecycleInvocation, extract_connected_arguments,
    extract_disconnected_arguments, extract_lifecycle_arguments,
};
pub use parts::{
    ConnectionId, ConnectionLocal, EventName, MessageDeadline, MessageHeaders, MessageLocal,
    Namespace, WebSocketExtractionError, WebSocketExtractionFailureKind, WebSocketMessageContext,
};
pub use payload::{BinaryPayload, Payload, RawPayload, TextPayload, WebSocketPayloadInput};
pub use service::Service;

use std::future::Future;
use std::sync::{Arc, RwLock};

use lily_injection::Extensions;
use lily_web_core::RequestExtensions;

use crate::codec::{
    EncodedWebSocketPayload, RawEnvelope, WebSocketFrameKind, WebSocketMessageHeaders,
    WebSocketPayloadCodec,
};
use crate::controller::WebSocketContext;
use crate::outcome::WebSocketActionError;

/// Maximum distinct concrete message-local types retained by one dispatch.
pub const MAX_WEBSOCKET_MESSAGE_LOCAL_ENTRIES: usize = 32;

/// Admission failure while publishing state scoped to one message dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum WebSocketMessageLocalError {
    /// The dispatch already contains Lily's maximum number of distinct types.
    #[error("WebSocket message local-state entry limit was reached")]
    TooManyEntries,
}

/// One message's shared typed local-state authority.
///
/// Middleware and guards publish values before extraction; extractors and the
/// reverse middleware unwind observe the same map. The handle is crate-owned
/// so application code cannot retain the complete map beyond the dispatch.
#[derive(Clone, Default)]
pub(crate) struct WebSocketMessageLocals {
    values: Arc<RwLock<RequestExtensions>>,
}

impl WebSocketMessageLocals {
    pub(crate) fn insert<T>(&self, value: T) -> Result<Option<T>, WebSocketMessageLocalError>
    where
        T: Send + Sync + 'static,
    {
        let mut values = self
            .values
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if values.get::<T>().is_none() && values.len() >= MAX_WEBSOCKET_MESSAGE_LOCAL_ENTRIES {
            return Err(WebSocketMessageLocalError::TooManyEntries);
        }
        Ok(values.insert(value))
    }

    pub(crate) fn get_cloned<T>(&self) -> Option<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.values
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get::<T>()
            .cloned()
    }

    pub(crate) fn remove<T>(&self) -> Option<T>
    where
        T: Send + Sync + 'static,
    {
        self.values
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove::<T>()
    }
}

/// Complete owned state for one typed message-action invocation.
///
/// The framework creates this value only after frame decode, exact route
/// lookup and payload-codec selection. Public custom extractors can inspect
/// its bounded parts but cannot obtain a second payload authority.
#[doc(hidden)]
pub struct WebSocketMessageInvocation {
    extensions: Arc<Extensions>,
    context: Arc<WebSocketContext>,
    principal: Option<lily_web_core::Principal>,
    namespace: String,
    event: String,
    headers: WebSocketMessageHeaders,
    payload: Option<EncodedWebSocketPayload>,
    payload_codec: Arc<dyn WebSocketPayloadCodec>,
    raw_envelope: Option<RawEnvelope>,
    message_locals: WebSocketMessageLocals,
    cancellation: ExecutionCancellation,
    deadline: tokio::time::Instant,
    message_id: Option<String>,
    ack_id: Option<String>,
    room: Option<String>,
    timestamp_millis: i64,
    frame_kind: WebSocketFrameKind,
}

impl WebSocketMessageInvocation {
    /// Constructs a validated runtime invocation from codec-owned state.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        extensions: Arc<Extensions>,
        context: Arc<WebSocketContext>,
        principal: Option<lily_web_core::Principal>,
        namespace: String,
        event: String,
        headers: WebSocketMessageHeaders,
        payload: EncodedWebSocketPayload,
        payload_codec: Arc<dyn WebSocketPayloadCodec>,
        raw_envelope: RawEnvelope,
        cancellation: ExecutionCancellation,
        deadline: tokio::time::Instant,
        message_id: Option<String>,
        ack_id: Option<String>,
        room: Option<String>,
        timestamp_millis: i64,
        message_locals: WebSocketMessageLocals,
    ) -> Self {
        let frame_kind = raw_envelope.kind();
        Self {
            extensions,
            context,
            principal,
            namespace,
            event,
            headers,
            payload: Some(payload),
            payload_codec,
            raw_envelope: Some(raw_envelope),
            message_locals,
            cancellation,
            deadline,
            message_id,
            ack_id,
            room,
            timestamp_millis,
            frame_kind,
        }
    }

    /// Read-only application DI provider for custom parts extractors.
    #[must_use]
    pub fn extensions(&self) -> Arc<Extensions> {
        Arc::clone(&self.extensions)
    }

    /// Shared connection-scoped controller context.
    #[must_use]
    pub fn context(&self) -> &Arc<WebSocketContext> {
        &self.context
    }

    /// Application-owned principal frozen for this message invocation.
    #[must_use]
    pub fn principal(&self) -> Option<&lily_web_core::Principal> {
        self.principal.as_ref()
    }

    /// Exact controller namespace.
    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Controller-local message event.
    #[must_use]
    pub fn event(&self) -> &str {
        &self.event
    }

    /// Bounded application metadata.
    #[must_use]
    pub const fn headers(&self) -> &WebSocketMessageHeaders {
        &self.headers
    }

    /// Returns an owned clone of one message-local value.
    ///
    /// Store `Arc<T>` when cloning the underlying application value would be
    /// expensive. Semantic absence is preserved as `None`.
    #[must_use]
    pub fn message_local<T>(&self) -> Option<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.message_locals.get_cloned::<T>()
    }

    /// Publishes message-local state for sequential custom extraction.
    #[doc(hidden)]
    pub fn insert_message_local<T>(
        &mut self,
        value: T,
    ) -> Result<Option<T>, WebSocketMessageLocalError>
    where
        T: Send + Sync + 'static,
    {
        self.message_locals.insert(value)
    }

    /// Read-only execution signal tied to this dispatch.
    #[must_use]
    pub const fn cancellation(&self) -> &ExecutionCancellation {
        &self.cancellation
    }

    /// Absolute message deadline shared by middleware, guards, extraction,
    /// action, response preparation and normal reverse exit.
    #[must_use]
    pub const fn deadline(&self) -> tokio::time::Instant {
        self.deadline
    }

    /// Optional application message identity.
    #[must_use]
    pub fn message_id(&self) -> Option<&str> {
        self.message_id.as_deref()
    }

    /// Optional inbound acknowledgement authority.
    #[must_use]
    pub fn ack_id(&self) -> Option<&str> {
        self.ack_id.as_deref()
    }

    /// Optional public-room target declared by the decoded envelope.
    #[must_use]
    pub fn room(&self) -> Option<&str> {
        self.room.as_deref()
    }

    /// Non-negative sender timestamp from the decoded envelope.
    #[must_use]
    pub const fn timestamp_millis(&self) -> i64 {
        self.timestamp_millis
    }

    /// Original inbound frame kind.
    #[must_use]
    pub const fn frame_kind(&self) -> WebSocketFrameKind {
        self.frame_kind
    }

    fn payload_input(&mut self) -> WebSocketPayloadInput<'_> {
        WebSocketPayloadInput::new(
            &mut self.payload,
            self.payload_codec.as_ref(),
            &mut self.raw_envelope,
        )
    }
}

/// Extracts an owned value without consuming message payload authority.
pub trait FromWebSocketMessageParts: Sized {
    /// Typed extraction failure converted at Lily's safe action boundary.
    type Rejection: Into<WebSocketActionError> + Send;

    /// Produces one owned value from bounded message and connection context.
    fn from_message_parts(
        invocation: &mut WebSocketMessageInvocation,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send;
}

/// Optional parts extraction preserving malformed input as an error.
pub trait OptionalFromWebSocketMessageParts: Sized {
    /// Typed extraction failure converted at Lily's safe action boundary.
    type Rejection: Into<WebSocketActionError> + Send;

    /// `Ok(None)` represents semantic absence only.
    fn from_message_parts(
        invocation: &mut WebSocketMessageInvocation,
    ) -> impl Future<Output = Result<Option<Self>, Self::Rejection>> + Send;
}

impl<E> FromWebSocketMessageParts for Option<E>
where
    E: OptionalFromWebSocketMessageParts,
{
    type Rejection = E::Rejection;

    fn from_message_parts(
        invocation: &mut WebSocketMessageInvocation,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        E::from_message_parts(invocation)
    }
}

/// Extracts the action's single payload-owning argument.
pub trait FromWebSocketPayload: Sized {
    /// Typed extraction failure converted at Lily's safe action boundary.
    type Rejection: Into<WebSocketActionError> + Send;

    /// Consumes exactly one decoded-payload or raw-envelope authority.
    fn from_websocket_payload(
        input: WebSocketPayloadInput<'_>,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send;
}

/// Type-level marker for one message-parts argument.
#[doc(hidden)]
pub enum MessagePartsMode {}
/// Type-level marker for the action's single payload-owning argument.
#[doc(hidden)]
pub enum MessagePayloadMode {}

/// Hidden tuple extraction ABI used by generated controller adapters.
#[doc(hidden)]
pub trait ExtractWebSocketMessageArguments<Plan>: Sized {
    /// Runs every parts extractor in declaration order, then the sole payload
    /// extractor, and rebuilds the tuple in its original declaration order.
    fn extract(
        invocation: &mut WebSocketMessageInvocation,
    ) -> impl Future<Output = Result<Self, WebSocketActionError>> + Send;
}

impl ExtractWebSocketMessageArguments<()> for () {
    async fn extract(
        _invocation: &mut WebSocketMessageInvocation,
    ) -> Result<Self, WebSocketActionError> {
        Ok(())
    }
}

macro_rules! impl_message_parts_tuple {
    ($($type:ident),+ $(,)?) => {
        impl<$($type),+>
            ExtractWebSocketMessageArguments<($(impl_message_parts_tuple!(@mode $type),)+)>
            for ($($type,)+)
        where
            $($type: FromWebSocketMessageParts + Send,)+
        {
            #[allow(non_snake_case)]
            async fn extract(
                invocation: &mut WebSocketMessageInvocation,
            ) -> Result<Self, WebSocketActionError> {
                $(let $type = $type::from_message_parts(invocation).await.map_err(Into::into)?;)+
                Ok(($($type,)+))
            }
        }
    };
    (@mode $type:ident) => { MessagePartsMode };
}

macro_rules! impl_one_payload_position {
    (($($before:ident),*) $payload:ident ($($after:ident),*)) => {
        impl<$payload, $($before,)* $($after,)*>
            ExtractWebSocketMessageArguments<(
                $(impl_one_payload_position!(@parts $before),)*
                MessagePayloadMode,
                $(impl_one_payload_position!(@parts $after),)*
            )>
            for ($($before,)* $payload, $($after,)*)
        where
            $payload: FromWebSocketPayload + Send,
            $($before: FromWebSocketMessageParts + Send,)*
            $($after: FromWebSocketMessageParts + Send,)*
        {
            #[allow(non_snake_case)]
            async fn extract(
                invocation: &mut WebSocketMessageInvocation,
            ) -> Result<Self, WebSocketActionError> {
                $(let $before = $before::from_message_parts(invocation).await.map_err(Into::into)?;)*
                $(let $after = $after::from_message_parts(invocation).await.map_err(Into::into)?;)*
                let $payload = $payload::from_websocket_payload(invocation.payload_input())
                    .await
                    .map_err(Into::into)?;
                Ok(($($before,)* $payload, $($after,)*))
            }
        }
    };
    (@parts $type:ident) => { MessagePartsMode };
}

macro_rules! impl_payload_positions {
    (($($before:ident),*); $payload:ident $(, $after:ident)*) => {
        impl_one_payload_position!(($($before),*) $payload ($($after),*));
        impl_payload_positions!(@next ($($before,)* $payload); $($after),*);
    };
    (@next ($($before:ident),*);) => {};
    (@next ($($before:ident),*); $next:ident $(, $after:ident)*) => {
        impl_payload_positions!(($($before),*); $next $(, $after)*);
    };
}

impl_message_parts_tuple!(A);
impl_message_parts_tuple!(A, B);
impl_message_parts_tuple!(A, B, C);
impl_message_parts_tuple!(A, B, C, D);
impl_message_parts_tuple!(A, B, C, D, E);
impl_message_parts_tuple!(A, B, C, D, E, F);
impl_message_parts_tuple!(A, B, C, D, E, F, G);
impl_message_parts_tuple!(A, B, C, D, E, F, G, H);
impl_message_parts_tuple!(A, B, C, D, E, F, G, H, I);
impl_message_parts_tuple!(A, B, C, D, E, F, G, H, I, J);
impl_message_parts_tuple!(A, B, C, D, E, F, G, H, I, J, K);
impl_message_parts_tuple!(A, B, C, D, E, F, G, H, I, J, K, L);
impl_message_parts_tuple!(A, B, C, D, E, F, G, H, I, J, K, L, M);
impl_message_parts_tuple!(A, B, C, D, E, F, G, H, I, J, K, L, M, N);
impl_message_parts_tuple!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O);
impl_message_parts_tuple!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P);

impl_payload_positions!((); A);
impl_payload_positions!((); A, B);
impl_payload_positions!((); A, B, C);
impl_payload_positions!((); A, B, C, D);
impl_payload_positions!((); A, B, C, D, E);
impl_payload_positions!((); A, B, C, D, E, F);
impl_payload_positions!((); A, B, C, D, E, F, G);
impl_payload_positions!((); A, B, C, D, E, F, G, H);
impl_payload_positions!((); A, B, C, D, E, F, G, H, I);
impl_payload_positions!((); A, B, C, D, E, F, G, H, I, J);
impl_payload_positions!((); A, B, C, D, E, F, G, H, I, J, K);
impl_payload_positions!((); A, B, C, D, E, F, G, H, I, J, K, L);
impl_payload_positions!((); A, B, C, D, E, F, G, H, I, J, K, L, M);
impl_payload_positions!((); A, B, C, D, E, F, G, H, I, J, K, L, M, N);
impl_payload_positions!((); A, B, C, D, E, F, G, H, I, J, K, L, M, N, O);
impl_payload_positions!((); A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P);

/// Extracts a generated action's complete tuple using its inferred type-level plan.
///
/// Two payload authorities have no valid plan and therefore fail at compile time:
///
/// ```compile_fail
/// use lily_websocket::{
///     BinaryPayload, MessagePayloadMode, Payload, WebSocketMessageInvocation,
///     extract_message_arguments,
/// };
///
/// fn invalid(invocation: &mut WebSocketMessageInvocation) {
///     let _ = extract_message_arguments::<
///         (Payload<()>, BinaryPayload),
///         (MessagePayloadMode, MessagePayloadMode),
///     >(invocation);
/// }
/// ```
#[doc(hidden)]
pub fn extract_message_arguments<'a, Arguments, Plan>(
    invocation: &'a mut WebSocketMessageInvocation,
) -> impl Future<Output = Result<Arguments, WebSocketActionError>> + Send + 'a
where
    Arguments: ExtractWebSocketMessageArguments<Plan> + 'a,
    Plan: 'a,
{
    Arguments::extract(invocation)
}

#[cfg(test)]
mod tests {
    use std::future::ready;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct LocalSlot<const INDEX: usize>(u8);

    #[test]
    fn message_locals_enforce_exact_distinct_type_bound_and_allow_replacement() {
        let locals = WebSocketMessageLocals::default();
        macro_rules! insert_slots {
            ($($index:literal),+ $(,)?) => {
                $(locals.insert(LocalSlot::<$index>(0)).unwrap();)+
            };
        }

        insert_slots!(
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
            24, 25, 26, 27, 28, 29, 30, 31,
        );
        assert_eq!(
            locals.insert(LocalSlot::<0>(1)).unwrap(),
            Some(LocalSlot::<0>(0))
        );
        assert_eq!(
            locals.insert(LocalSlot::<32>(0)),
            Err(WebSocketMessageLocalError::TooManyEntries)
        );
    }
    use crate::codec::{
        DecodedWebSocketPayload, LilyEnvelopeCodec, WebSocketFrameKind, WebSocketMessageHeaders,
        WebSocketPayloadCodec,
    };
    use crate::connection::{
        AuthenticatedWebSocketIdentity, ConnectionManager, WebSocketIdentitySnapshot,
    };
    use crate::controller::WebSocketContext;
    use crate::outcome::WebSocketErrorCode;
    use crate::request::WsHeaders;
    use crate::server::{WsHandshakeContext, WsTransportSecurity};
    use lily_injection::Injectable;
    use lily_injection::{ApplicationContainer, ServiceTrait};
    use lily_web_core::{Principal, RequestConnectionInfo, RequestExtensions};
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    static ORDERED_PARTS: AtomicUsize = AtomicUsize::new(0);
    static ORDERED_PAYLOADS: AtomicUsize = AtomicUsize::new(0);

    struct Part(u8);
    struct Body(Vec<u8>);
    struct FirstOrderedPart;
    struct SecondOrderedPart;
    struct FailingPart;
    struct OrderedBody;

    trait RuntimeProbe: Send + Sync {
        fn marker(&self) -> usize;
    }

    #[derive(Default, Injectable)]
    #[service(lifetime = "Singleton", interface = dyn RuntimeProbe)]
    struct RuntimeProbeService;

    impl RuntimeProbe for RuntimeProbeService {
        fn marker(&self) -> usize {
            42
        }
    }

    impl ServiceTrait for RuntimeProbeService {}

    #[derive(Debug)]
    struct ConnectionStateValue {
        value: usize,
        clones: Arc<AtomicUsize>,
    }

    impl Clone for ConnectionStateValue {
        fn clone(&self) -> Self {
            self.clones.fetch_add(1, Ordering::SeqCst);
            Self {
                value: self.value,
                clones: Arc::clone(&self.clones),
            }
        }
    }

    #[derive(Debug)]
    struct MessageStateValue {
        value: usize,
        clones: Arc<AtomicUsize>,
    }

    impl Clone for MessageStateValue {
        fn clone(&self) -> Self {
            self.clones.fetch_add(1, Ordering::SeqCst);
            Self {
                value: self.value,
                clones: Arc::clone(&self.clones),
            }
        }
    }

    impl FromWebSocketMessageParts for Part {
        type Rejection = WebSocketActionError;

        fn from_message_parts(
            _invocation: &mut WebSocketMessageInvocation,
        ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
            ready(Ok(Self(7)))
        }
    }

    impl FromWebSocketPayload for Body {
        type Rejection = WebSocketActionError;

        fn from_websocket_payload(
            mut input: WebSocketPayloadInput<'_>,
        ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
            ready(input.take_binary().map(Self).map_err(Into::into))
        }
    }

    impl FromWebSocketMessageParts for FirstOrderedPart {
        type Rejection = WebSocketActionError;

        fn from_message_parts(
            _invocation: &mut WebSocketMessageInvocation,
        ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
            assert_eq!(ORDERED_PARTS.fetch_add(1, Ordering::SeqCst), 0);
            ready(Ok(Self))
        }
    }

    impl FromWebSocketMessageParts for SecondOrderedPart {
        type Rejection = WebSocketActionError;

        fn from_message_parts(
            _invocation: &mut WebSocketMessageInvocation,
        ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
            assert_eq!(ORDERED_PARTS.fetch_add(1, Ordering::SeqCst), 1);
            ready(Ok(Self))
        }
    }

    impl FromWebSocketMessageParts for FailingPart {
        type Rejection = WebSocketActionError;

        fn from_message_parts(
            _invocation: &mut WebSocketMessageInvocation,
        ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
            ready(Err(rejection()))
        }
    }

    impl FromWebSocketPayload for OrderedBody {
        type Rejection = WebSocketActionError;

        fn from_websocket_payload(
            mut input: WebSocketPayloadInput<'_>,
        ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
            ORDERED_PAYLOADS.fetch_add(1, Ordering::SeqCst);
            let result = if ORDERED_PARTS.load(Ordering::SeqCst) == 2 {
                input.take_binary().map(|_| Self).map_err(Into::into)
            } else {
                Err(rejection())
            };
            ready(result)
        }
    }

    fn rejection() -> WebSocketActionError {
        WebSocketActionError::rejected(WebSocketErrorCode::EXTRACTION_FAILED, "payload is invalid")
            .unwrap()
    }

    #[allow(dead_code)]
    async fn compile_tuple_plans(
        invocation: &mut WebSocketMessageInvocation,
    ) -> Result<(), WebSocketActionError> {
        let (part, body): (Part, Body) =
            extract_message_arguments::<_, (MessagePartsMode, MessagePayloadMode)>(invocation)
                .await?;
        if part.0 != 7 || body.0.is_empty() {
            return Err(rejection());
        }
        let _: (Part,) = extract_message_arguments::<_, (MessagePartsMode,)>(invocation).await?;
        Ok(())
    }

    #[allow(dead_code)]
    fn compile_payload_first_plan(
        invocation: &mut WebSocketMessageInvocation,
    ) -> impl Future<Output = Result<(Body, Part), WebSocketActionError>> + Send + '_ {
        extract_message_arguments::<_, (MessagePayloadMode, MessagePartsMode)>(invocation)
    }

    async fn invocation_with_context(
        context: Arc<WebSocketContext>,
    ) -> (ApplicationContainer, WebSocketMessageInvocation) {
        let container = ApplicationContainer::build().await.unwrap();
        let raw = RawEnvelope::try_new(WebSocketFrameKind::Binary, [1, 2, 3], 3).unwrap();
        let payload_codec: Arc<dyn WebSocketPayloadCodec> = Arc::new(LilyEnvelopeCodec);
        let payload = payload_codec
            .encode_payload(DecodedWebSocketPayload::Binary(vec![1, 2, 3]))
            .unwrap();
        let principal = context.principal().or_else(|| {
            context
                .handshake()
                .and_then(|handshake| handshake.principal().cloned())
        });
        let invocation = WebSocketMessageInvocation::new(
            container.services(),
            context,
            principal,
            "chat".to_owned(),
            "send".to_owned(),
            WebSocketMessageHeaders::default(),
            payload,
            payload_codec,
            raw,
            ExecutionCancellation::new(CancellationToken::new()),
            tokio::time::Instant::now() + Duration::from_secs(1),
            None,
            None,
            None,
            0,
            WebSocketMessageLocals::default(),
        );
        (container, invocation)
    }

    #[tokio::test]
    async fn message_token_and_context_extractors_share_the_invocation_signal() {
        let (container, mut invocation) = binary_invocation().await;
        let source = CancellationToken::new();
        invocation.cancellation = ExecutionCancellation::new(source.clone());
        let (signal, context) = extract_message_arguments::<
            (ExecutionCancellation, WebSocketMessageContext),
            (MessagePartsMode, MessagePartsMode),
        >(&mut invocation)
        .await
        .unwrap();
        assert!(!signal.is_cancelled());
        assert!(!context.cancellation().is_cancelled());
        source.cancel();
        signal.cancelled().await;
        assert!(context.cancellation().is_cancelled());
        assert!(invocation.cancellation().is_cancelled());
        container.close().await.unwrap();
    }

    async fn binary_invocation() -> (ApplicationContainer, WebSocketMessageInvocation) {
        invocation_with_context(Arc::new(WebSocketContext::new(
            Uuid::new_v4(),
            Arc::new(ConnectionManager::new()),
            "chat".to_owned(),
        )))
        .await
    }

    #[tokio::test]
    async fn parts_run_before_payload_and_partial_failure_does_not_consume_it() {
        ORDERED_PARTS.store(0, Ordering::SeqCst);
        ORDERED_PAYLOADS.store(0, Ordering::SeqCst);
        let (container, mut invocation) = binary_invocation().await;

        let _: (OrderedBody, FirstOrderedPart, SecondOrderedPart) = extract_message_arguments::<
            _,
            (MessagePayloadMode, MessagePartsMode, MessagePartsMode),
        >(&mut invocation)
        .await
        .unwrap();

        assert_eq!(ORDERED_PARTS.load(Ordering::SeqCst), 2);
        assert_eq!(ORDERED_PAYLOADS.load(Ordering::SeqCst), 1);
        container.close().await.unwrap();

        ORDERED_PARTS.store(0, Ordering::SeqCst);
        ORDERED_PAYLOADS.store(0, Ordering::SeqCst);
        let (container, mut invocation) = binary_invocation().await;

        let result: Result<(OrderedBody, FailingPart), _> =
            extract_message_arguments::<_, (MessagePayloadMode, MessagePartsMode)>(&mut invocation)
                .await;

        assert!(result.is_err());
        assert_eq!(ORDERED_PAYLOADS.load(Ordering::SeqCst), 0);
        assert!(invocation.payload.is_some());
        assert!(invocation.raw_envelope.is_some());
        container.close().await.unwrap();
    }

    #[test]
    fn raw_envelope_keeps_its_kind() {
        let raw = RawEnvelope::try_new(WebSocketFrameKind::Binary, [1, 2, 3], 3).unwrap();
        assert_eq!(raw.kind(), WebSocketFrameKind::Binary);
    }

    #[tokio::test]
    async fn service_extractors_resolve_concrete_and_trait_routes_from_the_real_container() {
        let (container, mut invocation) = binary_invocation().await;

        let (concrete, interface): (Service<RuntimeProbeService>, Service<dyn RuntimeProbe>) =
            extract_message_arguments::<_, (MessagePartsMode, MessagePartsMode)>(&mut invocation)
                .await
                .unwrap();

        assert_eq!(concrete.marker(), 42);
        assert_eq!(interface.marker(), 42);
        assert_eq!(
            Arc::as_ptr(&concrete.0) as *const (),
            Arc::as_ptr(&interface.0) as *const ()
        );
        container.close().await.unwrap();
    }

    #[tokio::test]
    async fn principal_extractors_use_the_message_snapshot_not_the_live_connection_view() {
        let live = Principal::new(
            "live-subject",
            ["live-role".to_owned()],
            [],
            serde_json::Map::new(),
        );
        let frozen = Principal::new(
            "message-subject",
            ["message-role".to_owned()],
            [],
            serde_json::Map::new(),
        );
        let live_identity =
            AuthenticatedWebSocketIdentity::try_new(live).expect("bounded live identity snapshot");
        let context = Arc::new(
            WebSocketContext::new(
                Uuid::new_v4(),
                Arc::new(ConnectionManager::new()),
                "chat".to_owned(),
            )
            .with_identity_snapshot(Some(WebSocketIdentitySnapshot::authenticated(
                live_identity,
            ))),
        );
        let (container, mut invocation) = invocation_with_context(context).await;
        invocation.principal = Some(frozen);

        let extracted =
            <Principal as FromWebSocketMessageParts>::from_message_parts(&mut invocation)
                .await
                .expect("message principal snapshot");
        let message_context =
            <WebSocketMessageContext as FromWebSocketMessageParts>::from_message_parts(
                &mut invocation,
            )
            .await
            .expect("message context snapshot");

        assert_eq!(extracted.subject(), "message-subject");
        assert_eq!(
            message_context.principal().map(Principal::subject),
            Some("message-subject")
        );
        assert_eq!(
            invocation
                .context()
                .principal()
                .map(|principal| principal.subject().to_owned()),
            Some("live-subject".to_owned())
        );
        container.close().await.unwrap();
    }

    #[tokio::test]
    #[allow(clippy::type_complexity)]
    async fn principal_and_local_extractors_preserve_required_optional_and_clone_contracts() {
        let connection_clones = Arc::new(AtomicUsize::new(0));
        let message_clones = Arc::new(AtomicUsize::new(0));
        let mut connection_locals = RequestExtensions::new();
        connection_locals.insert(ConnectionStateValue {
            value: 7,
            clones: Arc::clone(&connection_clones),
        });
        let principal = Principal::new(
            "subject-42",
            ["operator".to_owned()],
            ["chat:send".to_owned()],
            serde_json::Map::new(),
        );
        let peer_addr = "127.0.0.1:43123".parse().unwrap();
        let handshake = Arc::new(WsHandshakeContext::new(
            "chat".to_owned(),
            WsHeaders::default(),
            Some(principal),
            None,
            peer_addr,
            RequestConnectionInfo::direct(peer_addr.ip()),
            WsTransportSecurity::Plaintext,
        ));
        let context = Arc::new(
            WebSocketContext::new(
                Uuid::new_v4(),
                Arc::new(ConnectionManager::new()),
                "chat".to_owned(),
            )
            .with_connection_locals(Arc::new(connection_locals))
            .with_handshake_context(handshake),
        );
        let (container, mut invocation) = invocation_with_context(context).await;
        invocation
            .insert_message_local(MessageStateValue {
                value: 9,
                clones: Arc::clone(&message_clones),
            })
            .unwrap();

        let (
            principal,
            optional_principal,
            connection,
            optional_connection,
            message,
            optional_message,
        ): (
            Principal,
            Option<Principal>,
            ConnectionLocal<ConnectionStateValue>,
            Option<ConnectionLocal<ConnectionStateValue>>,
            MessageLocal<MessageStateValue>,
            Option<MessageLocal<MessageStateValue>>,
        ) = extract_message_arguments::<
            _,
            (
                MessagePartsMode,
                MessagePartsMode,
                MessagePartsMode,
                MessagePartsMode,
                MessagePartsMode,
                MessagePartsMode,
            ),
        >(&mut invocation)
        .await
        .unwrap();

        assert_eq!(principal.subject(), "subject-42");
        assert_eq!(optional_principal.unwrap().subject(), "subject-42");
        assert_eq!(connection.0.value, 7);
        assert_eq!(optional_connection.unwrap().0.value, 7);
        assert_eq!(message.0.value, 9);
        assert_eq!(optional_message.unwrap().0.value, 9);
        assert_eq!(connection_clones.load(Ordering::SeqCst), 2);
        assert_eq!(message_clones.load(Ordering::SeqCst), 2);
        container.close().await.unwrap();

        let (container, mut invocation) = binary_invocation().await;
        let required_principal =
            <Principal as FromWebSocketMessageParts>::from_message_parts(&mut invocation).await;
        assert!(matches!(
            required_principal,
            Err(error)
                if error.kind() == WebSocketExtractionFailureKind::MissingPrincipal
        ));
        let required_connection = <ConnectionLocal<ConnectionStateValue> as
            FromWebSocketMessageParts>::from_message_parts(&mut invocation)
        .await;
        assert!(matches!(
            required_connection,
            Err(error)
                if error.kind() == WebSocketExtractionFailureKind::MissingConnectionLocal
        ));
        let required_message =
            <MessageLocal<MessageStateValue> as FromWebSocketMessageParts>::from_message_parts(
                &mut invocation,
            )
            .await;
        assert!(matches!(
            required_message,
            Err(error) if error.kind() == WebSocketExtractionFailureKind::MissingMessageLocal
        ));

        let (principal, connection, message): (
            Option<Principal>,
            Option<ConnectionLocal<ConnectionStateValue>>,
            Option<MessageLocal<MessageStateValue>>,
        ) = extract_message_arguments::<_, (MessagePartsMode, MessagePartsMode, MessagePartsMode)>(
            &mut invocation,
        )
        .await
        .unwrap();
        assert!(principal.is_none());
        assert!(connection.is_none());
        assert!(message.is_none());
        container.close().await.unwrap();
    }
}
