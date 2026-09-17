//! Typed queue-delivery extraction contracts.

mod parts;
mod payload;
mod service;

pub use parts::Local;
pub use payload::{BinaryPayload, Json, RawDelivery, TextPayload};
pub use service::Service;

use std::{
    any::{Any, TypeId},
    collections::HashMap,
    future::Future,
    sync::{Arc, RwLock},
};

use lily_error::application::QueueHandlerError;
use lily_injection::Extensions;
#[cfg(feature = "asyncapi")]
use lily_queue_registry::QueueAsyncApiPayload;
use lily_queue_registry::{QueueHandlerInputContract, QueuePayloadKind};

use crate::{
    DeliveryCancellation, DeliveryContext, DeliveryDeadline, DeliveryHeaders, DeliveryProperties,
    delivery_context::DeliveryInput,
};

pub use payload::DeliveryPayloadInput;

/// Maximum distinct concrete local-state types retained by one delivery.
pub const MAX_DELIVERY_LOCAL_ENTRIES: usize = 32;

#[derive(Clone, Default)]
struct DeliveryLocals {
    values: Arc<RwLock<HashMap<TypeId, Box<dyn Any + Send + Sync>>>>,
}

impl DeliveryLocals {
    fn insert<T>(&self, value: T) -> Result<Option<T>, QueueHandlerError>
    where
        T: Send + Sync + 'static,
    {
        let mut values = self
            .values
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let type_id = TypeId::of::<T>();
        if !values.contains_key(&type_id) && values.len() >= MAX_DELIVERY_LOCAL_ENTRIES {
            return Err(QueueHandlerError::retryable("QUEUE_DELIVERY_LOCAL_LIMIT"));
        }
        Ok(values.insert(type_id, Box::new(value)).map(|previous| {
            *previous
                .downcast::<T>()
                .expect("delivery-local TypeId and stored value type must agree")
        }))
    }

    fn get_cloned<T>(&self) -> Option<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.values
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&TypeId::of::<T>())
            .and_then(|value| value.downcast_ref::<T>())
            .cloned()
    }

    fn remove<T>(&self) -> Option<T>
    where
        T: Send + Sync + 'static,
    {
        self.values
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&TypeId::of::<T>())
            .map(|value| {
                *value
                    .downcast::<T>()
                    .expect("delivery-local TypeId and stored value type must agree")
            })
    }
}

/// Complete framework-owned state exposed to custom queue extractors.
///
/// Lily constructs one invocation for each admitted delivery. Custom
/// [`FromDeliveryParts`] implementations may inspect its bounded metadata,
/// resolve dependencies through its DI provider and exchange bounded typed
/// local state. The invocation deliberately exposes neither a second payload
/// nor broker settlement authority.
pub struct DeliveryInvocation {
    extensions: Arc<Extensions>,
    body: Option<bytes::Bytes>,
    context: DeliveryContext,
    headers: DeliveryHeaders,
    properties: DeliveryProperties,
    locals: DeliveryLocals,
    cancellation: DeliveryCancellation,
    deadline: DeliveryDeadline,
}

impl DeliveryInvocation {
    pub(crate) fn new(extensions: Arc<Extensions>, input: DeliveryInput) -> Self {
        Self {
            extensions,
            body: Some(input.body),
            context: input.context,
            headers: input.headers,
            properties: input.properties,
            locals: DeliveryLocals::default(),
            cancellation: DeliveryCancellation(input.cancellation),
            deadline: DeliveryDeadline(input.deadline),
        }
    }

    /// Read-only provider owned by the active application and delivery scope.
    #[must_use]
    pub const fn extensions(&self) -> &Arc<Extensions> {
        &self.extensions
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

    /// Immutable bounded body bytes without consuming payload authority.
    ///
    /// Middleware and guards may inspect this view. Only the terminal
    /// [`FromDelivery`] extractor can take the body authority.
    #[must_use]
    pub fn body(&self) -> &[u8] {
        self.body.as_deref().unwrap_or_default()
    }

    /// Cooperative cancellation for the admitted delivery execution.
    #[must_use]
    pub const fn cancellation(&self) -> &DeliveryCancellation {
        &self.cancellation
    }

    /// Absolute execution deadline.
    #[must_use]
    pub const fn deadline(&self) -> DeliveryDeadline {
        self.deadline
    }

    /// Returns an owned clone of one delivery-local value.
    #[must_use]
    pub fn local<T>(&self) -> Option<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.locals.get_cloned::<T>()
    }

    /// Publishes bounded typed state for a later extractor or middleware stage.
    pub fn insert_local<T>(&mut self, value: T) -> Result<Option<T>, QueueHandlerError>
    where
        T: Send + Sync + 'static,
    {
        self.locals.insert(value)
    }

    /// Removes and returns one typed delivery-local value.
    pub fn remove_local<T>(&mut self) -> Option<T>
    where
        T: Send + Sync + 'static,
    {
        self.locals.remove::<T>()
    }

    pub(crate) fn set_deadline(&mut self, deadline: tokio::time::Instant) {
        self.deadline = DeliveryDeadline(deadline);
    }

    fn payload_input(&mut self) -> DeliveryPayloadInput<'_> {
        DeliveryPayloadInput::new(
            &mut self.body,
            &self.context,
            &self.headers,
            &self.properties,
        )
    }
}

/// Extracts one owned value without consuming the delivery body.
pub trait FromDeliveryParts: Sized {
    /// Typed rejection materialized at Lily's settlement boundary.
    type Rejection: Into<QueueHandlerError> + Send;

    /// Produces one owned value from bounded delivery metadata or DI state.
    fn from_delivery_parts(
        invocation: &mut DeliveryInvocation,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send;
}

/// Extracts an optional parts value while preserving malformed input.
pub trait OptionalFromDeliveryParts: Sized {
    /// Typed rejection materialized at Lily's settlement boundary.
    type Rejection: Into<QueueHandlerError> + Send;

    /// `Ok(None)` represents semantic absence only.
    fn from_delivery_parts(
        invocation: &mut DeliveryInvocation,
    ) -> impl Future<Output = Result<Option<Self>, Self::Rejection>> + Send;
}

impl<E> FromDeliveryParts for Option<E>
where
    E: OptionalFromDeliveryParts,
{
    type Rejection = E::Rejection;

    fn from_delivery_parts(
        invocation: &mut DeliveryInvocation,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        E::from_delivery_parts(invocation)
    }
}

/// Extracts the handler's sole, terminal payload-owning argument.
pub trait FromDelivery: Sized {
    /// Static registry classification of this payload authority.
    const PAYLOAD_KIND: QueuePayloadKind;

    /// Typed rejection materialized at Lily's settlement boundary.
    type Rejection: Into<QueueHandlerError> + Send;

    /// Consumes the bounded body authority exactly once.
    fn from_delivery(
        input: DeliveryPayloadInput<'_>,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send;
}

/// Feature-gated schema authority implemented by Lily's canonical payload extractors.
///
/// Application-defined and raw extractors use explicit `schema` or
/// `opaque + content_type` handler metadata instead of implementing a second
/// registration path.
#[cfg(feature = "asyncapi")]
#[doc(hidden)]
pub trait QueueAsyncApiPayloadProvider {
    /// Produces the static payload contract retained by handler metadata.
    fn asyncapi_payload() -> QueueAsyncApiPayload;
}

#[cfg(feature = "asyncapi")]
impl<T> QueueAsyncApiPayloadProvider for Json<T>
where
    T: lily_asyncapi::schemars::JsonSchema,
{
    fn asyncapi_payload() -> QueueAsyncApiPayload {
        QueueAsyncApiPayload::generated(
            lily_asyncapi::__private::SchemaFactory::inbound::<T>(),
            "application/json",
        )
    }
}

#[cfg(feature = "asyncapi")]
impl QueueAsyncApiPayloadProvider for TextPayload {
    fn asyncapi_payload() -> QueueAsyncApiPayload {
        QueueAsyncApiPayload::text()
    }
}

#[cfg(feature = "asyncapi")]
impl QueueAsyncApiPayloadProvider for BinaryPayload {
    fn asyncapi_payload() -> QueueAsyncApiPayload {
        QueueAsyncApiPayload::binary()
    }
}

#[cfg(feature = "asyncapi")]
impl QueueAsyncApiPayloadProvider for RawDelivery {
    fn asyncapi_payload() -> QueueAsyncApiPayload {
        QueueAsyncApiPayload::Unspecified
    }
}

/// Type-level marker for a body-free delivery argument.
#[doc(hidden)]
pub enum DeliveryPartsMode {}

/// Type-level marker for the sole terminal payload argument.
#[doc(hidden)]
pub enum DeliveryPayloadMode {}

/// Hidden tuple extraction ABI used by generated queue adapters.
#[doc(hidden)]
pub trait ExtractDeliveryArguments<Plan>: Sized {
    /// Runs parts in declaration order and the optional terminal payload last.
    fn extract(
        invocation: &mut DeliveryInvocation,
    ) -> impl Future<Output = Result<Self, QueueHandlerError>> + Send;

    /// Returns static payload metadata for the registry descriptor.
    fn input_contract() -> QueueHandlerInputContract;
}

impl ExtractDeliveryArguments<()> for () {
    async fn extract(_invocation: &mut DeliveryInvocation) -> Result<Self, QueueHandlerError> {
        Ok(())
    }

    fn input_contract() -> QueueHandlerInputContract {
        QueueHandlerInputContract::new(QueuePayloadKind::None, None)
    }
}

macro_rules! impl_parts_tuple {
    ($($type:ident),+ $(,)?) => {
        impl<$($type),+>
            ExtractDeliveryArguments<($(impl_parts_tuple!(@mode $type),)+)>
            for ($($type,)+)
        where
            $($type: FromDeliveryParts + Send,)+
        {
            #[allow(non_snake_case)]
            async fn extract(
                invocation: &mut DeliveryInvocation,
            ) -> Result<Self, QueueHandlerError> {
                $(let $type = $type::from_delivery_parts(invocation).await.map_err(Into::into)?;)+
                Ok(($($type,)+))
            }

            fn input_contract() -> QueueHandlerInputContract {
                QueueHandlerInputContract::new(QueuePayloadKind::None, None)
            }
        }
    };
    (@mode $type:ident) => { DeliveryPartsMode };
}

macro_rules! impl_terminal_payload_tuple {
    (($($part:ident),*) $payload:ident) => {
        impl<$payload, $($part,)*>
            ExtractDeliveryArguments<(
                $(impl_terminal_payload_tuple!(@parts $part),)*
                DeliveryPayloadMode,
            )>
            for ($($part,)* $payload,)
        where
            $payload: FromDelivery + Send,
            $($part: FromDeliveryParts + Send,)*
        {
            #[allow(non_snake_case)]
            async fn extract(
                invocation: &mut DeliveryInvocation,
            ) -> Result<Self, QueueHandlerError> {
                $(let $part = $part::from_delivery_parts(invocation).await.map_err(Into::into)?;)*
                let $payload = $payload::from_delivery(invocation.payload_input())
                    .await
                    .map_err(Into::into)?;
                Ok(($($part,)* $payload,))
            }

            fn input_contract() -> QueueHandlerInputContract {
                QueueHandlerInputContract::new(
                    $payload::PAYLOAD_KIND,
                    Some(::std::any::type_name::<$payload>()),
                )
            }
        }
    };
    (@parts $type:ident) => { DeliveryPartsMode };
}

impl_parts_tuple!(A);
impl_parts_tuple!(A, B);
impl_parts_tuple!(A, B, C);
impl_parts_tuple!(A, B, C, D);
impl_parts_tuple!(A, B, C, D, E);
impl_parts_tuple!(A, B, C, D, E, F);
impl_parts_tuple!(A, B, C, D, E, F, G);
impl_parts_tuple!(A, B, C, D, E, F, G, H);
impl_parts_tuple!(A, B, C, D, E, F, G, H, I);
impl_parts_tuple!(A, B, C, D, E, F, G, H, I, J);
impl_parts_tuple!(A, B, C, D, E, F, G, H, I, J, K);
impl_parts_tuple!(A, B, C, D, E, F, G, H, I, J, K, L);
impl_parts_tuple!(A, B, C, D, E, F, G, H, I, J, K, L, M);
impl_parts_tuple!(A, B, C, D, E, F, G, H, I, J, K, L, M, N);
impl_parts_tuple!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O);
impl_parts_tuple!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P);

impl_terminal_payload_tuple!(() A);
impl_terminal_payload_tuple!((A) B);
impl_terminal_payload_tuple!((A, B) C);
impl_terminal_payload_tuple!((A, B, C) D);
impl_terminal_payload_tuple!((A, B, C, D) E);
impl_terminal_payload_tuple!((A, B, C, D, E) F);
impl_terminal_payload_tuple!((A, B, C, D, E, F) G);
impl_terminal_payload_tuple!((A, B, C, D, E, F, G) H);
impl_terminal_payload_tuple!((A, B, C, D, E, F, G, H) I);
impl_terminal_payload_tuple!((A, B, C, D, E, F, G, H, I) J);
impl_terminal_payload_tuple!((A, B, C, D, E, F, G, H, I, J) K);
impl_terminal_payload_tuple!((A, B, C, D, E, F, G, H, I, J, K) L);
impl_terminal_payload_tuple!((A, B, C, D, E, F, G, H, I, J, K, L) M);
impl_terminal_payload_tuple!((A, B, C, D, E, F, G, H, I, J, K, L, M) N);
impl_terminal_payload_tuple!((A, B, C, D, E, F, G, H, I, J, K, L, M, N) O);
impl_terminal_payload_tuple!((A, B, C, D, E, F, G, H, I, J, K, L, M, N, O) P);

/// Feature-gated tuple ABI that extracts only static documentation metadata.
#[cfg(feature = "asyncapi")]
#[doc(hidden)]
pub trait ExtractDeliveryAsyncApiPayload<Plan>: Sized {
    /// Returns the payload schema/content contract for this exact tuple plan.
    fn asyncapi_payload() -> QueueAsyncApiPayload;
}

#[cfg(feature = "asyncapi")]
impl ExtractDeliveryAsyncApiPayload<()> for () {
    fn asyncapi_payload() -> QueueAsyncApiPayload {
        QueueAsyncApiPayload::Unspecified
    }
}

#[cfg(feature = "asyncapi")]
macro_rules! impl_asyncapi_parts_tuple {
    ($($type:ident),+ $(,)?) => {
        impl<$($type),+>
            ExtractDeliveryAsyncApiPayload<($(impl_asyncapi_parts_tuple!(@mode $type),)+)>
            for ($($type,)+)
        where
            $($type: FromDeliveryParts + Send,)+
        {
            fn asyncapi_payload() -> QueueAsyncApiPayload {
                QueueAsyncApiPayload::Unspecified
            }
        }
    };
    (@mode $type:ident) => { DeliveryPartsMode };
}

#[cfg(feature = "asyncapi")]
macro_rules! impl_asyncapi_terminal_payload_tuple {
    (($($part:ident),*) $payload:ident) => {
        impl<$payload, $($part,)*>
            ExtractDeliveryAsyncApiPayload<(
                $(impl_asyncapi_terminal_payload_tuple!(@parts $part),)*
                DeliveryPayloadMode,
            )>
            for ($($part,)* $payload,)
        where
            $payload: FromDelivery + QueueAsyncApiPayloadProvider + Send,
            $($part: FromDeliveryParts + Send,)*
        {
            fn asyncapi_payload() -> QueueAsyncApiPayload {
                $payload::asyncapi_payload()
            }
        }
    };
    (@parts $type:ident) => { DeliveryPartsMode };
}

#[cfg(feature = "asyncapi")]
impl_asyncapi_parts_tuple!(A);
#[cfg(feature = "asyncapi")]
impl_asyncapi_parts_tuple!(A, B);
#[cfg(feature = "asyncapi")]
impl_asyncapi_parts_tuple!(A, B, C);
#[cfg(feature = "asyncapi")]
impl_asyncapi_parts_tuple!(A, B, C, D);
#[cfg(feature = "asyncapi")]
impl_asyncapi_parts_tuple!(A, B, C, D, E);
#[cfg(feature = "asyncapi")]
impl_asyncapi_parts_tuple!(A, B, C, D, E, F);
#[cfg(feature = "asyncapi")]
impl_asyncapi_parts_tuple!(A, B, C, D, E, F, G);
#[cfg(feature = "asyncapi")]
impl_asyncapi_parts_tuple!(A, B, C, D, E, F, G, H);
#[cfg(feature = "asyncapi")]
impl_asyncapi_parts_tuple!(A, B, C, D, E, F, G, H, I);
#[cfg(feature = "asyncapi")]
impl_asyncapi_parts_tuple!(A, B, C, D, E, F, G, H, I, J);
#[cfg(feature = "asyncapi")]
impl_asyncapi_parts_tuple!(A, B, C, D, E, F, G, H, I, J, K);
#[cfg(feature = "asyncapi")]
impl_asyncapi_parts_tuple!(A, B, C, D, E, F, G, H, I, J, K, L);
#[cfg(feature = "asyncapi")]
impl_asyncapi_parts_tuple!(A, B, C, D, E, F, G, H, I, J, K, L, M);
#[cfg(feature = "asyncapi")]
impl_asyncapi_parts_tuple!(A, B, C, D, E, F, G, H, I, J, K, L, M, N);
#[cfg(feature = "asyncapi")]
impl_asyncapi_parts_tuple!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O);
#[cfg(feature = "asyncapi")]
impl_asyncapi_parts_tuple!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P);

#[cfg(feature = "asyncapi")]
impl_asyncapi_terminal_payload_tuple!(() A);
#[cfg(feature = "asyncapi")]
impl_asyncapi_terminal_payload_tuple!((A) B);
#[cfg(feature = "asyncapi")]
impl_asyncapi_terminal_payload_tuple!((A, B) C);
#[cfg(feature = "asyncapi")]
impl_asyncapi_terminal_payload_tuple!((A, B, C) D);
#[cfg(feature = "asyncapi")]
impl_asyncapi_terminal_payload_tuple!((A, B, C, D) E);
#[cfg(feature = "asyncapi")]
impl_asyncapi_terminal_payload_tuple!((A, B, C, D, E) F);
#[cfg(feature = "asyncapi")]
impl_asyncapi_terminal_payload_tuple!((A, B, C, D, E, F) G);
#[cfg(feature = "asyncapi")]
impl_asyncapi_terminal_payload_tuple!((A, B, C, D, E, F, G) H);
#[cfg(feature = "asyncapi")]
impl_asyncapi_terminal_payload_tuple!((A, B, C, D, E, F, G, H) I);
#[cfg(feature = "asyncapi")]
impl_asyncapi_terminal_payload_tuple!((A, B, C, D, E, F, G, H, I) J);
#[cfg(feature = "asyncapi")]
impl_asyncapi_terminal_payload_tuple!((A, B, C, D, E, F, G, H, I, J) K);
#[cfg(feature = "asyncapi")]
impl_asyncapi_terminal_payload_tuple!((A, B, C, D, E, F, G, H, I, J, K) L);
#[cfg(feature = "asyncapi")]
impl_asyncapi_terminal_payload_tuple!((A, B, C, D, E, F, G, H, I, J, K, L) M);
#[cfg(feature = "asyncapi")]
impl_asyncapi_terminal_payload_tuple!((A, B, C, D, E, F, G, H, I, J, K, L, M) N);
#[cfg(feature = "asyncapi")]
impl_asyncapi_terminal_payload_tuple!((A, B, C, D, E, F, G, H, I, J, K, L, M, N) O);
#[cfg(feature = "asyncapi")]
impl_asyncapi_terminal_payload_tuple!((A, B, C, D, E, F, G, H, I, J, K, L, M, N, O) P);

/// Returns a documented handler tuple's static payload contract.
#[cfg(feature = "asyncapi")]
#[doc(hidden)]
pub fn delivery_asyncapi_payload<Arguments, Plan>() -> QueueAsyncApiPayload
where
    Arguments: ExtractDeliveryAsyncApiPayload<Plan>,
{
    Arguments::asyncapi_payload()
}

/// Extracts one generated handler's complete argument tuple.
#[doc(hidden)]
pub fn extract_delivery_arguments<'a, Arguments, Plan>(
    invocation: &'a mut DeliveryInvocation,
) -> impl Future<Output = Result<Arguments, QueueHandlerError>> + Send + 'a
where
    Arguments: ExtractDeliveryArguments<Plan> + 'a,
    Plan: 'a,
{
    Arguments::extract(invocation)
}

/// Computes the registry input contract for a generated handler tuple.
#[doc(hidden)]
pub fn delivery_input_contract<Arguments, Plan>() -> QueueHandlerInputContract
where
    Arguments: ExtractDeliveryArguments<Plan>,
{
    Arguments::input_contract()
}

#[cfg(feature = "fuzzing")]
pub(crate) async fn fuzz_extractor_plan(
    selector: u8,
    content_kind: &str,
    body: &[u8],
) -> (QueueHandlerInputContract, Result<usize, &'static str>, bool) {
    use crate::{
        BinaryPayload, ContentKind, DeliveryCancellation, DeliveryContext, DeliveryDeadline,
        DeliveryHeaders, DeliveryProperties, EventId, RawDelivery, Redelivered, RetryCount,
        SchemaVersion, TextPayload,
    };

    let selector = selector % 5;
    let contract = match selector {
        0 => delivery_input_contract::<
            (
                DeliveryContext,
                DeliveryHeaders,
                DeliveryProperties,
                EventId,
                SchemaVersion,
                ContentKind,
                RetryCount,
                Redelivered,
                DeliveryCancellation,
                DeliveryDeadline,
                Local<u8>,
                Option<Local<u16>>,
                Service<FuzzUnregisteredService>,
            ),
            (
                DeliveryPartsMode,
                DeliveryPartsMode,
                DeliveryPartsMode,
                DeliveryPartsMode,
                DeliveryPartsMode,
                DeliveryPartsMode,
                DeliveryPartsMode,
                DeliveryPartsMode,
                DeliveryPartsMode,
                DeliveryPartsMode,
                DeliveryPartsMode,
                DeliveryPartsMode,
                DeliveryPartsMode,
            ),
        >(),
        1 => delivery_input_contract::<(crate::Json<serde_json::Value>,), (DeliveryPayloadMode,)>(),
        2 => delivery_input_contract::<(TextPayload,), (DeliveryPayloadMode,)>(),
        3 => delivery_input_contract::<(BinaryPayload,), (DeliveryPayloadMode,)>(),
        _ => delivery_input_contract::<(RawDelivery,), (DeliveryPayloadMode,)>(),
    };

    if selector == 0 {
        return (contract, Ok(0), false);
    }

    let content_kind = match ContentKind::try_new(content_kind) {
        Ok(content_kind) => content_kind,
        Err(error) => return (contract, Err(error.code()), false),
    };
    let context = DeliveryContext {
        event_id: EventId(uuid::Uuid::nil()),
        schema_version: SchemaVersion::try_new(1).expect("one is a valid schema version"),
        content_kind,
        retry_count: RetryCount(0),
        redelivered: Redelivered(false),
        queue: Arc::from("fuzz.queue"),
        exchange: Arc::from("fuzz.exchange"),
        routing_key: Arc::from("fuzz.route"),
    };
    let headers = DeliveryHeaders::default();
    let properties = DeliveryProperties::default();
    let mut body_authority = Some(bytes::Bytes::copy_from_slice(body));
    let result = match selector {
        1 => crate::Json::<serde_json::Value>::from_delivery(DeliveryPayloadInput::new(
            &mut body_authority,
            &context,
            &headers,
            &properties,
        ))
        .await
        .map(|_| body.len())
        .map_err(|error| error.code()),
        2 => TextPayload::from_delivery(DeliveryPayloadInput::new(
            &mut body_authority,
            &context,
            &headers,
            &properties,
        ))
        .await
        .map(|payload| payload.0.len())
        .map_err(|error| error.code()),
        3 => BinaryPayload::from_delivery(DeliveryPayloadInput::new(
            &mut body_authority,
            &context,
            &headers,
            &properties,
        ))
        .await
        .map(|payload| payload.0.len())
        .map_err(|error| error.code()),
        _ => RawDelivery::from_delivery(DeliveryPayloadInput::new(
            &mut body_authority,
            &context,
            &headers,
            &properties,
        ))
        .await
        .map(|payload| payload.body().len())
        .map_err(|error| error.code()),
    };
    (contract, result, body_authority.is_none())
}

#[cfg(feature = "fuzzing")]
struct FuzzUnregisteredService;

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, Copy)]
    struct LocalSlot<const INDEX: usize>(u8);

    #[test]
    fn delivery_locals_enforce_exact_distinct_type_bound_and_allow_replacement() {
        let locals = DeliveryLocals::default();
        macro_rules! insert_slots {
            ($($index:literal),+ $(,)?) => {
                $(locals.insert(LocalSlot::<$index>(0)).unwrap();)+
            };
        }
        insert_slots!(
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
            24, 25, 26, 27, 28, 29, 30, 31,
        );

        assert_eq!(locals.insert(LocalSlot::<0>(9)).unwrap().unwrap().0, 0);
        assert_eq!(locals.get_cloned::<LocalSlot<0>>().unwrap().0, 9);
        assert_eq!(
            locals.insert(LocalSlot::<32>(0)).unwrap_err().code(),
            "QUEUE_DELIVERY_LOCAL_LIMIT"
        );
    }

    #[derive(serde::Deserialize)]
    #[cfg_attr(feature = "asyncapi", derive(lily_asyncapi::schemars::JsonSchema))]
    #[cfg_attr(feature = "asyncapi", schemars(crate = "crate::schemars"))]
    struct FixtureBody {
        _value: u8,
    }

    #[test]
    fn input_contract_distinguishes_parts_only_and_terminal_payload_plans() {
        let parts = delivery_input_contract::<(DeliveryContext,), (DeliveryPartsMode,)>();
        assert_eq!(parts.payload_kind, QueuePayloadKind::None);
        assert_eq!(parts.payload_type_name, None);

        let payload = delivery_input_contract::<
            (DeliveryContext, Json<FixtureBody>),
            (DeliveryPartsMode, DeliveryPayloadMode),
        >();
        assert_eq!(payload.payload_kind, QueuePayloadKind::Json);
        assert!(
            payload
                .payload_type_name
                .is_some_and(|name| name.contains("Json") && name.contains("FixtureBody"))
        );
    }

    #[cfg(feature = "asyncapi")]
    #[test]
    fn asyncapi_payload_contracts_follow_the_terminal_extractor() {
        let json = delivery_asyncapi_payload::<
            (DeliveryContext, Json<FixtureBody>),
            (DeliveryPartsMode, DeliveryPayloadMode),
        >();
        assert!(matches!(
            json,
            QueueAsyncApiPayload::Generated {
                content_type: "application/json",
                ..
            }
        ));

        let text = delivery_asyncapi_payload::<(TextPayload,), (DeliveryPayloadMode,)>();
        assert!(matches!(
            text,
            QueueAsyncApiPayload::Text {
                content_type: "text/plain; charset=utf-8"
            }
        ));

        let binary = delivery_asyncapi_payload::<(BinaryPayload,), (DeliveryPayloadMode,)>();
        assert!(matches!(
            binary,
            QueueAsyncApiPayload::Binary {
                content_type: "application/octet-stream"
            }
        ));
    }

    #[cfg(feature = "asyncapi")]
    #[test]
    fn raw_and_parts_only_handlers_require_an_explicit_payload_contract() {
        assert!(matches!(
            delivery_asyncapi_payload::<(RawDelivery,), (DeliveryPayloadMode,)>(),
            QueueAsyncApiPayload::Unspecified
        ));
        assert!(matches!(
            delivery_asyncapi_payload::<(DeliveryContext,), (DeliveryPartsMode,)>(),
            QueueAsyncApiPayload::Unspecified
        ));
    }
}
