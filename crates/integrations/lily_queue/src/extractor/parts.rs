use std::future::{Future, ready};

use lily_error::application::QueueHandlerError;

use crate::{
    ContentKind, DeliveryCancellation, DeliveryContext, DeliveryDeadline, DeliveryHeaders,
    DeliveryProperties, EventId, Redelivered, RetryCount, SchemaVersion,
};

use super::{DeliveryInvocation, FromDeliveryParts, OptionalFromDeliveryParts};

/// Owned clone of state scoped to one delivery execution.
///
/// Store and extract `Arc<T>` when cloning the application value would be
/// expensive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Local<T>(pub T)
where
    T: Clone + Send + Sync + 'static;

impl<T> Local<T>
where
    T: Clone + Send + Sync + 'static,
{
    /// Returns the owned local value.
    #[must_use]
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl FromDeliveryParts for DeliveryContext {
    type Rejection = QueueHandlerError;

    fn from_delivery_parts(
        invocation: &mut DeliveryInvocation,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(Ok(invocation.context().clone()))
    }
}

impl FromDeliveryParts for DeliveryHeaders {
    type Rejection = QueueHandlerError;

    fn from_delivery_parts(
        invocation: &mut DeliveryInvocation,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(Ok(invocation.headers().clone()))
    }
}

impl FromDeliveryParts for DeliveryProperties {
    type Rejection = QueueHandlerError;

    fn from_delivery_parts(
        invocation: &mut DeliveryInvocation,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(Ok(invocation.properties().clone()))
    }
}

impl FromDeliveryParts for EventId {
    type Rejection = QueueHandlerError;

    fn from_delivery_parts(
        invocation: &mut DeliveryInvocation,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(Ok(invocation.context().event_id()))
    }
}

impl FromDeliveryParts for SchemaVersion {
    type Rejection = QueueHandlerError;

    fn from_delivery_parts(
        invocation: &mut DeliveryInvocation,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(Ok(invocation.context().schema_version()))
    }
}

impl FromDeliveryParts for ContentKind {
    type Rejection = QueueHandlerError;

    fn from_delivery_parts(
        invocation: &mut DeliveryInvocation,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(Ok(invocation.context().content_kind().clone()))
    }
}

impl FromDeliveryParts for RetryCount {
    type Rejection = QueueHandlerError;

    fn from_delivery_parts(
        invocation: &mut DeliveryInvocation,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(Ok(invocation.context().retry_count()))
    }
}

impl FromDeliveryParts for Redelivered {
    type Rejection = QueueHandlerError;

    fn from_delivery_parts(
        invocation: &mut DeliveryInvocation,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(Ok(invocation.context().redelivered()))
    }
}

impl FromDeliveryParts for DeliveryCancellation {
    type Rejection = QueueHandlerError;

    fn from_delivery_parts(
        invocation: &mut DeliveryInvocation,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(Ok(invocation.cancellation().clone()))
    }
}

impl FromDeliveryParts for DeliveryDeadline {
    type Rejection = QueueHandlerError;

    fn from_delivery_parts(
        invocation: &mut DeliveryInvocation,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(Ok(invocation.deadline()))
    }
}

impl<T> FromDeliveryParts for Local<T>
where
    T: Clone + Send + Sync + 'static,
{
    type Rejection = QueueHandlerError;

    fn from_delivery_parts(
        invocation: &mut DeliveryInvocation,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(
            invocation
                .local::<T>()
                .map(Self)
                .ok_or_else(|| QueueHandlerError::retryable("QUEUE_DELIVERY_LOCAL_MISSING")),
        )
    }
}

impl<T> OptionalFromDeliveryParts for Local<T>
where
    T: Clone + Send + Sync + 'static,
{
    type Rejection = QueueHandlerError;

    fn from_delivery_parts(
        invocation: &mut DeliveryInvocation,
    ) -> impl Future<Output = Result<Option<Self>, Self::Rejection>> + Send {
        ready(Ok(invocation.local::<T>().map(Self)))
    }
}
