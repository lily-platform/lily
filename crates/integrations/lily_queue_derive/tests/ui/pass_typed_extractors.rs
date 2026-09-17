use std::future::{ready, Future};

use lily_error::application::QueueHandlerError;
use queue_runtime::{
    queue, queue_service, BinaryPayload, DeliveryInvocation, DeliveryPayloadInput, FromDelivery,
    FromDeliveryParts, Json, QueuePayloadKind, RawDelivery, TextPayload,
};

struct CustomParts;

impl FromDeliveryParts for CustomParts {
    type Rejection = QueueHandlerError;

    fn from_delivery_parts(
        _invocation: &mut DeliveryInvocation,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(Ok(Self))
    }
}

struct CustomPayload;

impl FromDelivery for CustomPayload {
    const PAYLOAD_KIND: QueuePayloadKind = QueuePayloadKind::Custom;
    type Rejection = QueueHandlerError;

    fn from_delivery(
        _input: DeliveryPayloadInput<'_>,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(Ok(Self))
    }
}

struct TypedService;

#[queue_service]
impl TypedService {
    #[queue("audit.empty", version = 1, content = "json")]
    async fn empty(&self) -> Result<(), QueueHandlerError> {
        Ok(())
    }

    #[queue("audit.json", version = 1, content = "json")]
    async fn json(
        &self,
        _parts: CustomParts,
        _payload: Json<String>,
    ) -> Result<(), QueueHandlerError> {
        Ok(())
    }

    #[queue("audit.text", version = 1, content = "text")]
    async fn text(&self, _payload: TextPayload) -> Result<(), QueueHandlerError> {
        Ok(())
    }

    #[queue("audit.binary", version = 1, content = "binary")]
    async fn binary(&self, _payload: BinaryPayload) -> Result<(), QueueHandlerError> {
        Ok(())
    }

    #[queue("audit.raw", version = 1, content = "application.raw")]
    async fn raw(&self, _payload: RawDelivery) -> Result<(), QueueHandlerError> {
        Ok(())
    }

    #[queue("audit.custom", version = 1, content = "application.custom")]
    async fn custom(
        &self,
        _parts: CustomParts,
        _payload: CustomPayload,
    ) -> Result<(), QueueHandlerError> {
        Ok(())
    }
}

fn main() {}
