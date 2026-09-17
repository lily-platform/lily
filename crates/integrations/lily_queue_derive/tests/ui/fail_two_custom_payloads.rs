use std::future::{ready, Future};

use lily_error::application::QueueHandlerError;
use queue_runtime::{queue, queue_service, DeliveryPayloadInput, FromDelivery, QueuePayloadKind};

struct FirstPayload;
struct SecondPayload;

macro_rules! impl_payload {
    ($type:ty) => {
        impl FromDelivery for $type {
            const PAYLOAD_KIND: QueuePayloadKind = QueuePayloadKind::Custom;
            type Rejection = QueueHandlerError;

            fn from_delivery(
                _input: DeliveryPayloadInput<'_>,
            ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
                ready(Ok(Self))
            }
        }
    };
}

impl_payload!(FirstPayload);
impl_payload!(SecondPayload);

struct AuditService;

#[queue_service]
impl AuditService {
    #[queue("audit.custom", version = 1, content = "application.custom")]
    async fn handle(
        &self,
        _first: FirstPayload,
        _second: SecondPayload,
    ) -> Result<(), QueueHandlerError> {
        Ok(())
    }
}

fn main() {}
