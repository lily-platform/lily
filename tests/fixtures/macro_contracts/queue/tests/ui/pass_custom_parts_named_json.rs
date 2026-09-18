use std::future::{ready, Future};

use lily_error::application::QueueHandlerError;
use queue_runtime::{queue, queue_service, DeliveryInvocation, FromDeliveryParts};

// Extractor roles are defined by traits, not by the final identifier spelling.
// An application-owned parts extractor may legitimately share a short name
// with one of Lily's payload wrappers.
struct Json;

impl FromDeliveryParts for Json {
    type Rejection = QueueHandlerError;

    fn from_delivery_parts(
        _invocation: &mut DeliveryInvocation,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(Ok(Self))
    }
}

struct AuditService;

#[queue_service]
impl AuditService {
    #[queue("audit.custom-parts", version = 1, content = "application.custom")]
    async fn handle(&self, _parts: Json) -> Result<(), QueueHandlerError> {
        Ok(())
    }
}

fn main() {}
