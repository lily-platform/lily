use std::sync::Arc;

use lily_error::application::QueueHandlerError;
use queue_runtime::{
    async_trait, guard, middleware, queue, queue_service, Extensions, QueueDeliveryExchange,
    QueueGuard, QueueMiddleware, QueuePipelineComponentInitError,
};

struct ServiceFirst;
struct ServiceSecond;
struct HandlerFirst;
struct HandlerSecond;
struct ServiceGuard;
struct ServiceGuardSecond;
struct HandlerGuard;
struct HandlerGuardSecond;

macro_rules! middleware_impl {
    ($ty:ty) => {
        #[async_trait]
        impl QueueMiddleware for $ty {
            async fn new(
                _extensions: Arc<Extensions>,
            ) -> Result<Self, QueuePipelineComponentInitError> {
                Ok(Self)
            }
        }
    };
}

middleware_impl!(ServiceFirst);
middleware_impl!(ServiceSecond);
middleware_impl!(HandlerFirst);
middleware_impl!(HandlerSecond);

macro_rules! guard_impl {
    ($ty:ty) => {
        #[async_trait]
        impl QueueGuard for $ty {
            async fn new(
                _extensions: Arc<Extensions>,
            ) -> Result<Self, QueuePipelineComponentInitError> {
                Ok(Self)
            }

            async fn can_activate(
                &self,
                _exchange: &mut QueueDeliveryExchange<'_>,
            ) -> Result<(), QueueHandlerError> {
                Ok(())
            }
        }
    };
}

guard_impl!(ServiceGuard);
guard_impl!(ServiceGuardSecond);
guard_impl!(HandlerGuard);
guard_impl!(HandlerGuardSecond);

struct AuditService;

#[queue_service]
#[middleware(ServiceFirst)]
#[queue_runtime::middleware(ServiceSecond)]
#[guard(ServiceGuard)]
#[queue_runtime::guard(ServiceGuardSecond)]
impl AuditService {
    #[queue("audit.pipeline", version = 1, content = "json")]
    #[middleware(HandlerFirst)]
    #[queue_runtime::middleware(HandlerSecond)]
    #[queue_runtime::guard(HandlerGuard)]
    #[guard(HandlerGuardSecond)]
    async fn handle(&self) -> Result<(), QueueHandlerError> {
        Ok(())
    }
}

fn main() {
    let metadata = queue_runtime::__private::get_all_queue_handlers()
        .into_iter()
        .find(|metadata| metadata.queue_name == "audit.pipeline")
        .expect("pipeline metadata");

    assert_eq!(metadata.service_middlewares.len(), 2);
    assert!(metadata.service_middlewares[0]
        .type_name()
        .ends_with("ServiceFirst"));
    assert!(metadata.service_middlewares[1]
        .type_name()
        .ends_with("ServiceSecond"));
    assert_eq!(metadata.service_guards.len(), 2);
    assert!(metadata.service_guards[0]
        .type_name()
        .ends_with("ServiceGuard"));
    assert!(metadata.service_guards[1]
        .type_name()
        .ends_with("ServiceGuardSecond"));
    assert_eq!(metadata.handler_middlewares.len(), 2);
    assert!(metadata.handler_middlewares[0]
        .type_name()
        .ends_with("HandlerFirst"));
    assert!(metadata.handler_middlewares[1]
        .type_name()
        .ends_with("HandlerSecond"));
    assert_eq!(metadata.handler_guards.len(), 2);
    assert!(metadata.handler_guards[0]
        .type_name()
        .ends_with("HandlerGuard"));
    assert!(metadata.handler_guards[1]
        .type_name()
        .ends_with("HandlerGuardSecond"));
}
