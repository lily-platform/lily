use runtime::{Json, QueueHandlerError, queue, queue_service};
use serde::Deserialize;

#[derive(Deserialize)]
struct Event {
    message: String,
}
struct Handler;

#[queue_service]
impl Handler {
    #[queue("facade.events", version = 1, content = "json")]
    async fn process(&self, Json(event): Json<Event>) -> Result<(), QueueHandlerError> {
        let _ = event.message;
        Ok(())
    }
}

#[test]
fn queue_macros_register_with_the_same_runtime_registry() {
    let handlers = runtime::__private::get_all_queue_handlers();
    let matches: Vec<_> = handlers
        .iter()
        .filter(|h| h.queue_name == "facade.events")
        .collect();
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0].schema_version, 1);
}

#[cfg(feature = "asyncapi")]
mod documented {
    use runtime::{QueueHandlerError, TextPayload, asyncapi, queue, queue_service};
    struct DocumentedHandler;

    #[queue_service]
    #[asyncapi(documented)]
    impl DocumentedHandler {
        #[queue("facade.documented", version = 2, content = "text")]
        async fn process(&self, _payload: TextPayload) -> Result<(), QueueHandlerError> {
            Ok(())
        }
    }

    #[test]
    fn optional_asyncapi_uses_the_facades_registry_and_schema_contracts() {
        let handlers = runtime::__private::get_all_queue_handlers();
        let handler = handlers
            .iter()
            .find(|h| h.queue_name == "facade.documented")
            .unwrap();
        assert_eq!(handler.schema_version, 2);
        assert!(matches!(
            handler.asyncapi.status,
            runtime::__private::QueueAsyncApiStatus::Documented
        ));
        assert!(matches!(
            handler.asyncapi.payload,
            runtime::__private::QueueAsyncApiPayload::Text { .. }
        ));
    }
}
