#![allow(dead_code, unused_imports)]
mod mongodb_contract {
    use lily::mongodb as runtime;
    include!("../../../component_facades/mongodb.rs");
}
mod postgresql_contract {
    use lily::postgresql as runtime;
    include!("../../../component_facades/postgresql.rs");
}
mod clickhouse_contract {
    use lily::clickhouse as runtime;
    include!("../../../component_facades/clickhouse.rs");
}
pub use lily::trace as runtime;
include!("../../trace.rs");

#[test]
fn public_component_modules_preserve_their_types() {
    assert_eq!(
        std::any::TypeId::of::<lily::cancellation::ExecutionCancellation>(),
        std::any::TypeId::of::<lily::background_service::ExecutionCancellation>(),
    );
    let _ = lily::websocket::WsAppBuilder::new("127.0.0.1:0")
        .backplane::<lily::websocket_redis::RedisWebSocketBackplane>(
        lily::websocket::BackplaneRequirement::Required,
    );
    let _ = std::any::TypeId::of::<lily::config::ConfigService>();
    let _ = std::any::TypeId::of::<lily::http_client::HttpClient>();
    let _ = std::any::TypeId::of::<lily::background_service::BackgroundServices>();
    let _ = std::any::TypeId::of::<lily::error::injection::InjectionError>();
    let _ = std::any::TypeId::of::<lily::redis::CacheService>();
    let _ = std::any::TypeId::of::<lily::queue_client::QueueClientService>();
    let _ = std::any::TypeId::of::<lily::websocket_client::WebSocketClientService>();
    #[cfg(feature = "factory")]
    {
        let _ = std::any::TypeId::of::<lily::redis::CacheFactory>();
        let _ = std::any::TypeId::of::<lily::queue_client::QueueClientFactory>();
        let _ = std::any::TypeId::of::<lily::websocket_client::WebSocketClientFactory>();
    }
}

mod queue_contract {
    use lily::queue::{QueueHandlerError, TextPayload};
    struct Handler;
    #[lily::queue::queue_service]
    impl Handler {
        #[lily::queue::queue("umbrella.events", version = 3, content = "text")]
        async fn process(&self, _payload: TextPayload) -> Result<(), QueueHandlerError> {
            Ok(())
        }
    }
    #[test]
    fn nested_markers_register_once_in_the_component_registry() {
        let handlers = lily::queue::__private::get_all_queue_handlers();
        let matches: Vec<_> = handlers
            .iter()
            .filter(|h| h.queue_name == "umbrella.events")
            .collect();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].schema_version, 3);
        assert_eq!(matches[0].method_name, "process");
    }
    #[cfg(feature = "asyncapi")]
    mod documented {
        use super::*;
        struct Documented;
        #[lily::queue::queue_service]
        #[lily::queue::asyncapi(documented)]
        impl Documented {
            #[lily::queue::queue("umbrella.documented", version = 2, content = "text")]
            #[lily::queue::asyncapi(summary = "Documented umbrella event")]
            async fn handle(&self, _payload: TextPayload) -> Result<(), QueueHandlerError> {
                Ok(())
            }
        }
        #[test]
        fn nested_asyncapi_markers_reach_the_runtime_metadata() {
            let handlers = lily::queue::__private::get_all_queue_handlers();
            let matches: Vec<_> = handlers
                .iter()
                .filter(|h| h.queue_name == "umbrella.documented")
                .collect();
            assert_eq!(matches.len(), 1);
            assert!(matches!(
                matches[0].asyncapi.status,
                lily::queue::__private::QueueAsyncApiStatus::Documented
            ));
            assert!(matches!(
                matches[0].asyncapi.payload,
                lily::queue::__private::QueueAsyncApiPayload::Text { .. }
            ));
        }
    }
}
