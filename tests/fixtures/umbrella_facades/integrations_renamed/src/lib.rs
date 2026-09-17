#![allow(dead_code, unused_imports)]
mod mongodb_contract {
    use platform::mongodb as runtime;
    include!("../../../component_facades/mongodb.rs");
}
mod postgresql_contract {
    use platform::postgresql as runtime;
    include!("../../../component_facades/postgresql.rs");
}
mod clickhouse_contract {
    use platform::clickhouse as runtime;
    include!("../../../component_facades/clickhouse.rs");
}
pub use platform::trace as runtime;
include!("../../trace.rs");

#[test]
fn public_component_modules_preserve_their_types() {
    assert_eq!(
        std::any::TypeId::of::<platform::cancellation::ExecutionCancellation>(),
        std::any::TypeId::of::<platform::background_service::ExecutionCancellation>(),
    );
    let _ = platform::websocket::WsAppBuilder::new("127.0.0.1:0")
        .backplane::<platform::websocket_redis::RedisWebSocketBackplane>(
        platform::websocket::BackplaneRequirement::Required,
    );
    let _ = std::any::TypeId::of::<platform::config::ConfigService>();
    let _ = std::any::TypeId::of::<platform::http_client::HttpClient>();
    let _ = std::any::TypeId::of::<platform::background_service::BackgroundServices>();
    let _ = std::any::TypeId::of::<platform::error::injection::InjectionError>();
    let _ = std::any::TypeId::of::<platform::redis::CacheService>();
    let _ = std::any::TypeId::of::<platform::queue_client::QueueClientService>();
    let _ = std::any::TypeId::of::<platform::websocket_client::WebSocketClientService>();
    #[cfg(feature = "factory")]
    {
        let _ = std::any::TypeId::of::<platform::redis::CacheFactory>();
        let _ = std::any::TypeId::of::<platform::queue_client::QueueClientFactory>();
        let _ = std::any::TypeId::of::<platform::websocket_client::WebSocketClientFactory>();
    }
}

mod queue_contract {
    use platform::queue::{QueueHandlerError, TextPayload};
    struct Handler;
    #[platform::queue::queue_service]
    impl Handler {
        #[platform::queue::queue("umbrella.events", version = 3, content = "text")]
        async fn process(&self, _payload: TextPayload) -> Result<(), QueueHandlerError> {
            Ok(())
        }
    }
    #[test]
    fn nested_markers_register_once_in_the_component_registry() {
        let handlers = platform::queue::__private::get_all_queue_handlers();
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
        #[platform::queue::queue_service]
        #[platform::queue::asyncapi(documented)]
        impl Documented {
            #[platform::queue::queue("umbrella.documented", version = 2, content = "text")]
            #[platform::queue::asyncapi(summary = "Documented umbrella event")]
            async fn handle(&self, _payload: TextPayload) -> Result<(), QueueHandlerError> {
                Ok(())
            }
        }
        #[test]
        fn nested_asyncapi_markers_reach_the_runtime_metadata() {
            let handlers = platform::queue::__private::get_all_queue_handlers();
            let matches: Vec<_> = handlers
                .iter()
                .filter(|h| h.queue_name == "umbrella.documented")
                .collect();
            assert_eq!(matches.len(), 1);
            assert!(matches!(
                matches[0].asyncapi.status,
                platform::queue::__private::QueueAsyncApiStatus::Documented
            ));
            assert!(matches!(
                matches[0].asyncapi.payload,
                platform::queue::__private::QueueAsyncApiPayload::Text { .. }
            ));
        }
    }
}
