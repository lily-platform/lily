#![allow(dead_code, unused_imports)]
mod mongodb_contract {
    use lilyrs::mongodb as runtime;
    include!("../../../component_facades/mongodb.rs");
}
mod postgresql_contract {
    use lilyrs::postgresql as runtime;
    include!("../../../component_facades/postgresql.rs");
}
mod clickhouse_contract {
    use lilyrs::clickhouse as runtime;
    include!("../../../component_facades/clickhouse.rs");
}
pub use lilyrs::trace as runtime;
include!("../../trace.rs");

#[test]
fn public_component_modules_preserve_their_types() {
    assert_eq!(
        std::any::TypeId::of::<lilyrs::cancellation::ExecutionCancellation>(),
        std::any::TypeId::of::<lilyrs::background_service::ExecutionCancellation>(),
    );
    let _ = lilyrs::websocket::WsAppBuilder::new("127.0.0.1:0")
        .backplane::<lilyrs::websocket_redis::RedisWebSocketBackplane>(
        lilyrs::websocket::BackplaneRequirement::Required,
    );
    let _ = std::any::TypeId::of::<lilyrs::config::ConfigService>();
    let _ = std::any::TypeId::of::<lilyrs::http_client::HttpClient>();
    let _ = std::any::TypeId::of::<lilyrs::background_service::BackgroundServices>();
    let _ = std::any::TypeId::of::<lilyrs::error::injection::InjectionError>();
    let _ = std::any::TypeId::of::<lilyrs::redis::CacheService>();
    let _ = std::any::TypeId::of::<lilyrs::queue_client::QueueClientService>();
    let _ = std::any::TypeId::of::<lilyrs::websocket_client::WebSocketClientService>();
    #[cfg(feature = "factory")]
    {
        let _ = std::any::TypeId::of::<lilyrs::redis::CacheFactory>();
        let _ = std::any::TypeId::of::<lilyrs::queue_client::QueueClientFactory>();
        let _ = std::any::TypeId::of::<lilyrs::websocket_client::WebSocketClientFactory>();
    }
}

mod queue_contract {
    use lilyrs::queue::{QueueHandlerError, TextPayload};
    struct Handler;
    #[lilyrs::queue::queue_service]
    impl Handler {
        #[lilyrs::queue::queue("umbrella.events", version = 3, content = "text")]
        async fn process(&self, _payload: TextPayload) -> Result<(), QueueHandlerError> {
            Ok(())
        }
    }
    #[test]
    fn nested_markers_register_once_in_the_component_registry() {
        let handlers = lilyrs::queue::__private::get_all_queue_handlers();
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
        #[lilyrs::queue::queue_service]
        #[lilyrs::queue::asyncapi(documented)]
        impl Documented {
            #[lilyrs::queue::queue("umbrella.documented", version = 2, content = "text")]
            #[lilyrs::queue::asyncapi(summary = "Documented umbrella event")]
            async fn handle(&self, _payload: TextPayload) -> Result<(), QueueHandlerError> {
                Ok(())
            }
        }
        #[test]
        fn nested_asyncapi_markers_reach_the_runtime_metadata() {
            let handlers = lilyrs::queue::__private::get_all_queue_handlers();
            let matches: Vec<_> = handlers
                .iter()
                .filter(|h| h.queue_name == "umbrella.documented")
                .collect();
            assert_eq!(matches.len(), 1);
            assert!(matches!(
                matches[0].asyncapi.status,
                lilyrs::queue::__private::QueueAsyncApiStatus::Documented
            ));
            assert!(matches!(
                matches[0].asyncapi.payload,
                lilyrs::queue::__private::QueueAsyncApiPayload::Text { .. }
            ));
        }
    }
}
