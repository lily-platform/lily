#![allow(dead_code, unused_imports)]
pub use platform::__private::lily_injection as runtime;
pub use platform::consumer as provider;
pub use provider::async_trait::async_trait as lifecycle;
#[path = "../../../di_facades/contract.rs"]
mod contract;

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
