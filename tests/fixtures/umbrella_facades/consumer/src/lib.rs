#![allow(dead_code, unused_imports)]
pub use lilyrs::__private::lily_injection as runtime;
pub use lilyrs::consumer as provider;
pub use provider::async_trait::async_trait as lifecycle;
#[path = "../../../di_facades/contract.rs"]
mod contract;

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
