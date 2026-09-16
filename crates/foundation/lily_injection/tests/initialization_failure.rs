use lily_error::injection::InjectionError;
use lily_injectable_derive::Injectable;
use lily_injection::{ApplicationContainer, ServiceTrait, async_trait::async_trait};

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
struct FailingService;

#[async_trait]
impl ServiceTrait for FailingService {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        Err(InjectionError::InitError(
            "intentional startup failure".to_string(),
        ))
    }
}

#[tokio::test]
async fn initialization_failure_aborts_container_build() {
    let error = ApplicationContainer::build().await.unwrap_err();

    match error {
        InjectionError::ServiceInitializationFailed { service, source } => {
            assert!(service.contains("FailingService"));
            assert_eq!(
                *source,
                InjectionError::InitError("intentional startup failure".to_string())
            );
        }
        other => panic!("unexpected error: {other}"),
    }
}
