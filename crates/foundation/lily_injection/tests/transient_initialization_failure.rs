use lily_error::injection::InjectionError;
use lily_injectable_derive::Injectable;
use lily_injection::{ApplicationContainer, ServiceTrait, async_trait::async_trait};
use std::any::TypeId;
use std::sync::Arc;

#[derive(Debug, Default, Injectable)]
#[service(lifetime = "Transient")]
struct FailingTransientService;

#[async_trait]
impl ServiceTrait for FailingTransientService {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        Err(InjectionError::InitError(
            "transient startup failure".to_string(),
        ))
    }
}

#[derive(Debug, Default, Injectable)]
#[service(lifetime = "Transient")]
#[allow(dead_code)]
struct ParentTransientService {
    #[inject]
    dependency: Arc<FailingTransientService>,
}

#[async_trait]
impl ServiceTrait for ParentTransientService {}

#[tokio::test]
async fn transient_initialization_failure_is_returned_with_service_context() {
    let container = ApplicationContainer::build().await.unwrap();
    let error = container
        .resolve::<FailingTransientService>(None)
        .await
        .unwrap_err();

    match error {
        InjectionError::ServiceInitializationFailed { service, source } => {
            assert!(service.contains("FailingTransientService"));
            assert_eq!(
                *source,
                InjectionError::InitError("transient startup failure".to_string())
            );
        }
        other => panic!("unexpected error: {other}"),
    }
}

#[tokio::test]
async fn dynamic_resolution_preserves_the_same_initialization_source_chain() {
    let container = ApplicationContainer::build().await.unwrap();
    let error = container
        .resolve_by_type_id(TypeId::of::<FailingTransientService>(), None)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        InjectionError::ServiceInitializationFailed { service, source }
            if service.contains("FailingTransientService")
                && matches!(*source, InjectionError::InitError(ref message) if message == "transient startup failure")
    ));
}

#[tokio::test]
async fn dependency_initialization_failure_preserves_the_full_source_chain() {
    let container = ApplicationContainer::build().await.unwrap();
    let error = container
        .resolve::<ParentTransientService>(None)
        .await
        .unwrap_err();

    match error {
        InjectionError::DependencyResolutionFailed {
            service,
            dependency,
            source,
        } => {
            assert!(service.contains("ParentTransientService"));
            assert!(dependency.contains("FailingTransientService"));
            assert!(matches!(
                *source,
                InjectionError::ServiceInitializationFailed { .. }
            ));
        }
        other => panic!("unexpected error: {other}"),
    }
}
