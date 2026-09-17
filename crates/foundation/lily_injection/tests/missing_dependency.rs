use lily_error::injection::InjectionError;
use lily_injectable_derive::Injectable;
use lily_injection::{ApplicationContainer, ServiceTrait, async_trait::async_trait};
use std::any::TypeId;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

static START_COUNT: AtomicUsize = AtomicUsize::new(0);

#[derive(Default)]
struct UnregisteredDependency;

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
struct IndependentStartupService;

#[async_trait]
impl ServiceTrait for IndependentStartupService {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        START_COUNT.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
#[allow(dead_code)]
struct ServiceWithMissingDependency {
    #[inject]
    dependency: Arc<UnregisteredDependency>,
}

#[async_trait]
impl ServiceTrait for ServiceWithMissingDependency {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        Ok(())
    }
}

#[tokio::test]
async fn missing_dependency_aborts_build_before_factory_execution() {
    let error = ApplicationContainer::build().await.unwrap_err();

    assert!(matches!(
        error,
        InjectionError::MissingDependency {
            service,
            dependency_type_id,
        } if service.contains("ServiceWithMissingDependency")
            && dependency_type_id == TypeId::of::<UnregisteredDependency>()
    ));
    assert_eq!(START_COUNT.load(Ordering::SeqCst), 0);
}
