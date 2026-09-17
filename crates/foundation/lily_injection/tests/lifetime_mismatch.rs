use lily_error::injection::InjectionError;
use lily_injectable_derive::Injectable;
use lily_injection::{ApplicationContainer, ServiceTrait, async_trait::async_trait};
use std::sync::Arc;

#[derive(Default, Injectable)]
#[service(lifetime = "Scoped")]
struct ScopedDependency;

#[async_trait]
impl ServiceTrait for ScopedDependency {}

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
#[allow(dead_code)]
struct SingletonWithScopedDependency {
    #[inject]
    dependency: Arc<ScopedDependency>,
}

#[async_trait]
impl ServiceTrait for SingletonWithScopedDependency {}

#[tokio::test]
async fn captive_dependency_aborts_container_build() {
    let error = ApplicationContainer::build().await.unwrap_err();

    assert!(matches!(
        error,
        InjectionError::LifetimeMismatch {
            service,
            service_lifetime,
            dependency,
            dependency_lifetime,
        } if service.contains("SingletonWithScopedDependency")
            && service_lifetime == "Singleton"
            && dependency.contains("ScopedDependency")
            && dependency_lifetime == "Scoped"
    ));
}
