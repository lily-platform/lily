use lily_error::injection::InjectionError;
use lily_injectable_derive::Injectable;
use lily_injection::{ApplicationContainer, ServiceTrait, async_trait::async_trait};
use std::sync::Arc;

#[derive(Default, Injectable)]
#[service(lifetime = "Scoped")]
#[allow(dead_code)]
struct FirstScopedService {
    #[inject]
    second: Arc<SecondScopedService>,
}

#[async_trait]
impl ServiceTrait for FirstScopedService {}

#[derive(Default, Injectable)]
#[service(lifetime = "Scoped")]
#[allow(dead_code)]
struct SecondScopedService {
    #[inject]
    first: Arc<FirstScopedService>,
}

#[async_trait]
impl ServiceTrait for SecondScopedService {}

#[tokio::test]
async fn scoped_cycle_aborts_build_before_resolution() {
    let error = ApplicationContainer::build().await.unwrap_err();

    match error {
        InjectionError::CircularDependency { cycle } => {
            assert_eq!(cycle.first(), cycle.last());
            assert!(cycle.iter().any(|name| name.contains("FirstScopedService")));
            assert!(
                cycle
                    .iter()
                    .any(|name| name.contains("SecondScopedService"))
            );
        }
        other => panic!("unexpected error: {other}"),
    }
}
