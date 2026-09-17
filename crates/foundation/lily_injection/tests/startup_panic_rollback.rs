use lily_error::injection::InjectionError;
use lily_injectable_derive::Injectable;
use lily_injection::{ApplicationContainer, ServiceTrait, async_trait::async_trait};
use std::sync::{Arc, Mutex};

static EVENTS: Mutex<Vec<&'static str>> = Mutex::new(Vec::new());

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
struct HealthyDependency;

#[async_trait]
impl ServiceTrait for HealthyDependency {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        EVENTS.lock().unwrap().push("healthy:start");
        Ok(())
    }
    async fn dispose(&self) -> Result<(), InjectionError> {
        EVENTS.lock().unwrap().push("healthy:dispose");
        Ok(())
    }
}

#[derive(Injectable)]
#[service(lifetime = "Singleton")]
struct PanickingApplication {
    #[inject]
    _dependency: Arc<HealthyDependency>,
}

#[async_trait]
impl ServiceTrait for PanickingApplication {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        EVENTS.lock().unwrap().push("panic:start");
        panic!("intentional startup panic");
    }
    async fn dispose(&self) -> Result<(), InjectionError> {
        EVENTS.lock().unwrap().push("panic:partial-dispose");
        Ok(())
    }
}

#[tokio::test]
async fn startup_panic_is_typed_and_does_not_skip_partial_cleanup_or_rollback() {
    EVENTS.lock().unwrap().clear();
    let error = ApplicationContainer::build().await.unwrap_err();
    let InjectionError::ServiceInitializationFailed { source, .. } = error else {
        panic!("startup panic was not returned as a typed initialization failure")
    };
    assert!(matches!(
        source.as_ref(),
        InjectionError::InitError(message) if message.contains("initialization panicked")
    ));
    assert_eq!(
        *EVENTS.lock().unwrap(),
        vec![
            "healthy:start",
            "panic:start",
            "panic:partial-dispose",
            "healthy:dispose",
        ]
    );
}
