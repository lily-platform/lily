use lily_error::injection::InjectionError;
use lily_injectable_derive::Injectable;
use lily_injection::{ApplicationContainer, ServiceTrait, async_trait::async_trait};
use std::sync::{Arc, Mutex};

static EVENTS: Mutex<Vec<&'static str>> = Mutex::new(Vec::new());

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
struct Database;

#[async_trait]
impl ServiceTrait for Database {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        EVENTS.lock().unwrap().push("database:start");
        Ok(())
    }
    async fn dispose(&self) -> Result<(), InjectionError> {
        EVENTS.lock().unwrap().push("database:dispose");
        Ok(())
    }
}

#[derive(Default, Injectable)]
#[service(lifetime = "Transient")]
struct StartupTransient;

#[async_trait]
impl ServiceTrait for StartupTransient {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        EVENTS.lock().unwrap().push("transient:start");
        Ok(())
    }
    async fn dispose(&self) -> Result<(), InjectionError> {
        EVENTS.lock().unwrap().push("transient:dispose");
        Ok(())
    }
}

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
struct Repository {
    #[inject]
    _database: Arc<Database>,
    #[inject]
    _transient: Arc<StartupTransient>,
}

#[async_trait]
impl ServiceTrait for Repository {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        EVENTS.lock().unwrap().push("repository:start");
        Ok(())
    }
    async fn dispose(&self) -> Result<(), InjectionError> {
        EVENTS.lock().unwrap().push("repository:dispose");
        Ok(())
    }
}

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
struct FailingApplication {
    #[inject]
    _repository: Arc<Repository>,
}

#[async_trait]
impl ServiceTrait for FailingApplication {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        EVENTS.lock().unwrap().push("application:start");
        Err(InjectionError::InitError("intentional failure".to_string()))
    }
    async fn dispose(&self) -> Result<(), InjectionError> {
        EVENTS.lock().unwrap().push("application:partial-dispose");
        Ok(())
    }
}

#[tokio::test]
async fn startup_failure_cleans_partial_service_and_rolls_back_reverse_order() {
    EVENTS.lock().unwrap().clear();
    assert!(matches!(
        ApplicationContainer::build().await,
        Err(InjectionError::ServiceInitializationFailed { .. })
    ));
    assert_eq!(
        *EVENTS.lock().unwrap(),
        vec![
            "database:start",
            "transient:start",
            "repository:start",
            "application:start",
            "application:partial-dispose",
            "repository:dispose",
            "transient:dispose",
            "database:dispose",
        ]
    );
}
