use lily_error::injection::InjectionError;
use lily_injectable_derive::Injectable;
use lily_injection::{ApplicationContainer, ServiceTrait, async_trait::async_trait};
use std::sync::{Arc, Mutex};

static EVENTS: Mutex<Vec<&'static str>> = Mutex::new(Vec::new());

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
struct FailingRollbackDependency;

#[async_trait]
impl ServiceTrait for FailingRollbackDependency {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        EVENTS.lock().unwrap().push("dependency:start");
        Ok(())
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        EVENTS.lock().unwrap().push("dependency:dispose-error");
        Err(InjectionError::DisposeError(
            "intentional rollback failure".to_string(),
        ))
    }
}

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
struct HealthyRollbackOwner {
    #[inject]
    _dependency: Arc<FailingRollbackDependency>,
}

#[async_trait]
impl ServiceTrait for HealthyRollbackOwner {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        EVENTS.lock().unwrap().push("owner:start");
        Ok(())
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        EVENTS.lock().unwrap().push("owner:dispose");
        Ok(())
    }
}

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
struct StartupFailure {
    #[inject]
    _owner: Arc<HealthyRollbackOwner>,
}

#[async_trait]
impl ServiceTrait for StartupFailure {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        EVENTS.lock().unwrap().push("failure:start");
        Err(InjectionError::InitError(
            "intentional startup failure".to_string(),
        ))
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        EVENTS.lock().unwrap().push("failure:partial-dispose");
        Ok(())
    }
}

#[tokio::test]
async fn rollback_failure_is_aggregated_without_skipping_remaining_disposers() {
    EVENTS.lock().unwrap().clear();
    let error = ApplicationContainer::build().await.unwrap_err();
    let InjectionError::StartupRollbackFailed {
        startup,
        rollback_errors,
        rollback_outcomes,
        rollback_remaining,
    } = error
    else {
        panic!("expected startup plus rollback aggregate")
    };

    assert!(matches!(
        *startup,
        InjectionError::ServiceInitializationFailed { .. }
    ));
    assert!(
        rollback_errors
            .iter()
            .any(|error| error.contains("intentional rollback failure"))
    );
    assert!(rollback_outcomes.iter().any(|outcome| {
        outcome
            .detail
            .as_deref()
            .is_some_and(|detail| detail.contains("intentional rollback failure"))
    }));
    assert!(rollback_remaining.is_some());
    assert_eq!(
        *EVENTS.lock().unwrap(),
        vec![
            "dependency:start",
            "owner:start",
            "failure:start",
            "failure:partial-dispose",
            "owner:dispose",
            "dependency:dispose-error",
        ]
    );
}
