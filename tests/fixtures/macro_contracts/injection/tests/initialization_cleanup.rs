use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use lily_error::injection::InjectionError;
use lily_injectable_derive::Injectable;
use lily_injection::{async_trait::async_trait, ApplicationContainer, ServiceTrait};

static SUCCESSFUL_CLEANUPS: AtomicUsize = AtomicUsize::new(0);
static FAILED_CLEANUPS: AtomicUsize = AtomicUsize::new(0);

#[derive(Default, Injectable)]
#[service(disabled)]
struct PartialInitialization;

#[async_trait]
impl ServiceTrait for PartialInitialization {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        Err(InjectionError::InitError(
            "initialization failed".to_string(),
        ))
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        SUCCESSFUL_CLEANUPS.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Default, Injectable)]
#[service(enabled = false)]
struct PartialInitializationWithFailedCleanup;

#[async_trait]
impl ServiceTrait for PartialInitializationWithFailedCleanup {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        Err(InjectionError::InitError(
            "initialization failed".to_string(),
        ))
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        FAILED_CLEANUPS.fetch_add(1, Ordering::SeqCst);
        Err(InjectionError::DisposeError("cleanup failed".to_string()))
    }
}

async fn provider() -> Arc<dyn std::any::Any + Send + Sync> {
    ApplicationContainer::build()
        .await
        .expect("empty application container must build")
        .services()
}

#[tokio::test]
async fn initialization_failure_runs_best_effort_cleanup() {
    SUCCESSFUL_CLEANUPS.store(0, Ordering::SeqCst);

    let error = match create_partialinitialization(provider().await).await {
        Ok(_) => panic!("failing initialization unexpectedly created a service"),
        Err(error) => error,
    };

    assert_eq!(SUCCESSFUL_CLEANUPS.load(Ordering::SeqCst), 1);
    assert!(matches!(
        error,
        InjectionError::ServiceInitializationFailed { source, .. }
            if matches!(*source, InjectionError::InitError(ref message) if message == "initialization failed")
    ));
}

#[tokio::test]
async fn initialization_and_cleanup_errors_are_both_preserved() {
    FAILED_CLEANUPS.store(0, Ordering::SeqCst);

    let error = match create_partialinitializationwithfailedcleanup(provider().await).await {
        Ok(_) => panic!("failing initialization unexpectedly created a service"),
        Err(error) => error,
    };

    assert_eq!(FAILED_CLEANUPS.load(Ordering::SeqCst), 1);
    let InjectionError::ServiceInitializationFailed { source, .. } = error else {
        panic!("unexpected error shape")
    };
    let InjectionError::InitializationCleanupFailed {
        initialization,
        cleanup,
        ..
    } = *source
    else {
        panic!("initialization and cleanup failures were not both preserved")
    };
    assert!(matches!(*initialization, InjectionError::InitError(_)));
    assert!(matches!(*cleanup, InjectionError::DisposeError(_)));
}
