//! Tests for integration with external dependencies

use lily_error::injection::InjectionError;
use lily_injection_registry::*;
use std::any::TypeId;
use std::sync::Arc;

#[tokio::test]
async fn test_integration_with_lily_error() {
    // Test integration with lily_error crate

    let metadata = ServiceMetadata {
        type_id: TypeId::of::<String>(),
        type_name: "ErrorTestService",
        trait_type_id: None,
        trait_name: None,
        lifetime: ServiceLifetime::Singleton,
        factory_fn: |_| {
            Box::pin(async { Err(InjectionError::ServiceNotFound("TestService".to_string())) })
        },
        dependencies: vec![],
    };

    let extensions = Arc::new(()) as Arc<dyn std::any::Any + Send + Sync>;
    let result = (metadata.factory_fn)(extensions).await;

    // Verify error integration
    assert!(result.is_err());
    match result {
        Err(InjectionError::ServiceNotFound(name)) => {
            assert_eq!(name, "TestService");
        }
        _ => panic!("Expected ServiceNotFound error"),
    }
}

#[tokio::test]
async fn test_integration_with_tokio() {
    // Test integration with tokio runtime
    use std::time::Duration;

    let metadata = ServiceMetadata {
        type_id: TypeId::of::<String>(),
        type_name: "TokioService",
        trait_type_id: None,
        trait_name: None,
        lifetime: ServiceLifetime::Singleton,
        factory_fn: |_| {
            Box::pin(async {
                // Use tokio delay
                tokio::time::sleep(Duration::from_millis(10)).await;
                Ok(Box::new("delayed service".to_string()) as Box<dyn std::any::Any + Send + Sync>)
            })
        },
        dependencies: vec![],
    };

    let extensions = Arc::new(()) as Arc<dyn std::any::Any + Send + Sync>;
    let start = std::time::Instant::now();
    let result = (metadata.factory_fn)(extensions).await;

    // Verify tokio integration worked
    assert!(result.is_ok());
    assert!(start.elapsed() >= Duration::from_millis(10));

    let service = result.unwrap();
    let value = service.downcast_ref::<String>().unwrap();
    assert_eq!(value, "delayed service");
}

#[tokio::test]
async fn test_integration_with_linkme() {
    // Test that linkme is properly exported and can be used
    use lily_injection_registry::linkme;

    // This is mostly a compile-time check to ensure linkme is properly re-exported

    // Try to define a distributed slice to ensure the macro is available
    #[linkme::distributed_slice]
    static TEST_SLICE: [fn() -> &'static str] = [..];

    // Define a function that would contribute to the slice
    #[linkme::distributed_slice(TEST_SLICE)]
    static TEST_FUNCTION: fn() -> &'static str = || "test_value";

    // This test mainly checks that the linkme import works
    assert_eq!(1, 1);
}

#[tokio::test]
async fn test_async_trait_integration() {
    use async_trait::async_trait;

    // Define a trait using async_trait
    #[async_trait]
    trait AsyncService {
        async fn perform_async_operation(&self) -> Result<String, InjectionError>;
    }

    // Implement for a test struct
    struct TestAsyncService;

    #[async_trait]
    impl AsyncService for TestAsyncService {
        async fn perform_async_operation(&self) -> Result<String, InjectionError> {
            Ok("async operation completed".to_string())
        }
    }

    // Create service that uses this trait
    let metadata = ServiceMetadata {
        type_id: TypeId::of::<TestAsyncService>(),
        type_name: "AsyncTraitService",
        trait_type_id: None,
        trait_name: None,
        lifetime: ServiceLifetime::Singleton,
        factory_fn: |_| {
            Box::pin(async {
                let service = TestAsyncService;
                Ok(Box::new(service) as Box<dyn std::any::Any + Send + Sync>)
            })
        },
        dependencies: vec![],
    };

    let extensions = Arc::new(()) as Arc<dyn std::any::Any + Send + Sync>;
    let result = (metadata.factory_fn)(extensions).await;

    // Verify async_trait integration worked
    assert!(result.is_ok());
}

#[test]
fn test_error_conversion_integration() {
    // Test integration between error types

    // Create standard error
    let std_err = std::io::Error::new(std::io::ErrorKind::NotFound, "Resource not found");

    // Convert to InjectionError
    let inj_err = InjectionError::General(std_err.to_string());

    // Verify error integration
    match inj_err {
        InjectionError::General(msg) => {
            assert!(msg.contains("Resource not found"));
        }
        _ => panic!("Expected General variant"),
    }
}
