//! Error handling tests for lily_injection_registry

use crate::{analyze_dependencies, lifetime_utils::*};
use lily_injection_registry::*;

#[test]
fn test_lifetime_validation_error_messages() {
    // Test Singleton -> Scoped error
    let result =
        validate_lifetime_compatibility(ServiceLifetime::Singleton, ServiceLifetime::Scoped);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.contains("Singleton"));
    assert!(err.contains("Scoped"));
    assert!(err.contains("cannot depend"));

    // A transient constructed for a singleton is owned by the root ledger.
    assert!(validate_lifetime_compatibility(
        ServiceLifetime::Singleton,
        ServiceLifetime::Transient
    )
    .is_ok());

    // Scoped -> Transient is safe because the active scope owns the transient.
    assert!(
        validate_lifetime_compatibility(ServiceLifetime::Scoped, ServiceLifetime::Transient)
            .is_ok()
    );
}

#[tokio::test]
async fn test_factory_function_error_handling() {
    use lily_error::injection::InjectionError;
    use std::any::TypeId;
    use std::sync::Arc;

    let metadata = ServiceMetadata {
        type_id: TypeId::of::<String>(),
        type_name: "FailingService",
        trait_type_id: None,
        trait_name: None,
        lifetime: ServiceLifetime::Transient,
        factory_fn: |_| Box::pin(async { Err(InjectionError::General("Test error".to_string())) }),
        dependencies: vec![],
    };

    let extensions = Arc::new(()) as Arc<dyn std::any::Any + Send + Sync>;
    let result = (metadata.factory_fn)(extensions).await;

    assert!(result.is_err());
    match result {
        Err(InjectionError::General(msg)) => {
            assert_eq!(msg, "Test error");
        }
        _ => panic!("Expected General error"),
    }
}

#[test]
fn test_analyze_dependencies_with_empty_services() {
    // Should handle empty service list gracefully
    let result = analyze_dependencies();
    assert!(result.is_ok());
    let order = result.unwrap();
    // Empty or existing services depending on environment
    // assert!(order.len() >= 0);
}

#[test]
fn test_lifetime_validation_suggestions() {
    // Verify error messages provide helpful suggestions
    let result =
        validate_lifetime_compatibility(ServiceLifetime::Singleton, ServiceLifetime::Scoped);

    if let Err(msg) = result {
        // Should suggest factory pattern or changing lifetime
        assert!(msg.contains("factory pattern") || msg.contains("make dependency Singleton"));
    }
}

#[tokio::test]
async fn test_service_metadata_invalid_downcast() {
    use std::any::TypeId;
    use std::sync::Arc;

    let metadata = ServiceMetadata {
        type_id: TypeId::of::<String>(),
        type_name: "StringService",
        trait_type_id: None,
        trait_name: None,
        lifetime: ServiceLifetime::Transient,
        factory_fn: |_| {
            Box::pin(async {
                Ok(Box::new(String::from("test")) as Box<dyn std::any::Any + Send + Sync>)
            })
        },
        dependencies: vec![],
    };

    let extensions = Arc::new(()) as Arc<dyn std::any::Any + Send + Sync>;
    let result = (metadata.factory_fn)(extensions).await;

    assert!(result.is_ok());
    let service = result.unwrap();

    // Try to downcast to wrong type
    let wrong_type = service.downcast_ref::<i32>();
    assert!(wrong_type.is_none());

    // Correct downcast should work
    let correct_type = service.downcast_ref::<String>();
    assert!(correct_type.is_some());
}
