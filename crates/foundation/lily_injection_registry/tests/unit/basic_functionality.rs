//! Basic functionality tests for core components

use crate::analyze_dependencies;
use lily_injection_registry::*;
use std::any::TypeId;
use std::sync::Arc;

#[tokio::test]
async fn test_service_metadata_creation() {
    let metadata = ServiceMetadata {
        type_id: TypeId::of::<String>(),
        type_name: "TestService",
        trait_type_id: None,
        trait_name: None,
        lifetime: ServiceLifetime::Singleton,
        factory_fn: |_| {
            Box::pin(async {
                Ok(Box::new(String::from("test")) as Box<dyn std::any::Any + Send + Sync>)
            })
        },
        dependencies: vec![],
    };

    assert_eq!(metadata.type_name, "TestService");
    assert_eq!(metadata.lifetime, ServiceLifetime::Singleton);
    assert_eq!(metadata.type_id, TypeId::of::<String>());
    assert!(metadata.dependencies.is_empty());
}

#[tokio::test]
async fn test_service_lifetime_enum() {
    assert_eq!(ServiceLifetime::Singleton, ServiceLifetime::Singleton);
    assert_ne!(ServiceLifetime::Singleton, ServiceLifetime::Scoped);
    assert_ne!(ServiceLifetime::Scoped, ServiceLifetime::Transient);
}

#[tokio::test]
async fn test_factory_function_execution() {
    let metadata = ServiceMetadata {
        type_id: TypeId::of::<i32>(),
        type_name: "i32",
        trait_type_id: None,
        trait_name: None,
        lifetime: ServiceLifetime::Transient,
        factory_fn: |_| {
            Box::pin(async { Ok(Box::new(42i32) as Box<dyn std::any::Any + Send + Sync>) })
        },
        dependencies: vec![],
    };

    let extensions = Arc::new(()) as Arc<dyn std::any::Any + Send + Sync>;
    let result = (metadata.factory_fn)(extensions).await;

    assert!(result.is_ok());
    let service = result.unwrap();
    let value = service.downcast_ref::<i32>().unwrap();
    assert_eq!(*value, 42);
}

#[test]
fn test_get_all_service_metadata_empty() {
    // Should return empty or existing services depending on test environment
    let metadata = get_all_service_metadata();
    // Just verify it doesn't panic and returns a Vec
    // assert!(metadata.len() >= 0);
}

#[test]
fn test_analyze_dependencies_empty() {
    // With no singleton services, should return empty Vec
    let result = analyze_dependencies();
    // Should not error on empty dependency graph
    assert!(result.is_ok());
}
