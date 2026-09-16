//! Edge case tests for lily_injection_registry

use crate::lifetime_utils::*;
use lily_injection_registry::*;
use std::any::TypeId;

#[test]
fn test_lifetime_validation_edge_cases() {
    // Test all valid combinations
    assert!(validate_lifetime_compatibility(
        ServiceLifetime::Singleton,
        ServiceLifetime::Singleton
    )
    .is_ok());

    assert!(
        validate_lifetime_compatibility(ServiceLifetime::Scoped, ServiceLifetime::Singleton)
            .is_ok()
    );

    assert!(
        validate_lifetime_compatibility(ServiceLifetime::Scoped, ServiceLifetime::Scoped).is_ok()
    );

    assert!(validate_lifetime_compatibility(
        ServiceLifetime::Transient,
        ServiceLifetime::Singleton
    )
    .is_ok());

    assert!(
        validate_lifetime_compatibility(ServiceLifetime::Transient, ServiceLifetime::Scoped)
            .is_ok()
    );

    assert!(validate_lifetime_compatibility(
        ServiceLifetime::Transient,
        ServiceLifetime::Transient
    )
    .is_ok());
}

#[test]
fn test_lifetime_validation_invalid_combinations() {
    // Singleton cannot capture request-owned state.
    assert!(
        validate_lifetime_compatibility(ServiceLifetime::Singleton, ServiceLifetime::Scoped)
            .is_err()
    );

    assert!(validate_lifetime_compatibility(
        ServiceLifetime::Singleton,
        ServiceLifetime::Transient
    )
    .is_ok());

    // Scoped owns and disposes every transient dependency in its scope ledger.
    assert!(
        validate_lifetime_compatibility(ServiceLifetime::Scoped, ServiceLifetime::Transient)
            .is_ok()
    );
}

#[test]
fn test_requires_context_edge_cases() {
    assert!(!requires_context(ServiceLifetime::Singleton));
    assert!(requires_context(ServiceLifetime::Scoped));
    assert!(!requires_context(ServiceLifetime::Transient));
}

#[test]
fn test_lifetime_descriptions() {
    let singleton_desc = lifetime_description(ServiceLifetime::Singleton);
    assert!(singleton_desc.contains("one instance"));
    assert!(singleton_desc.contains("application"));

    let scoped_desc = lifetime_description(ServiceLifetime::Scoped);
    assert!(scoped_desc.contains("per request") || scoped_desc.contains("per process"));

    let transient_desc = lifetime_description(ServiceLifetime::Transient);
    assert!(transient_desc.contains("new instance"));
    assert!(transient_desc.contains("every time"));
}

#[tokio::test]
async fn test_service_metadata_with_trait() {
    let metadata = ServiceMetadata {
        type_id: TypeId::of::<String>(),
        type_name: "TestService",
        trait_type_id: Some(TypeId::of::<u64>()),
        trait_name: Some("ITestService"),
        lifetime: ServiceLifetime::Scoped,
        factory_fn: |_| {
            Box::pin(async {
                Ok(Box::new(String::from("test")) as Box<dyn std::any::Any + Send + Sync>)
            })
        },
        dependencies: vec![],
    };

    assert!(metadata.trait_type_id.is_some());
    assert_eq!(metadata.trait_name, Some("ITestService"));
    assert_eq!(metadata.lifetime, ServiceLifetime::Scoped);
}

#[tokio::test]
async fn test_service_metadata_with_dependencies() {
    let dep_id = TypeId::of::<i32>();

    let metadata = ServiceMetadata {
        type_id: TypeId::of::<String>(),
        type_name: "DependentService",
        trait_type_id: None,
        trait_name: None,
        lifetime: ServiceLifetime::Transient,
        factory_fn: |_| {
            Box::pin(async {
                Ok(Box::new(String::from("dependent")) as Box<dyn std::any::Any + Send + Sync>)
            })
        },
        dependencies: vec![dep_id],
    };

    assert_eq!(metadata.dependencies.len(), 1);
    assert_eq!(metadata.dependencies[0], dep_id);
}

#[test]
fn test_service_metadata_clone() {
    let metadata = ServiceMetadata {
        type_id: TypeId::of::<String>(),
        type_name: "CloneTest",
        trait_type_id: None,
        trait_name: None,
        lifetime: ServiceLifetime::Singleton,
        factory_fn: |_| {
            Box::pin(async { Ok(Box::new(String::new()) as Box<dyn std::any::Any + Send + Sync>) })
        },
        dependencies: vec![],
    };

    let cloned = metadata.clone();
    assert_eq!(cloned.type_name, metadata.type_name);
    assert_eq!(cloned.lifetime, metadata.lifetime);
    assert_eq!(cloned.type_id, metadata.type_id);
}
