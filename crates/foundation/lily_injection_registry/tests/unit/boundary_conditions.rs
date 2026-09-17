//! Boundary condition tests for lily_injection_registry

use lily_injection_registry::*;
use std::any::TypeId;
use std::sync::Arc;

#[tokio::test]
async fn test_service_metadata_with_many_dependencies() {
    // Test service with multiple dependencies
    let dependencies: Vec<TypeId> = vec![
        TypeId::of::<i32>(),
        TypeId::of::<String>(),
        TypeId::of::<u64>(),
        TypeId::of::<bool>(),
        TypeId::of::<Vec<i32>>(),
    ];

    let metadata = ServiceMetadata {
        type_id: TypeId::of::<String>(),
        type_name: "ServiceWithManyDeps",
        trait_type_id: None,
        trait_name: None,
        lifetime: ServiceLifetime::Transient,
        factory_fn: |_| {
            Box::pin(async {
                Ok(Box::new(String::from("multi-dep")) as Box<dyn std::any::Any + Send + Sync>)
            })
        },
        dependencies: dependencies.clone(),
    };

    assert_eq!(metadata.dependencies.len(), 5);
    assert_eq!(metadata.dependencies, dependencies);
}

#[tokio::test]
async fn test_service_metadata_with_zero_dependencies() {
    let metadata = ServiceMetadata {
        type_id: TypeId::of::<String>(),
        type_name: "IndependentService",
        trait_type_id: None,
        trait_name: None,
        lifetime: ServiceLifetime::Singleton,
        factory_fn: |_| {
            Box::pin(async {
                Ok(Box::new(String::from("independent")) as Box<dyn std::any::Any + Send + Sync>)
            })
        },
        dependencies: vec![],
    };

    assert_eq!(metadata.dependencies.len(), 0);
    assert!(metadata.dependencies.is_empty());
}

#[test]
fn test_type_id_uniqueness() {
    // Verify that different types have different TypeIds
    let id1 = TypeId::of::<String>();
    let id2 = TypeId::of::<i32>();
    let id3 = TypeId::of::<String>();

    assert_ne!(id1, id2);
    assert_eq!(id1, id3); // Same type should have same TypeId
}

#[tokio::test]
async fn test_large_type_name() {
    struct VeryLongTypeNameForTestingBoundaryConditionsInServiceRegistry;

    let metadata = ServiceMetadata {
        type_id: TypeId::of::<VeryLongTypeNameForTestingBoundaryConditionsInServiceRegistry>(),
        type_name: "VeryLongTypeNameForTestingBoundaryConditionsInServiceRegistry",
        trait_type_id: None,
        trait_name: None,
        lifetime: ServiceLifetime::Transient,
        factory_fn: |_| {
            Box::pin(async { Ok(Box::new(()) as Box<dyn std::any::Any + Send + Sync>) })
        },
        dependencies: vec![],
    };

    assert!(metadata.type_name.len() > 50);
    assert_eq!(
        metadata.type_name,
        "VeryLongTypeNameForTestingBoundaryConditionsInServiceRegistry"
    );
}

#[test]
fn test_service_lifetime_all_variants() {
    // Test all lifetime variants
    let lifetimes = vec![
        ServiceLifetime::Singleton,
        ServiceLifetime::Scoped,
        ServiceLifetime::Transient,
    ];

    assert_eq!(lifetimes.len(), 3);

    // Verify each is unique
    assert_ne!(lifetimes[0], lifetimes[1]);
    assert_ne!(lifetimes[1], lifetimes[2]);
    assert_ne!(lifetimes[0], lifetimes[2]);
}

#[tokio::test]
async fn test_factory_function_with_complex_type() {
    use std::collections::HashMap;

    let metadata = ServiceMetadata {
        type_id: TypeId::of::<HashMap<String, Vec<i32>>>(),
        type_name: "ComplexTypeService",
        trait_type_id: None,
        trait_name: None,
        lifetime: ServiceLifetime::Transient,
        factory_fn: |_| {
            Box::pin(async {
                let mut map = HashMap::new();
                map.insert("test".to_string(), vec![1, 2, 3]);
                Ok(Box::new(map) as Box<dyn std::any::Any + Send + Sync>)
            })
        },
        dependencies: vec![],
    };

    let extensions = Arc::new(()) as Arc<dyn std::any::Any + Send + Sync>;
    let result = (metadata.factory_fn)(extensions).await;

    assert!(result.is_ok());
    let service = result.unwrap();
    let map = service.downcast_ref::<HashMap<String, Vec<i32>>>().unwrap();
    assert_eq!(map.get("test").unwrap(), &vec![1, 2, 3]);
}

#[test]
fn test_empty_metadata_vector() {
    // Test behavior with empty metadata collection
    let empty_vec: Vec<&ServiceMetadata> = vec![];
    assert_eq!(empty_vec.len(), 0);
    assert!(empty_vec.is_empty());
}

#[tokio::test]
async fn test_service_with_optional_trait_fields() {
    // Test metadata with Some trait fields
    let with_trait = ServiceMetadata {
        type_id: TypeId::of::<String>(),
        type_name: "WithTrait",
        trait_type_id: Some(TypeId::of::<u64>()),
        trait_name: Some("ITrait"),
        lifetime: ServiceLifetime::Singleton,
        factory_fn: |_| {
            Box::pin(async { Ok(Box::new(String::new()) as Box<dyn std::any::Any + Send + Sync>) })
        },
        dependencies: vec![],
    };

    assert!(with_trait.trait_type_id.is_some());
    assert!(with_trait.trait_name.is_some());

    // Test metadata with None trait fields
    let without_trait = ServiceMetadata {
        type_id: TypeId::of::<i32>(),
        type_name: "WithoutTrait",
        trait_type_id: None,
        trait_name: None,
        lifetime: ServiceLifetime::Transient,
        factory_fn: |_| {
            Box::pin(async { Ok(Box::new(42i32) as Box<dyn std::any::Any + Send + Sync>) })
        },
        dependencies: vec![],
    };

    assert!(without_trait.trait_type_id.is_none());
    assert!(without_trait.trait_name.is_none());
}
