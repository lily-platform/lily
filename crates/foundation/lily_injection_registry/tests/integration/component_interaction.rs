//! Tests interaction between registry components

use crate::ServiceRegistrar;
use lily_injection_registry::*;
use std::any::TypeId;
use std::collections::HashMap;
use std::sync::Arc;

/// Mock implementation of ServiceRegistrar for testing
struct MockServiceRegistrar {
    registered_services: HashMap<TypeId, ServiceLifetime>,
    trait_registrations: HashMap<TypeId, TypeId>, // trait_id -> impl_id
}

impl MockServiceRegistrar {
    fn new() -> Self {
        Self {
            registered_services: HashMap::new(),
            trait_registrations: HashMap::new(),
        }
    }

    fn get_registered_count(&self) -> usize {
        self.registered_services.len()
    }

    fn is_registered(&self, type_id: &TypeId) -> bool {
        self.registered_services.contains_key(type_id)
    }

    fn get_lifetime(&self, type_id: &TypeId) -> Option<ServiceLifetime> {
        self.registered_services.get(type_id).copied()
    }
}

impl crate::ServiceRegistrar for MockServiceRegistrar {
    fn register_service<F>(&mut self, type_id: TypeId, lifetime: ServiceLifetime, _factory: F)
    where
        F: Fn(
                Arc<dyn std::any::Any + Send + Sync>,
            ) -> std::pin::Pin<
                Box<
                    dyn std::future::Future<
                            Output = Result<
                                Box<dyn std::any::Any + Send + Sync>,
                                lily_error::injection::InjectionError,
                            >,
                        > + Send
                        + 'static,
                >,
            > + Send
            + Sync
            + 'static,
    {
        self.registered_services.insert(type_id, lifetime);
    }
}

/// Helper to create test metadata
fn create_test_metadata(
    name: &'static str,
    lifetime: ServiceLifetime,
    dependencies: Vec<TypeId>,
    trait_id: Option<TypeId>,
) -> ServiceMetadata {
    ServiceMetadata {
        type_id: TypeId::of::<String>(),
        type_name: name,
        trait_type_id: trait_id,
        trait_name: trait_id.map(|_| "ITestService"),
        lifetime,
        factory_fn: |_| {
            Box::pin(async { Ok(Box::new(String::new()) as Box<dyn std::any::Any + Send + Sync>) })
        },
        dependencies,
    }
}

#[tokio::test]
async fn test_registrar_with_metadata_interaction() {
    // Test that ServiceMetadata properly interacts with ServiceRegistrar

    let metadata = create_test_metadata("TestService", ServiceLifetime::Singleton, vec![], None);

    let mut registrar = MockServiceRegistrar::new();

    // Initially no services registered
    assert_eq!(registrar.get_registered_count(), 0);

    // Register the service
    registrar.register_service(metadata.type_id, metadata.lifetime, metadata.factory_fn);

    // Verify registration was successful
    assert_eq!(registrar.get_registered_count(), 1);
    assert!(registrar.is_registered(&metadata.type_id));
    assert_eq!(
        registrar.get_lifetime(&metadata.type_id),
        Some(ServiceLifetime::Singleton)
    );
}

#[tokio::test]
async fn test_trait_registration_interaction() {
    // Test trait registration interaction
    let trait_type_id = TypeId::of::<u64>(); // Simulating a trait

    let metadata = create_test_metadata(
        "TraitService",
        ServiceLifetime::Scoped,
        vec![],
        Some(trait_type_id),
    );

    let mut registrar = MockServiceRegistrar::new();

    // Register both implementation and trait
    registrar.register_service(
        metadata.type_id,
        metadata.lifetime,
        metadata.factory_fn.clone(),
    );

    if let Some(trait_id) = metadata.trait_type_id {
        registrar.register_service(trait_id, metadata.lifetime, metadata.factory_fn);
        registrar
            .trait_registrations
            .insert(trait_id, metadata.type_id);
    }

    // Verify both registrations
    assert_eq!(registrar.get_registered_count(), 2);
    assert!(registrar.is_registered(&metadata.type_id));
    assert!(registrar.is_registered(&trait_type_id));

    // Verify trait maps to implementation
    assert_eq!(
        registrar.trait_registrations.get(&trait_type_id),
        Some(&metadata.type_id)
    );
}

#[tokio::test]
async fn test_lifetime_validation_with_registrar() {
    use crate::lifetime_utils::*;

    // Test that lifetime validation works with registration process
    let service_a_id = TypeId::of::<i32>();
    let _service_b_id = TypeId::of::<String>();

    // Create metadata for services
    let metadata_a = create_test_metadata("ServiceA", ServiceLifetime::Singleton, vec![], None);

    let metadata_b = create_test_metadata(
        "ServiceB",
        ServiceLifetime::Scoped,
        vec![service_a_id],
        None,
    );

    let mut registrar = MockServiceRegistrar::new();

    // Register services
    registrar.register_service(
        metadata_a.type_id,
        metadata_a.lifetime,
        metadata_a.factory_fn,
    );

    registrar.register_service(
        metadata_b.type_id,
        metadata_b.lifetime,
        metadata_b.factory_fn,
    );

    // Validate lifetimes compatibility
    let result = validate_lifetime_compatibility(
        metadata_b.lifetime, // ServiceB (Scoped)
        metadata_a.lifetime, // ServiceA (Singleton)
    );

    // This should be compatible (Scoped can depend on Singleton)
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_get_all_service_metadata_integration() {
    // Test interaction between registry functions

    // This is mostly a compile-time check since we can't easily inject
    // into SERVICE_METADATA_GETTERS in a test

    let metadata = get_all_service_metadata();

    // This should at least return a valid Vec
    // assert!(metadata.len() >= 0);
}
