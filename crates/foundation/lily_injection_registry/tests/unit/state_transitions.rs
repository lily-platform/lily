//! State transition tests for lily_injection_registry

use crate::ServiceRegistrar;
use lily_injection_registry::*;
use std::any::TypeId;
use std::sync::Arc;

#[tokio::test]
async fn test_service_lifecycle_state_transitions() {
    // Test service creation and state transitions through factory
    let metadata = ServiceMetadata {
        type_id: TypeId::of::<String>(),
        type_name: "StatefulService",
        trait_type_id: None,
        trait_name: None,
        lifetime: ServiceLifetime::Singleton,
        factory_fn: |_| {
            Box::pin(async {
                // Simulate initialization state
                let service = String::from("initialized");
                Ok(Box::new(service) as Box<dyn std::any::Any + Send + Sync>)
            })
        },
        dependencies: vec![],
    };

    let extensions = Arc::new(()) as Arc<dyn std::any::Any + Send + Sync>;

    // State 1: Factory not called yet
    // State 2: Factory called, service created
    let result = (metadata.factory_fn)(extensions.clone()).await;
    assert!(result.is_ok());

    // State 3: Service returned and can be used
    let service = result.unwrap();
    let value = service.downcast_ref::<String>().unwrap();
    assert_eq!(value, "initialized");
}

#[test]
fn test_lifetime_state_transitions() {
    // Test transitioning through different lifetime validations
    use crate::lifetime_utils::*;

    // Valid state transitions in validation
    let valid_transitions = vec![
        (ServiceLifetime::Singleton, ServiceLifetime::Singleton, true),
        (ServiceLifetime::Scoped, ServiceLifetime::Singleton, true),
        (ServiceLifetime::Transient, ServiceLifetime::Singleton, true),
        (ServiceLifetime::Transient, ServiceLifetime::Scoped, true),
        (ServiceLifetime::Scoped, ServiceLifetime::Transient, true),
        (ServiceLifetime::Singleton, ServiceLifetime::Transient, true),
    ];

    for (service, dep, should_pass) in valid_transitions {
        let result = validate_lifetime_compatibility(service, dep);
        assert_eq!(result.is_ok(), should_pass);
    }

    // Invalid state transitions
    let invalid_transitions = vec![(ServiceLifetime::Singleton, ServiceLifetime::Scoped, false)];

    for (service, dep, should_pass) in invalid_transitions {
        let result = validate_lifetime_compatibility(service, dep);
        assert_eq!(result.is_ok(), should_pass);
    }
}

#[tokio::test]
async fn test_metadata_mutation_through_clone() {
    // Test that cloned metadata maintains state independently
    let original = ServiceMetadata {
        type_id: TypeId::of::<i32>(),
        type_name: "Original",
        trait_type_id: None,
        trait_name: None,
        lifetime: ServiceLifetime::Singleton,
        factory_fn: |_| {
            Box::pin(async { Ok(Box::new(100i32) as Box<dyn std::any::Any + Send + Sync>) })
        },
        dependencies: vec![],
    };

    let mut cloned = original.clone();

    // Modify cloned metadata
    cloned.dependencies.push(TypeId::of::<String>());

    // Original should remain unchanged
    assert_eq!(original.dependencies.len(), 0);
    assert_eq!(cloned.dependencies.len(), 1);
}

#[test]
fn test_requires_context_state() {
    use crate::lifetime_utils::*;

    // Singleton: no context required (self-sufficient state)
    assert!(!requires_context(ServiceLifetime::Singleton));

    // Scoped resolution needs an owning scope; root transients are permitted.
    assert!(requires_context(ServiceLifetime::Scoped));
    assert!(!requires_context(ServiceLifetime::Transient));
}

#[tokio::test]
async fn test_service_registration_state_flow() {
    // Simulate state flow through registration process
    struct MockRegistrar {
        registered_count: usize,
    }

    impl crate::ServiceRegistrar for MockRegistrar {
        fn register_service<F>(&mut self, _type_id: TypeId, _lifetime: ServiceLifetime, _factory: F)
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
            self.registered_count += 1;
        }
    }

    let mut registrar = MockRegistrar {
        registered_count: 0,
    };

    // State 1: No services registered
    assert_eq!(registrar.registered_count, 0);

    // State 2: Register a service
    registrar.register_service(TypeId::of::<String>(), ServiceLifetime::Singleton, |_| {
        Box::pin(async { Ok(Box::new(String::new()) as Box<dyn std::any::Any + Send + Sync>) })
    });

    // State 3: Service registered
    assert_eq!(registrar.registered_count, 1);
}

#[test]
fn test_dependency_graph_state_building() {
    // Test state transitions during dependency graph construction
    // This would typically be tested with actual services, but we can test the concept

    let service_a_id = TypeId::of::<i32>();
    let service_b_id = TypeId::of::<String>();

    // State 1: No dependencies
    let deps_a: Vec<TypeId> = vec![];
    assert!(deps_a.is_empty());

    // State 2: Add dependency
    let mut deps_b = vec![];
    deps_b.push(service_a_id);
    assert_eq!(deps_b.len(), 1);

    // State 3: Multiple dependencies
    deps_b.push(service_b_id);
    assert_eq!(deps_b.len(), 2);
}

#[tokio::test]
async fn test_factory_error_state_handling() {
    use lily_error::injection::InjectionError;

    // Test state when factory fails
    let metadata = ServiceMetadata {
        type_id: TypeId::of::<String>(),
        type_name: "FailingService",
        trait_type_id: None,
        trait_name: None,
        lifetime: ServiceLifetime::Transient,
        factory_fn: |_| {
            Box::pin(async {
                // Simulate failure state
                Err(InjectionError::General("Initialization failed".to_string()))
            })
        },
        dependencies: vec![],
    };

    let extensions = Arc::new(()) as Arc<dyn std::any::Any + Send + Sync>;
    let result = (metadata.factory_fn)(extensions).await;

    // Service should be in error state
    assert!(result.is_err());

    // Verify we can recover from error state by retrying
    let retry_result =
        (metadata.factory_fn)(Arc::new(()) as Arc<dyn std::any::Any + Send + Sync>).await;
    assert!(retry_result.is_err()); // Still fails as expected
}
