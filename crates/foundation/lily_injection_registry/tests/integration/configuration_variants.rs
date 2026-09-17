//! Tests for different configuration variants

use crate::ServiceRegistrar;
use lily_injection_registry::*;
use std::any::TypeId;
use std::sync::Arc;

/// Mock implementation of ServiceRegistrar for testing
struct MockServiceRegistrar {
    registered_services: Vec<(TypeId, ServiceLifetime)>,
}

impl MockServiceRegistrar {
    fn new() -> Self {
        Self {
            registered_services: Vec::new(),
        }
    }

    fn get_registered_count(&self) -> usize {
        self.registered_services.len()
    }

    fn get_registered_lifetimes(&self) -> Vec<ServiceLifetime> {
        self.registered_services
            .iter()
            .map(|(_, lifetime)| *lifetime)
            .collect()
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
        self.registered_services.push((type_id, lifetime));
    }
}

#[tokio::test]
async fn test_all_lifetime_configurations() {
    // Test registration with all possible lifetime configurations

    // Create metadata for each lifetime
    let singleton_metadata = ServiceMetadata {
        type_id: TypeId::of::<String>(),
        type_name: "SingletonService",
        trait_type_id: None,
        trait_name: None,
        lifetime: ServiceLifetime::Singleton,
        factory_fn: |_| {
            Box::pin(async {
                Ok(Box::new(String::from("singleton")) as Box<dyn std::any::Any + Send + Sync>)
            })
        },
        dependencies: vec![],
    };

    let scoped_metadata = ServiceMetadata {
        type_id: TypeId::of::<i32>(),
        type_name: "ScopedService",
        trait_type_id: None,
        trait_name: None,
        lifetime: ServiceLifetime::Scoped,
        factory_fn: |_| {
            Box::pin(async { Ok(Box::new(42i32) as Box<dyn std::any::Any + Send + Sync>) })
        },
        dependencies: vec![],
    };

    let transient_metadata = ServiceMetadata {
        type_id: TypeId::of::<bool>(),
        type_name: "TransientService",
        trait_type_id: None,
        trait_name: None,
        lifetime: ServiceLifetime::Transient,
        factory_fn: |_| {
            Box::pin(async { Ok(Box::new(true) as Box<dyn std::any::Any + Send + Sync>) })
        },
        dependencies: vec![],
    };

    // Register all configurations
    let mut registrar = MockServiceRegistrar::new();

    registrar.register_service(
        singleton_metadata.type_id,
        singleton_metadata.lifetime,
        singleton_metadata.factory_fn,
    );

    registrar.register_service(
        scoped_metadata.type_id,
        scoped_metadata.lifetime,
        scoped_metadata.factory_fn,
    );

    registrar.register_service(
        transient_metadata.type_id,
        transient_metadata.lifetime,
        transient_metadata.factory_fn,
    );

    // Verify all configurations were registered
    assert_eq!(registrar.get_registered_count(), 3);

    let lifetimes = registrar.get_registered_lifetimes();
    assert!(lifetimes.contains(&ServiceLifetime::Singleton));
    assert!(lifetimes.contains(&ServiceLifetime::Scoped));
    assert!(lifetimes.contains(&ServiceLifetime::Transient));
}

#[tokio::test]
async fn test_trait_implementation_configurations() {
    // Test different trait implementation configurations

    // Create metadata with trait implementation
    let trait_type_id = TypeId::of::<u64>();

    let metadata = ServiceMetadata {
        type_id: TypeId::of::<String>(),
        type_name: "TraitImplementingService",
        trait_type_id: Some(trait_type_id),
        trait_name: Some("ITrait"),
        lifetime: ServiceLifetime::Singleton,
        factory_fn: |_| {
            Box::pin(async {
                Ok(Box::new(String::from("trait impl")) as Box<dyn std::any::Any + Send + Sync>)
            })
        },
        dependencies: vec![],
    };

    let mut registrar = MockServiceRegistrar::new();

    // Register both the concrete type and trait interface
    registrar.register_service(
        metadata.type_id,
        metadata.lifetime,
        metadata.factory_fn.clone(),
    );

    if let Some(trait_id) = metadata.trait_type_id {
        registrar.register_service(trait_id, metadata.lifetime, metadata.factory_fn);
    }

    // Verify both registrations
    assert_eq!(registrar.get_registered_count(), 2);
}

#[tokio::test]
async fn test_dependency_configurations() {
    // Test different dependency configurations

    let dep1_id = TypeId::of::<i32>();
    let dep2_id = TypeId::of::<u32>();
    let dep3_id = TypeId::of::<i64>();

    // Service with no dependencies
    let no_deps_metadata = ServiceMetadata {
        type_id: TypeId::of::<String>(),
        type_name: "NoDepsService",
        trait_type_id: None,
        trait_name: None,
        lifetime: ServiceLifetime::Singleton,
        factory_fn: |_| {
            Box::pin(async {
                Ok(Box::new(String::from("no deps")) as Box<dyn std::any::Any + Send + Sync>)
            })
        },
        dependencies: vec![],
    };

    // Service with one dependency
    let one_dep_metadata = ServiceMetadata {
        type_id: TypeId::of::<bool>(),
        type_name: "OneDepService",
        trait_type_id: None,
        trait_name: None,
        lifetime: ServiceLifetime::Singleton,
        factory_fn: |_| {
            Box::pin(async { Ok(Box::new(true) as Box<dyn std::any::Any + Send + Sync>) })
        },
        dependencies: vec![dep1_id],
    };

    // Service with multiple dependencies
    let multi_deps_metadata = ServiceMetadata {
        type_id: TypeId::of::<u64>(),
        type_name: "MultiDepsService",
        trait_type_id: None,
        trait_name: None,
        lifetime: ServiceLifetime::Singleton,
        factory_fn: |_| {
            Box::pin(async { Ok(Box::new(42u64) as Box<dyn std::any::Any + Send + Sync>) })
        },
        dependencies: vec![dep1_id, dep2_id, dep3_id],
    };

    // Verify different dependency configurations
    assert_eq!(no_deps_metadata.dependencies.len(), 0);
    assert_eq!(one_dep_metadata.dependencies.len(), 1);
    assert_eq!(multi_deps_metadata.dependencies.len(), 3);

    // Verify dependency IDs
    assert_eq!(one_dep_metadata.dependencies[0], dep1_id);
    assert_eq!(multi_deps_metadata.dependencies[0], dep1_id);
    assert_eq!(multi_deps_metadata.dependencies[1], dep2_id);
    assert_eq!(multi_deps_metadata.dependencies[2], dep3_id);
}
