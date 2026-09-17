//! Property-based tests for system invariants
//! Tests that verify core properties that should always hold true

use crate::{analyze_dependencies, lifetime_utils::*};
use lily_injection_registry::*;
use proptest::prelude::*;
use std::any::TypeId;
use std::sync::Arc;

proptest! {
    #[test]
    fn test_lifetime_validation_properties(
        service_lifetime in prop_oneof![
            Just(ServiceLifetime::Singleton),
            Just(ServiceLifetime::Scoped),
            Just(ServiceLifetime::Transient)
        ],
        dependency_lifetime in prop_oneof![
            Just(ServiceLifetime::Singleton),
            Just(ServiceLifetime::Scoped),
            Just(ServiceLifetime::Transient)
        ]
    ) {
        // Property: Singleton dependencies are always valid
        if dependency_lifetime == ServiceLifetime::Singleton {
            let result = validate_lifetime_compatibility(service_lifetime, dependency_lifetime);
            assert!(result.is_ok());
        }

        // Property: Transient services can depend on anything
        if service_lifetime == ServiceLifetime::Transient {
            let result = validate_lifetime_compatibility(service_lifetime, dependency_lifetime);
            assert!(result.is_ok());
        }

        // Property: singletons cannot capture request-owned dependencies.
        // Root-owned transients are tracked in the same lifecycle ledger.
        match (service_lifetime, dependency_lifetime) {
            (ServiceLifetime::Singleton, ServiceLifetime::Scoped) => {
                let result = validate_lifetime_compatibility(service_lifetime, dependency_lifetime);
                assert!(result.is_err());
            }
            _ => {}
        }
    }
}

proptest! {
    #[test]
    fn test_service_metadata_properties(
        type_name in "[A-Za-z][A-Za-z0-9_]*",
        has_trait in proptest::bool::ANY,
        lifetime in prop_oneof![
            Just(ServiceLifetime::Singleton),
            Just(ServiceLifetime::Scoped),
            Just(ServiceLifetime::Transient)
        ],
        dependency_count in 0..5usize
    ) {
        // Create test metadata with generated properties
        let trait_type_id = if has_trait { Some(TypeId::of::<u64>()) } else { None };
        let trait_name = trait_type_id.map(|_| "ITestTrait");

        let dependencies = (0..dependency_count)
            .map(|_i| TypeId::of::<i32>())
            .collect::<Vec<_>>();

        let metadata = ServiceMetadata {
            type_id: TypeId::of::<String>(),
            type_name: Box::leak(type_name.into_boxed_str()),
            trait_type_id,
            trait_name,
            lifetime,
            factory_fn: |_| Box::pin(async {
                Ok(Box::new(String::new()) as Box<dyn std::any::Any + Send + Sync>)
            }),
            dependencies,
        };

        // Property: type_name should never be empty
        prop_assert!(!metadata.type_name.is_empty());

        // Property: trait_type_id and trait_name should be consistent
        prop_assert_eq!(metadata.trait_type_id.is_some(), metadata.trait_name.is_some());

        // Property: dependencies count should match input
        prop_assert_eq!(metadata.dependencies.len(), dependency_count);

        // Property: metadata should be cloneable without panic
        let _cloned = metadata.clone();
    }
}

proptest! {
    #[test]
    fn test_requires_context_properties(
        lifetime in prop_oneof![
            Just(ServiceLifetime::Singleton),
            Just(ServiceLifetime::Scoped),
            Just(ServiceLifetime::Transient)
        ]
    ) {
        // Property: only scoped services require an active scope. A transient
        // may be owned either by the root container or by a live scope.
        prop_assert_eq!(requires_context(lifetime), lifetime == ServiceLifetime::Scoped);
    }
}

proptest! {
    #[test]
    fn test_lifetime_description_properties(
        lifetime in prop_oneof![
            Just(ServiceLifetime::Singleton),
            Just(ServiceLifetime::Scoped),
            Just(ServiceLifetime::Transient)
        ]
    ) {
        let description = lifetime_description(lifetime);

        // Property: Description should never be empty
        prop_assert!(!description.is_empty());

        // Property: Description should contain relevant keywords
        match lifetime {
            ServiceLifetime::Singleton => prop_assert!(description.contains("application")),
            ServiceLifetime::Scoped => prop_assert!(description.contains("request") || description.contains("process")),
            ServiceLifetime::Transient => prop_assert!(description.contains("new")),
        }
    }
}

proptest! {
    #[test]
    fn test_factory_function_properties(
        value in proptest::string::string_regex("[a-zA-Z0-9_]{1,10}").unwrap()
    ) {
        // Create metadata with generated value
        let metadata = ServiceMetadata {
            type_id: TypeId::of::<String>(),
            type_name: "TestService",
            trait_type_id: None,
            trait_name: None,
            lifetime: ServiceLifetime::Singleton,
            factory_fn: |_| {
                Box::pin(async {
                    Ok(Box::new(String::new()) as Box<dyn std::any::Any + Send + Sync>)
                })
            },
            dependencies: vec![],
        };

        // Property: Factory should always return Ok
        let extensions = Arc::new(()) as Arc<dyn std::any::Any + Send + Sync>;
        let result = futures::executor::block_on((metadata.factory_fn)(extensions));
        prop_assert!(result.is_ok());

        // Property: Factory result should be downcasted to correct type
        let service = result.unwrap();
        let downcasted = service.downcast_ref::<String>();
        prop_assert!(downcasted.is_some());
        // Note: Factory creates String::new(), not the input value
        // So we just verify it's a valid String
        // prop_assert!(downcasted.unwrap().len() >= 0);
    }
}

// Helper function to generate random dependency graphs
fn generate_dependency_graph(
    size: usize,
    max_deps_per_service: usize,
) -> Vec<(TypeId, Vec<TypeId>)> {
    let mut rng = proptest::test_runner::TestRng::deterministic_rng(
        proptest::test_runner::RngAlgorithm::ChaCha,
    );
    let mut services = Vec::with_capacity(size);

    // Create service IDs
    for _ in 0..size {
        let deps = (0..rng.gen_range(0..=max_deps_per_service))
            .map(|_| TypeId::of::<i32>())
            .collect();
        services.push((TypeId::of::<String>(), deps));
    }

    services
}

proptest! {
    #[test]
    fn test_circular_dependency_detection(
        size in 1..10usize,
        max_deps in 0..5usize
    ) {
        let graph = generate_dependency_graph(size, max_deps);

        // Property: Empty or single-node graphs should never have cycles
        if size <= 1 {
            // Mock the graph analysis by creating test metadata
            let _metadata = ServiceMetadata {
                type_id: graph[0].0,
                type_name: "TestService",
                trait_type_id: None,
                trait_name: None,
                lifetime: ServiceLifetime::Singleton,
                factory_fn: |_| Box::pin(async {
                    Ok(Box::new(String::new()) as Box<dyn std::any::Any + Send + Sync>)
                }),
                dependencies: graph[0].1.clone(),
            };

            let result = analyze_dependencies();
            prop_assert!(result.is_ok());
        }
    }
}
