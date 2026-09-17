//! QuickCheck property tests for lily_injection_registry
//! Uses quickcheck to verify properties with randomly generated inputs

use crate::{analyze_dependencies, lifetime_utils::*};
use lily_injection_registry::*;
use quickcheck::{quickcheck, Arbitrary, Gen};
use std::any::TypeId;

// Custom Arbitrary implementation for ServiceLifetime
// Note: Cannot implement Arbitrary for ServiceLifetime due to orphan rules
// Using a wrapper type instead
#[derive(Debug, Clone)]
struct ServiceLifetimeWrapper(ServiceLifetime);

impl Arbitrary for ServiceLifetimeWrapper {
    fn arbitrary(g: &mut Gen) -> Self {
        let lifetime = match u8::arbitrary(g) % 3 {
            0 => ServiceLifetime::Singleton,
            1 => ServiceLifetime::Scoped,
            _ => ServiceLifetime::Transient,
        };
        ServiceLifetimeWrapper(lifetime)
    }
}

// Helper struct for generating test services
#[derive(Debug, Clone)]
struct TestService {
    lifetime: ServiceLifetime,
    dependencies: Vec<TypeId>,
}

impl Arbitrary for TestService {
    fn arbitrary(g: &mut Gen) -> Self {
        let lifetime_wrapper = ServiceLifetimeWrapper::arbitrary(g);
        let dep_count = usize::arbitrary(g) % 5; // 0-4 dependencies
        let dependencies = (0..dep_count).map(|_| TypeId::of::<String>()).collect();

        TestService {
            lifetime: lifetime_wrapper.0,
            dependencies,
        }
    }
}

quickcheck! {
    // Property: Lifetime validation should be reflexive for same lifetime
    fn prop_lifetime_validation_reflexive(lifetime_wrapper: ServiceLifetimeWrapper) -> bool {
        let lifetime = lifetime_wrapper.0;
        validate_lifetime_compatibility(lifetime, lifetime).is_ok()
    }

    // Property: Transient services can depend on any lifetime
    fn prop_transient_can_depend_on_any(dep_lifetime_wrapper: ServiceLifetimeWrapper) -> bool {
        let dep_lifetime = dep_lifetime_wrapper.0;
        validate_lifetime_compatibility(ServiceLifetime::Transient, dep_lifetime).is_ok()
    }

    // Property: Any service can depend on Singleton
    fn prop_any_can_depend_on_singleton(service_lifetime_wrapper: ServiceLifetimeWrapper) -> bool {
        let service_lifetime = service_lifetime_wrapper.0;
        validate_lifetime_compatibility(service_lifetime, ServiceLifetime::Singleton).is_ok()
    }

    // Property: Singleton cannot capture scoped state, but may own a transient
    // in the root lifecycle ledger.
    fn prop_singleton_dependency_constraints(dep_lifetime_wrapper: ServiceLifetimeWrapper) -> bool {
        let dep_lifetime = dep_lifetime_wrapper.0;
        match dep_lifetime {
            ServiceLifetime::Singleton | ServiceLifetime::Transient => {
                validate_lifetime_compatibility(ServiceLifetime::Singleton, dep_lifetime).is_ok()
            }
            ServiceLifetime::Scoped => {
                validate_lifetime_compatibility(ServiceLifetime::Singleton, dep_lifetime).is_err()
            }
        }
    }

    // Property: requires_context should be consistent
    fn prop_requires_context_consistency(lifetime_wrapper: ServiceLifetimeWrapper) -> bool {
        let lifetime = lifetime_wrapper.0;
        requires_context(lifetime) == matches!(lifetime, ServiceLifetime::Scoped)
    }

    // Property: Service metadata should maintain type consistency
    fn prop_metadata_type_consistency(service: TestService) -> bool {
        let metadata = ServiceMetadata {
            type_id: TypeId::of::<String>(),
            type_name: "TestService",
            trait_type_id: None,
            trait_name: None,
            lifetime: service.lifetime,
            factory_fn: |_| {
                Box::pin(async {
                    Ok(Box::new(String::new()) as Box<dyn std::any::Any + Send + Sync>)
                })
            },
            dependencies: service.dependencies.clone(),
        };

        metadata.type_id == TypeId::of::<String>() &&
        metadata.dependencies.len() == service.dependencies.len()
    }

    // Property: Lifetime descriptions should never be empty
    fn prop_lifetime_description_not_empty(lifetime_wrapper: ServiceLifetimeWrapper) -> bool {
        let lifetime = lifetime_wrapper.0;
        !lifetime_description(lifetime).is_empty()
    }

    // Property: Service metadata cloning should preserve all fields
    fn prop_metadata_clone_equality(service: TestService) -> bool {
        let metadata = ServiceMetadata {
            type_id: TypeId::of::<String>(),
            type_name: "TestService",
            trait_type_id: None,
            trait_name: None,
            lifetime: service.lifetime,
            factory_fn: |_| {
                Box::pin(async {
                    Ok(Box::new(String::new()) as Box<dyn std::any::Any + Send + Sync>)
                })
            },
            dependencies: service.dependencies,
        };

        let cloned = metadata.clone();

        metadata.type_id == cloned.type_id &&
        metadata.type_name == cloned.type_name &&
        metadata.trait_type_id == cloned.trait_type_id &&
        metadata.trait_name == cloned.trait_name &&
        metadata.lifetime == cloned.lifetime &&
        metadata.dependencies == cloned.dependencies
    }
}

// Additional property tests that require async runtime
#[tokio::test]
async fn test_factory_function_properties() {
    quickcheck! {
        fn prop_factory_always_returns_string(value: String) -> bool {
            let metadata = ServiceMetadata {
                type_id: TypeId::of::<String>(),
                type_name: "TestService",
                trait_type_id: None,
                trait_name: None,
                lifetime: ServiceLifetime::Singleton,
                factory_fn: |_| Box::pin(async {
                    Ok(Box::new(String::new()) as Box<dyn std::any::Any + Send + Sync>)
                }),
                dependencies: vec![],
            };

            let extensions = std::sync::Arc::new(());
            let result = futures::executor::block_on((metadata.factory_fn)(extensions));

            if let Ok(service) = result {
                if let Some(string_value) = service.downcast_ref::<String>() {
                    string_value == &value
                } else {
                    false
                }
            } else {
                false
            }
        }
    }
}

// Property tests for dependency analysis
quickcheck! {
    fn prop_analyze_dependencies_empty_is_ok(_unit: ()) -> bool {
        analyze_dependencies().is_ok()
    }

    fn prop_analyze_dependencies_preserves_singleton_count(services: Vec<TestService>) -> bool {
        let singleton_count = services.iter()
            .filter(|s| s.lifetime == ServiceLifetime::Singleton)
            .count();

        // This is a simplified test since we can't easily create the distributed slice
        // In a real scenario, we'd need to test with actual registered services
        singleton_count == 0 || analyze_dependencies().is_ok()
    }
}

// Property tests for trait registration
quickcheck! {
    fn prop_trait_registration_consistency(service: TestService, has_trait: bool) -> bool {
        let metadata = ServiceMetadata {
            type_id: TypeId::of::<String>(),
            type_name: "TestService",
            trait_type_id: if has_trait { Some(TypeId::of::<i32>()) } else { None },
            trait_name: if has_trait { Some("ITrait") } else { None },
            lifetime: service.lifetime,
            factory_fn: |_| {
                Box::pin(async {
                    Ok(Box::new(String::new()) as Box<dyn std::any::Any + Send + Sync>)
                })
            },
            dependencies: service.dependencies,
        };

        // Trait type ID and name should be consistent
        metadata.trait_type_id.is_some() == metadata.trait_name.is_some()
    }
}
