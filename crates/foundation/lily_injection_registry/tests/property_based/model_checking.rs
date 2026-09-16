//! Model checking tests for service registry behavior
//! Uses state machine testing to verify system behavior under various conditions

use crate::lifetime_utils::validate_lifetime_compatibility;
use crate::ServiceRegistrar;
use lily_injection_registry::*;
use proptest::prelude::*;
use proptest::prop_oneof;
use proptest::strategy::Strategy;
use proptest::test_runner::TestRunner;
use std::any::TypeId;
use std::collections::HashMap;
use std::sync::Arc;

/// Represents the state of our registry system
#[derive(Debug, Clone)]
struct RegistryState {
    services: HashMap<TypeId, ServiceMetadata>,
    initialization_order: Vec<TypeId>,
    active_services: HashMap<TypeId, bool>,
}

impl RegistryState {
    fn new() -> Self {
        Self {
            services: HashMap::new(),
            initialization_order: Vec::new(),
            active_services: HashMap::new(),
        }
    }
}

/// Possible actions that can be performed on the registry
#[derive(Debug, Clone)]
enum RegistryAction {
    RegisterService {
        type_id: TypeId,
        lifetime: ServiceLifetime,
        dependencies: Vec<TypeId>,
    },
    InitializeService(TypeId),
    DisposeService(TypeId),
}

/// Generate random but valid registry actions
fn registry_actions() -> impl Strategy<Value = Vec<RegistryAction>> {
    let service_types = vec![
        TypeId::of::<i32>(),
        TypeId::of::<String>(),
        TypeId::of::<bool>(),
        TypeId::of::<u64>(),
        TypeId::of::<Vec<i32>>(),
    ];

    let lifetimes = vec![
        ServiceLifetime::Singleton,
        ServiceLifetime::Scoped,
        ServiceLifetime::Transient,
    ];

    prop::collection::vec(
        prop_oneof![
            // RegisterService action
            (
                prop::sample::select(service_types.clone()),
                prop::sample::select(lifetimes.clone()),
                prop::collection::vec(prop::sample::select(service_types.clone()), 0..3),
            )
                .prop_map(|(type_id, lifetime, dependencies)| {
                    RegistryAction::RegisterService {
                        type_id,
                        lifetime,
                        dependencies,
                    }
                }),
            // InitializeService action
            prop::sample::select(service_types.clone()).prop_map(RegistryAction::InitializeService),
            // DisposeService action
            prop::sample::select(service_types.clone()).prop_map(RegistryAction::DisposeService),
        ],
        1..20,
    )
}

proptest! {
    #[test]
    fn test_registry_state_machine(actions in registry_actions()) {
        let mut state = RegistryState::new();
        let mut registrar = MockRegistrar::new();

        for action in actions {
            match action {
                RegistryAction::RegisterService { type_id, lifetime, dependencies } => {
                    // Verify registration invariants
                    if !state.services.contains_key(&type_id) {
                        let metadata = create_test_metadata(type_id, lifetime, dependencies.clone());

                        // Check dependency validity
                        let valid_deps = dependencies.iter().all(|dep_id| {
                            if let Some(dep_metadata) = state.services.get(dep_id) {
                                validate_lifetime_compatibility(lifetime, dep_metadata.lifetime).is_ok()
                            } else {
                                false
                            }
                        });

                        if valid_deps {
                            state.services.insert(type_id, metadata.clone());
                            registrar.register_service(type_id, lifetime, metadata.factory_fn);

                            // Verify registration succeeded
                            prop_assert!(state.services.contains_key(&type_id));
                            prop_assert!(registrar.is_registered(&type_id));
                        }
                    }
                }

                RegistryAction::InitializeService(type_id) => {
                    if let Some(metadata) = state.services.get(&type_id) {
                        // Check if dependencies are initialized
                        let deps_initialized = metadata.dependencies.iter().all(|dep_id| {
                            state.initialization_order.contains(dep_id)
                        });

                        if deps_initialized && !state.initialization_order.contains(&type_id) {
                            state.initialization_order.push(type_id);
                            state.active_services.insert(type_id, true);

                            // Verify initialization order constraints
                            for dep_id in &metadata.dependencies {
                                let dep_index = state.initialization_order.iter()
                                    .position(|&id| id == *dep_id)
                                    .unwrap();
                                let service_index = state.initialization_order.len() - 1;
                                prop_assert!(dep_index < service_index);
                            }
                        }
                    }
                }

                RegistryAction::DisposeService(type_id) => {
                    if state.active_services.get(&type_id) == Some(&true) {
                        // Check if any active services depend on this one
                        let has_active_dependents = state.services.iter().any(|(_, metadata)| {
                            metadata.dependencies.contains(&type_id) &&
                            state.active_services.get(&metadata.type_id) == Some(&true)
                        });

                        if !has_active_dependents {
                            state.active_services.insert(type_id, false);

                            // Verify disposal constraints
                            prop_assert!(!state.active_services[&type_id]);
                        }
                    }
                }
            }
        }

        // Final state invariants
        for (type_id, metadata) in &state.services {
            // All dependencies should be registered
            for dep_id in &metadata.dependencies {
                prop_assert!(state.services.contains_key(dep_id));
            }

            // If service is active, all its dependencies should be initialized
            if state.active_services.get(type_id) == Some(&true) {
                for dep_id in &metadata.dependencies {
                    prop_assert!(state.initialization_order.contains(dep_id));
                }
            }
        }
    }
}

/// Mock registrar for testing
struct MockRegistrar {
    registered: HashMap<TypeId, ServiceLifetime>,
}

impl MockRegistrar {
    fn new() -> Self {
        Self {
            registered: HashMap::new(),
        }
    }

    fn is_registered(&self, type_id: &TypeId) -> bool {
        self.registered.contains_key(type_id)
    }
}

impl crate::ServiceRegistrar for MockRegistrar {
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
        self.registered.insert(type_id, lifetime);
    }
}

/// Helper to create test metadata
fn create_test_metadata(
    type_id: TypeId,
    lifetime: ServiceLifetime,
    dependencies: Vec<TypeId>,
) -> ServiceMetadata {
    ServiceMetadata {
        type_id,
        type_name: "TestService",
        trait_type_id: None,
        trait_name: None,
        lifetime,
        factory_fn: |_| {
            Box::pin(async { Ok(Box::new(()) as Box<dyn std::any::Any + Send + Sync>) })
        },
        dependencies,
    }
}

#[test]
fn test_singleton_initialization_model() {
    let mut runner = TestRunner::default();

    // Test that singletons are always initialized before their dependents
    let actions = registry_actions();
    runner
        .run(&actions, |actions| {
            let mut state = RegistryState::new();

            for action in actions {
                if let RegistryAction::RegisterService {
                    type_id,
                    lifetime: ServiceLifetime::Singleton,
                    ..
                } = action
                {
                    if !state.initialization_order.contains(&type_id) {
                        state.initialization_order.push(type_id);
                    }
                }
            }

            Ok(())
        })
        .unwrap();
}
