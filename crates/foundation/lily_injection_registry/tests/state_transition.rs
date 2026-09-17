//! Deterministic service state-transition scenarios.

use arbitrary::Arbitrary;
use lily_injection_registry::*;
use std::any::TypeId;
use std::sync::{Arc, Mutex};

#[derive(Arbitrary, Debug, Clone)]
enum ServiceState {
    Unregistered,
    Registered,
    Initialized,
    Failed,
}

#[derive(Arbitrary, Debug)]
enum StateTransition {
    Register { name: String, lifetime_id: u8 },
    Initialize,
    Dispose,
    ChangeLifetime { new_lifetime_id: u8 },
    AddDependency { dependency_count: u8 },
    RemoveDependency,
}

#[derive(Clone)]
struct StatefulService {
    metadata: ServiceMetadata,
    state: ServiceState,
}

impl StatefulService {
    fn new(_name: &str, lifetime: ServiceLifetime) -> Self {
        Self {
            metadata: ServiceMetadata {
                type_id: TypeId::of::<String>(),
                type_name: "StatefulService",
                trait_type_id: None,
                trait_name: None,
                lifetime,
                factory_fn: |_| {
                    Box::pin(async {
                        Ok(Box::new(String::new()) as Box<dyn std::any::Any + Send + Sync>)
                    })
                },
                dependencies: vec![],
            },
            state: ServiceState::Unregistered,
        }
    }
}

struct StateScenarioRegistry {
    services: Arc<Mutex<Vec<StatefulService>>>,
}

impl StateScenarioRegistry {
    fn new() -> Self {
        Self {
            services: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

fn lifetime_from_id(id: u8) -> ServiceLifetime {
    match id % 3 {
        0 => ServiceLifetime::Singleton,
        1 => ServiceLifetime::Scoped,
        _ => ServiceLifetime::Transient,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_transitions() {
        let transitions = vec![
            StateTransition::Register {
                name: "TestService".to_string(),
                lifetime_id: 0,
            },
            StateTransition::AddDependency {
                dependency_count: 2,
            },
            StateTransition::Initialize,
            StateTransition::ChangeLifetime { new_lifetime_id: 1 },
            StateTransition::Dispose,
        ];

        let registry = StateScenarioRegistry::new();
        let mut service = StatefulService::new("TestService", ServiceLifetime::Singleton);

        // Apply transitions and verify state changes
        for transition in transitions {
            match transition {
                StateTransition::Register { name, lifetime_id } => {
                    service = StatefulService::new(&name, lifetime_from_id(lifetime_id));
                    service.state = ServiceState::Registered;
                }
                StateTransition::Initialize => {
                    if matches!(service.state, ServiceState::Registered) {
                        service.state = ServiceState::Initialized;
                    }
                }
                StateTransition::Dispose => {
                    if matches!(service.state, ServiceState::Initialized) {
                        service.state = ServiceState::Unregistered;
                    }
                }
                StateTransition::AddDependency { dependency_count } => {
                    service.metadata.dependencies =
                        vec![TypeId::of::<u8>(); usize::from(dependency_count)];
                }
                _ => {}
            }
        }

        assert_eq!(service.metadata.dependencies.len(), 2);
        registry.services.lock().unwrap().push(service);
        let services = registry.services.lock().unwrap();
        assert_eq!(services.len(), 1);
        assert!(matches!(services[0].state, ServiceState::Unregistered));
    }

    #[test]
    fn test_lifetime_transitions() {
        let mut service = StatefulService::new("TestService", ServiceLifetime::Singleton);
        service.state = ServiceState::Registered;

        // Test lifetime changes
        let transitions = [0, 1, 2].map(|id| StateTransition::ChangeLifetime {
            new_lifetime_id: id,
        });

        for transition in transitions {
            if let StateTransition::ChangeLifetime { new_lifetime_id } = transition {
                let new_lifetime = lifetime_from_id(new_lifetime_id);
                service.metadata.lifetime = new_lifetime;
            }
        }
    }
}
