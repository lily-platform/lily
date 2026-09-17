//! Scenario generators for creating test scenarios
//! Provides generators for complex test scenarios

use lily_injection_registry::*;
use std::any::TypeId;
use std::collections::HashMap;
use std::sync::Arc;

/// Represents a test scenario configuration
#[derive(Debug)]
pub struct ScenarioConfig {
    pub service_count: usize,
    pub max_dependencies: usize,
    pub include_traits: bool,
    pub include_failures: bool,
    pub circular_dependencies: bool,
}

impl Default for ScenarioConfig {
    fn default() -> Self {
        Self {
            service_count: 5,
            max_dependencies: 2,
            include_traits: false,
            include_failures: false,
            circular_dependencies: false,
        }
    }
}

/// Generates dependency graphs for testing
pub struct DependencyGraphGenerator;

impl DependencyGraphGenerator {
    /// Generates a linear dependency chain
    pub fn generate_linear_chain(length: usize) -> Vec<ServiceMetadata> {
        let mut services: Vec<ServiceMetadata> = Vec::with_capacity(length);

        for i in 0..length {
            let dependencies: Vec<TypeId> = if i > 0 {
                vec![services[i - 1].type_id]
            } else {
                vec![]
            };

            services.push(ServiceMetadata {
                type_id: TypeId::of::<String>(),
                type_name: "ChainService",
                trait_type_id: None,
                trait_name: None,
                lifetime: ServiceLifetime::Singleton,
                factory_fn: |_| {
                    Box::pin(async {
                        Ok(Box::new(String::new()) as Box<dyn std::any::Any + Send + Sync>)
                    })
                },
                dependencies,
            });
        }

        services
    }

    /// Generates a diamond dependency pattern
    pub fn generate_diamond_pattern() -> Vec<ServiceMetadata> {
        let type_a = TypeId::of::<i32>();
        let type_b = TypeId::of::<u32>();
        let type_c = TypeId::of::<i64>();
        let type_d = TypeId::of::<u64>();

        vec![
            // Top service (A)
            ServiceMetadata {
                type_id: type_a,
                type_name: "ServiceA",
                trait_type_id: None,
                trait_name: None,
                lifetime: ServiceLifetime::Singleton,
                factory_fn: |_| {
                    Box::pin(async { Ok(Box::new(1i32) as Box<dyn std::any::Any + Send + Sync>) })
                },
                dependencies: vec![],
            },
            // Middle services (B, C)
            ServiceMetadata {
                type_id: type_b,
                type_name: "ServiceB",
                trait_type_id: None,
                trait_name: None,
                lifetime: ServiceLifetime::Singleton,
                factory_fn: |_| {
                    Box::pin(async { Ok(Box::new(2u32) as Box<dyn std::any::Any + Send + Sync>) })
                },
                dependencies: vec![type_a],
            },
            ServiceMetadata {
                type_id: type_c,
                type_name: "ServiceC",
                trait_type_id: None,
                trait_name: None,
                lifetime: ServiceLifetime::Singleton,
                factory_fn: |_| {
                    Box::pin(async { Ok(Box::new(3i64) as Box<dyn std::any::Any + Send + Sync>) })
                },
                dependencies: vec![type_a],
            },
            // Bottom service (D)
            ServiceMetadata {
                type_id: type_d,
                type_name: "ServiceD",
                trait_type_id: None,
                trait_name: None,
                lifetime: ServiceLifetime::Singleton,
                factory_fn: |_| {
                    Box::pin(async { Ok(Box::new(4u64) as Box<dyn std::any::Any + Send + Sync>) })
                },
                dependencies: vec![type_b, type_c],
            },
        ]
    }

    /// Generates a complex dependency graph based on configuration
    pub fn generate_complex_graph(config: ScenarioConfig) -> Vec<ServiceMetadata> {
        let mut services: Vec<ServiceMetadata> = Vec::with_capacity(config.service_count);
        let mut type_map = HashMap::new();

        // Create base services
        for i in 0..config.service_count {
            let type_id = TypeId::of::<String>();
            type_map.insert(i, type_id);

            let mut dependencies = Vec::new();
            if !config.circular_dependencies {
                // Add dependencies only to previous services to avoid cycles
                for j in 0..i {
                    if dependencies.len() < config.max_dependencies {
                        dependencies.push(*type_map.get(&j).unwrap());
                    }
                }
            } else {
                // Allow circular dependencies
                for j in 0..config.service_count {
                    if j != i && dependencies.len() < config.max_dependencies {
                        dependencies.push(*type_map.get(&j).unwrap_or(&type_id));
                    }
                }
            }

            let trait_info = if config.include_traits && i % 2 == 0 {
                (Some(TypeId::of::<i32>()), Some("TestTrait"))
            } else {
                (None, None)
            };

            let lifetime = ServiceLifetime::Singleton;

            services.push(ServiceMetadata {
                type_id,
                type_name: "GeneratedService",
                trait_type_id: trait_info.0,
                trait_name: trait_info.1,
                lifetime,
                factory_fn: |_| {
                    Box::pin(async {
                        Ok(Box::new(String::new()) as Box<dyn std::any::Any + Send + Sync>)
                    })
                },
                dependencies,
            });
        }

        services
    }
}

/// Generates lifetime scenarios for testing
pub struct LifetimeScenarioGenerator;

impl LifetimeScenarioGenerator {
    /// Generates all possible lifetime combinations
    pub fn generate_lifetime_combinations() -> Vec<(ServiceLifetime, ServiceLifetime)> {
        let lifetimes = vec![
            ServiceLifetime::Singleton,
            ServiceLifetime::Scoped,
            ServiceLifetime::Transient,
        ];

        let mut combinations = Vec::new();
        for &service_lifetime in &lifetimes {
            for &dependency_lifetime in &lifetimes {
                combinations.push((service_lifetime, dependency_lifetime));
            }
        }

        combinations
    }

    /// Generates valid lifetime scenarios
    pub fn generate_valid_scenarios() -> Vec<ServiceMetadata> {
        vec![
            // Singleton depending on Singleton
            ServiceMetadata {
                type_id: TypeId::of::<i32>(),
                type_name: "SingletonService",
                trait_type_id: None,
                trait_name: None,
                lifetime: ServiceLifetime::Singleton,
                factory_fn: |_| {
                    Box::pin(async { Ok(Box::new(1i32) as Box<dyn std::any::Any + Send + Sync>) })
                },
                dependencies: vec![],
            },
            // Scoped depending on Singleton
            ServiceMetadata {
                type_id: TypeId::of::<u32>(),
                type_name: "ScopedService",
                trait_type_id: None,
                trait_name: None,
                lifetime: ServiceLifetime::Scoped,
                factory_fn: |_| {
                    Box::pin(async { Ok(Box::new(2u32) as Box<dyn std::any::Any + Send + Sync>) })
                },
                dependencies: vec![TypeId::of::<i32>()],
            },
            // Transient depending on anything
            ServiceMetadata {
                type_id: TypeId::of::<i64>(),
                type_name: "TransientService",
                trait_type_id: None,
                trait_name: None,
                lifetime: ServiceLifetime::Transient,
                factory_fn: |_| {
                    Box::pin(async { Ok(Box::new(3i64) as Box<dyn std::any::Any + Send + Sync>) })
                },
                dependencies: vec![TypeId::of::<i32>(), TypeId::of::<u32>()],
            },
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dependency_graph_generator() {
        // Test linear chain
        let chain = DependencyGraphGenerator::generate_linear_chain(3);
        assert_eq!(chain.len(), 3);
        assert!(chain[1].dependencies.contains(&chain[0].type_id));
        assert!(chain[2].dependencies.contains(&chain[1].type_id));

        // Test diamond pattern
        let diamond = DependencyGraphGenerator::generate_diamond_pattern();
        assert_eq!(diamond.len(), 4);
        assert_eq!(diamond[3].dependencies.len(), 2);

        // Test complex graph
        let config = ScenarioConfig {
            service_count: 5,
            max_dependencies: 2,
            include_traits: true,
            include_failures: true,
            circular_dependencies: false,
        };
        let complex = DependencyGraphGenerator::generate_complex_graph(config);
        assert_eq!(complex.len(), 5);
    }

    #[test]
    fn test_lifetime_scenario_generator() {
        // Test lifetime combinations
        let combinations = LifetimeScenarioGenerator::generate_lifetime_combinations();
        assert_eq!(combinations.len(), 9); // 3x3 combinations

        // Test valid scenarios
        let valid_scenarios = LifetimeScenarioGenerator::generate_valid_scenarios();
        assert_eq!(valid_scenarios.len(), 3);

        // Verify lifetime rules
        assert_eq!(valid_scenarios[0].lifetime, ServiceLifetime::Singleton);
        assert_eq!(valid_scenarios[1].lifetime, ServiceLifetime::Scoped);
        assert_eq!(valid_scenarios[2].lifetime, ServiceLifetime::Transient);
    }
}
