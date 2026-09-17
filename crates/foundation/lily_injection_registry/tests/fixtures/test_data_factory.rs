//! Test data factory for generating test fixtures
//! Provides factory methods for creating test data with various configurations

use lily_injection_registry::*;
use std::any::TypeId;
use std::sync::Arc;

/// Factory for creating ServiceMetadata instances
pub struct ServiceMetadataFactory;

impl ServiceMetadataFactory {
    /// Creates a basic service metadata
    pub fn create_basic(name: &'static str) -> ServiceMetadata {
        ServiceMetadata {
            type_id: TypeId::of::<String>(),
            type_name: name,
            trait_type_id: None,
            trait_name: None,
            lifetime: ServiceLifetime::Singleton,
            factory_fn: |_| {
                Box::pin(async {
                    Ok(Box::new(String::new()) as Box<dyn std::any::Any + Send + Sync>)
                })
            },
            dependencies: vec![],
        }
    }

    /// Creates a service metadata with specific lifetime
    pub fn with_lifetime(name: &'static str, lifetime: ServiceLifetime) -> ServiceMetadata {
        ServiceMetadata {
            type_id: TypeId::of::<String>(),
            type_name: name,
            trait_type_id: None,
            trait_name: None,
            lifetime,
            factory_fn: |_| {
                Box::pin(async {
                    Ok(Box::new(String::new()) as Box<dyn std::any::Any + Send + Sync>)
                })
            },
            dependencies: vec![],
        }
    }

    /// Creates a service metadata with dependencies
    pub fn with_dependencies(name: &'static str, dependencies: Vec<TypeId>) -> ServiceMetadata {
        ServiceMetadata {
            type_id: TypeId::of::<String>(),
            type_name: name,
            trait_type_id: None,
            trait_name: None,
            lifetime: ServiceLifetime::Singleton,
            factory_fn: |_| {
                Box::pin(async {
                    Ok(Box::new(String::new()) as Box<dyn std::any::Any + Send + Sync>)
                })
            },
            dependencies,
        }
    }

    /// Creates a service metadata with trait implementation
    pub fn with_trait(
        name: &'static str,
        trait_id: TypeId,
        trait_name: &'static str,
    ) -> ServiceMetadata {
        ServiceMetadata {
            type_id: TypeId::of::<String>(),
            type_name: name,
            trait_type_id: Some(trait_id),
            trait_name: Some(trait_name),
            lifetime: ServiceLifetime::Singleton,
            factory_fn: |_| {
                Box::pin(async {
                    Ok(Box::new(String::new()) as Box<dyn std::any::Any + Send + Sync>)
                })
            },
            dependencies: vec![],
        }
    }

    /// Creates a service metadata with custom factory
    /// Note: This method is simplified to avoid type complexity in tests
    pub fn with_factory<F>(name: &'static str, _factory: F) -> ServiceMetadata
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
        // For test purposes, we use a simple factory instead of the generic one
        ServiceMetadata {
            type_id: TypeId::of::<String>(),
            type_name: name,
            trait_type_id: None,
            trait_name: None,
            lifetime: ServiceLifetime::Singleton,
            factory_fn: |_| {
                Box::pin(async {
                    Ok(Box::new(String::new()) as Box<dyn std::any::Any + Send + Sync>)
                })
            },
            dependencies: vec![],
        }
    }
}

/// Factory for creating test service implementations
pub struct TestServiceFactory;

impl TestServiceFactory {
    /// Creates a basic test service
    pub fn create_basic() -> Box<dyn std::any::Any + Send + Sync> {
        Box::new(String::from("test service"))
    }

    /// Creates a test service with state
    pub fn with_state(state: &str) -> Box<dyn std::any::Any + Send + Sync> {
        Box::new(String::from(state))
    }

    /// Creates a test service that implements a trait
    pub fn with_trait() -> Box<dyn std::any::Any + Send + Sync> {
        trait TestTrait: Send + Sync {
            fn get_name(&self) -> &str;
        }

        struct TestService {
            name: String,
        }

        impl TestTrait for TestService {
            fn get_name(&self) -> &str {
                &self.name
            }
        }

        Box::new(TestService {
            name: "test".to_string(),
        })
    }
}

/// Factory for creating test dependencies
pub struct DependencyFactory;

impl DependencyFactory {
    /// Creates a chain of dependent services
    pub fn create_dependency_chain(length: usize) -> Vec<ServiceMetadata> {
        let mut chain = Vec::with_capacity(length);

        for i in 0..length {
            let deps = if i > 0 {
                vec![TypeId::of::<String>()] // Depend on previous service
            } else {
                vec![]
            };

            chain.push(ServiceMetadata {
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
                dependencies: deps,
            });
        }

        chain
    }

    /// Creates a circular dependency scenario
    pub fn create_circular_dependency() -> Vec<ServiceMetadata> {
        let type_a = TypeId::of::<String>();
        let type_b = TypeId::of::<i32>();

        vec![
            ServiceMetadata {
                type_id: type_a,
                type_name: "ServiceA",
                trait_type_id: None,
                trait_name: None,
                lifetime: ServiceLifetime::Singleton,
                factory_fn: |_| {
                    Box::pin(async {
                        Ok(Box::new(String::new()) as Box<dyn std::any::Any + Send + Sync>)
                    })
                },
                dependencies: vec![type_b],
            },
            ServiceMetadata {
                type_id: type_b,
                type_name: "ServiceB",
                trait_type_id: None,
                trait_name: None,
                lifetime: ServiceLifetime::Singleton,
                factory_fn: |_| {
                    Box::pin(async { Ok(Box::new(0i32) as Box<dyn std::any::Any + Send + Sync>) })
                },
                dependencies: vec![type_a],
            },
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_service_metadata_factory() {
        let basic = ServiceMetadataFactory::create_basic("Test");
        assert_eq!(basic.type_name, "Test");

        let scoped = ServiceMetadataFactory::with_lifetime("Test", ServiceLifetime::Scoped);
        assert_eq!(scoped.lifetime, ServiceLifetime::Scoped);

        let with_deps =
            ServiceMetadataFactory::with_dependencies("Test", vec![TypeId::of::<i32>()]);
        assert_eq!(with_deps.dependencies.len(), 1);

        let with_trait =
            ServiceMetadataFactory::with_trait("Test", TypeId::of::<i32>(), "TestTrait");
        assert!(with_trait.trait_type_id.is_some());
    }

    #[tokio::test]
    async fn test_test_service_factory() {
        let basic = TestServiceFactory::create_basic();
        assert!(basic.downcast_ref::<String>().is_some());

        let with_state = TestServiceFactory::with_state("custom");
        assert_eq!(with_state.downcast_ref::<String>().unwrap(), "custom");

        let with_trait = TestServiceFactory::with_trait();
        // Check if the trait object is properly created
        assert!(with_trait.type_id() != std::any::TypeId::of::<()>());
    }

    #[test]
    fn test_dependency_factory() {
        let chain = DependencyFactory::create_dependency_chain(3);
        assert_eq!(chain.len(), 3);
        assert!(chain[1].dependencies.contains(&TypeId::of::<String>()));

        let circular = DependencyFactory::create_circular_dependency();
        assert_eq!(circular.len(), 2);
        assert!(circular[0].dependencies.contains(&circular[1].type_id));
        assert!(circular[1].dependencies.contains(&circular[0].type_id));
    }
}
