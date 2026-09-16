//! Tests for potential race conditions in service registration and initialization

use crate::{lifetime_utils, ServiceRegistrar};
use lily_injection_registry::*;
use std::any::TypeId;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Barrier;

/// Mock service for testing concurrent initialization
#[derive(Debug)]
struct ConcurrentService {
    id: i32,
    initialized: bool,
}

impl ConcurrentService {
    fn new(id: i32) -> Self {
        Self {
            id,
            initialized: false,
        }
    }
}

/// Mock registrar for concurrent testing
struct ConcurrentRegistrar {
    services: Arc<Mutex<Vec<Box<dyn std::any::Any + Send + Sync>>>>,
    initialization_count: Arc<Mutex<i32>>,
}

impl ConcurrentRegistrar {
    fn new() -> Self {
        Self {
            services: Arc::new(Mutex::new(Vec::new())),
            initialization_count: Arc::new(Mutex::new(0)),
        }
    }
}

impl crate::ServiceRegistrar for ConcurrentRegistrar {
    fn register_service<F>(&mut self, _type_id: TypeId, _lifetime: ServiceLifetime, factory: F)
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
        let services = self.services.clone();
        let init_count = self.initialization_count.clone();

        tokio::spawn(async move {
            let extensions = Arc::new(()) as Arc<dyn std::any::Any + Send + Sync>;
            if let Ok(service) = factory(extensions).await {
                services.lock().unwrap().push(service);
                *init_count.lock().unwrap() += 1;
            }
        });
    }
}

#[tokio::test]
async fn test_concurrent_service_registration() {
    const NUM_SERVICES: i32 = 10;
    let barrier = Arc::new(Barrier::new(NUM_SERVICES as usize));
    let registrar = Arc::new(Mutex::new(ConcurrentRegistrar::new()));
    let mut handles = Vec::new();

    // Spawn multiple tasks that try to register services simultaneously
    for i in 0..NUM_SERVICES {
        let barrier = barrier.clone();
        let registrar = registrar.clone();

        let handle = tokio::spawn(async move {
            // Wait for all tasks to be ready
            barrier.wait().await;

            let metadata = ServiceMetadata {
                type_id: TypeId::of::<ConcurrentService>(),
                type_name: "ConcurrentService",
                trait_type_id: None,
                trait_name: None,
                lifetime: ServiceLifetime::Singleton,
                factory_fn: move |_| {
                    Box::pin(async move {
                        // Simulate some work
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        Ok(Box::new(String::new()) as Box<dyn std::any::Any + Send + Sync>)
                    })
                },
                dependencies: vec![],
            };

            registrar.lock().unwrap().register_service(
                metadata.type_id,
                metadata.lifetime,
                metadata.factory_fn,
            );
        });

        handles.push(handle);
    }

    // Wait for all registrations to complete
    for handle in handles {
        handle.await.unwrap();
    }

    // Give time for async operations to complete
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify all services were registered
    let registrar = registrar.lock().unwrap();
    assert_eq!(
        *registrar.initialization_count.lock().unwrap(),
        NUM_SERVICES
    );
    assert_eq!(
        registrar.services.lock().unwrap().len(),
        NUM_SERVICES as usize
    );
}

#[tokio::test]
async fn test_concurrent_dependency_resolution() {
    let barrier = Arc::new(Barrier::new(3)); // For 3 interdependent services
    let registrar = Arc::new(Mutex::new(ConcurrentRegistrar::new()));

    // Create three services with circular dependencies
    let type_a = TypeId::of::<i32>();
    let type_b = TypeId::of::<u32>();
    let type_c = TypeId::of::<i64>();

    let mut handles = Vec::new();

    // Service A depends on B
    let handle_a = {
        let barrier = barrier.clone();
        let registrar = registrar.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            let metadata = ServiceMetadata {
                type_id: type_a,
                type_name: "ServiceA",
                trait_type_id: None,
                trait_name: None,
                lifetime: ServiceLifetime::Singleton,
                factory_fn: |_| {
                    Box::pin(async {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        Ok(Box::new(1i32) as Box<dyn std::any::Any + Send + Sync>)
                    })
                },
                dependencies: vec![type_b],
            };

            registrar.lock().unwrap().register_service(
                metadata.type_id,
                metadata.lifetime,
                metadata.factory_fn,
            );
        })
    };
    handles.push(handle_a);

    // Service B depends on C
    let handle_b = {
        let barrier = barrier.clone();
        let registrar = registrar.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            let metadata = ServiceMetadata {
                type_id: type_b,
                type_name: "ServiceB",
                trait_type_id: None,
                trait_name: None,
                lifetime: ServiceLifetime::Singleton,
                factory_fn: |_| {
                    Box::pin(async {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        Ok(Box::new(2u32) as Box<dyn std::any::Any + Send + Sync>)
                    })
                },
                dependencies: vec![type_c],
            };

            registrar.lock().unwrap().register_service(
                metadata.type_id,
                metadata.lifetime,
                metadata.factory_fn,
            );
        })
    };
    handles.push(handle_b);

    // Service C depends on A
    let handle_c = {
        let barrier = barrier.clone();
        let registrar = registrar.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            let metadata = ServiceMetadata {
                type_id: type_c,
                type_name: "ServiceC",
                trait_type_id: None,
                trait_name: None,
                lifetime: ServiceLifetime::Singleton,
                factory_fn: |_| {
                    Box::pin(async {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        Ok(Box::new(3i64) as Box<dyn std::any::Any + Send + Sync>)
                    })
                },
                dependencies: vec![type_a],
            };

            registrar.lock().unwrap().register_service(
                metadata.type_id,
                metadata.lifetime,
                metadata.factory_fn,
            );
        })
    };
    handles.push(handle_c);

    // Wait for all registrations to complete
    for handle in handles {
        handle.await.unwrap();
    }

    // Give time for async operations to complete
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify services were registered despite circular dependencies
    let registrar = registrar.lock().unwrap();
    assert_eq!(*registrar.initialization_count.lock().unwrap(), 3);
    assert_eq!(registrar.services.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn test_concurrent_lifetime_validation() {
    let barrier = Arc::new(Barrier::new(3));
    let results = Arc::new(Mutex::new(Vec::new()));
    let mut handles = Vec::new();

    // Spawn multiple tasks that validate lifetimes concurrently
    for lifetime in [
        ServiceLifetime::Singleton,
        ServiceLifetime::Scoped,
        ServiceLifetime::Transient,
    ] {
        let barrier = barrier.clone();
        let results = results.clone();

        let handle = tokio::spawn(async move {
            barrier.wait().await;

            // Validate against all possible dependency lifetimes
            let validations = [
                ServiceLifetime::Singleton,
                ServiceLifetime::Scoped,
                ServiceLifetime::Transient,
            ]
            .iter()
            .map(|&dep_lifetime| {
                lifetime_utils::validate_lifetime_compatibility(lifetime, dep_lifetime)
            })
            .collect::<Vec<_>>();

            results.lock().unwrap().push(validations);
        });

        handles.push(handle);
    }

    // Wait for all validations to complete
    for handle in handles {
        handle.await.unwrap();
    }

    // Verify results
    let results = results.lock().unwrap();
    assert_eq!(results.len(), 3);

    // Verify validation rules were maintained
    for validation_set in results.iter() {
        assert_eq!(validation_set.len(), 3);
        // Singleton dependency should always be valid
        assert!(validation_set[0].is_ok());
    }
}
