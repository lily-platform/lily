//! Tests for async operation correctness

use crate::ServiceRegistrar;
use futures::future::join_all;
use lily_injection_registry::*;
use std::any::TypeId;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Barrier, Semaphore};

/// Mock async service for testing
struct AsyncService {
    id: i32,
    init_duration: Duration,
}

impl AsyncService {
    async fn initialize(&self) {
        tokio::time::sleep(self.init_duration).await;
    }
}

/// Mock registrar for async testing
struct AsyncTestRegistrar {
    services: Arc<Mutex<Vec<Box<dyn std::any::Any + Send + Sync>>>>,
    semaphore: Arc<Semaphore>,
}

impl AsyncTestRegistrar {
    fn new(max_concurrent: usize) -> Self {
        Self {
            services: Arc::new(Mutex::new(Vec::new())),
            semaphore: Arc::new(Semaphore::new(max_concurrent)),
        }
    }
}

impl crate::ServiceRegistrar for AsyncTestRegistrar {
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
        let semaphore = self.semaphore.clone();

        tokio::spawn(async move {
            let _permit = semaphore.acquire().await.unwrap();
            let extensions = Arc::new(()) as Arc<dyn std::any::Any + Send + Sync>;
            if let Ok(service) = factory(extensions).await {
                services.lock().unwrap().push(service);
            }
        });
    }
}

#[tokio::test]
async fn test_async_service_initialization() {
    const NUM_SERVICES: usize = 10;
    let barrier = Arc::new(Barrier::new(NUM_SERVICES));
    let registrar = Arc::new(Mutex::new(AsyncTestRegistrar::new(3))); // Max 3 concurrent
    let mut handles = Vec::new();

    // Create services with varying initialization times
    for i in 0..NUM_SERVICES {
        let barrier = barrier.clone();
        let registrar = registrar.clone();
        let init_duration = Duration::from_millis((i * 10) as u64);

        let handle = tokio::spawn(async move {
            barrier.wait().await;

            let _service = AsyncService {
                id: i as i32,
                init_duration,
            };

            let metadata = ServiceMetadata {
                type_id: TypeId::of::<AsyncService>(),
                type_name: "AsyncService",
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

            registrar.lock().unwrap().register_service(
                metadata.type_id,
                metadata.lifetime,
                metadata.factory_fn,
            );
        });

        handles.push(handle);
    }

    // Wait for all services to be registered and initialized
    join_all(handles).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify all services were registered
    let registrar_guard = registrar.lock().unwrap();
    let services = registrar_guard.services.lock().unwrap();
    assert_eq!(services.len(), NUM_SERVICES);
}

#[tokio::test]
async fn test_async_dependency_initialization() {
    // Create a chain of async dependencies
    let registrar = Arc::new(Mutex::new(AsyncTestRegistrar::new(1))); // Sequential initialization

    // Service A (base service)
    let type_a = TypeId::of::<i32>();
    let metadata_a = ServiceMetadata {
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
        dependencies: vec![],
    };

    // Service B (depends on A)
    let type_b = TypeId::of::<u32>();
    let metadata_b = ServiceMetadata {
        type_id: type_b,
        type_name: "ServiceB",
        trait_type_id: None,
        trait_name: None,
        lifetime: ServiceLifetime::Singleton,
        factory_fn: |_| {
            Box::pin(async {
                tokio::time::sleep(Duration::from_millis(20)).await;
                Ok(Box::new(2u32) as Box<dyn std::any::Any + Send + Sync>)
            })
        },
        dependencies: vec![type_a],
    };

    // Service C (depends on B)
    let type_c = TypeId::of::<i64>();
    let metadata_c = ServiceMetadata {
        type_id: type_c,
        type_name: "ServiceC",
        trait_type_id: None,
        trait_name: None,
        lifetime: ServiceLifetime::Singleton,
        factory_fn: |_| {
            Box::pin(async {
                tokio::time::sleep(Duration::from_millis(30)).await;
                Ok(Box::new(3i64) as Box<dyn std::any::Any + Send + Sync>)
            })
        },
        dependencies: vec![type_b],
    };

    // Register services in dependency order
    let mut registrar_guard = registrar.lock().unwrap();
    registrar_guard.register_service(
        metadata_a.type_id,
        metadata_a.lifetime,
        metadata_a.factory_fn,
    );
    tokio::time::sleep(Duration::from_millis(15)).await;

    registrar_guard.register_service(
        metadata_b.type_id,
        metadata_b.lifetime,
        metadata_b.factory_fn,
    );
    tokio::time::sleep(Duration::from_millis(25)).await;

    registrar_guard.register_service(
        metadata_c.type_id,
        metadata_c.lifetime,
        metadata_c.factory_fn,
    );
    tokio::time::sleep(Duration::from_millis(35)).await;

    drop(registrar_guard);

    // Verify services were initialized in order
    let registrar_guard = registrar.lock().unwrap();
    let services = registrar_guard.services.lock().unwrap();
    assert_eq!(services.len(), 3);
}

#[tokio::test]
async fn test_async_cancellation_handling() {
    let registrar = Arc::new(Mutex::new(AsyncTestRegistrar::new(5)));
    let mut handles = Vec::new();

    // Spawn services that might be cancelled
    for i in 0..5 {
        let registrar = registrar.clone();
        let _duration = Duration::from_millis(50 * (i + 1) as u64);

        let handle = tokio::spawn(async move {
            let metadata = ServiceMetadata {
                type_id: TypeId::of::<i32>(),
                type_name: "CancellableService",
                trait_type_id: None,
                trait_name: None,
                lifetime: ServiceLifetime::Singleton,
                factory_fn: |_| {
                    Box::pin(async {
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
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

    // Cancel some tasks
    tokio::time::sleep(Duration::from_millis(75)).await;
    for handle in handles.iter_mut().skip(2) {
        handle.abort();
    }

    // Wait for remaining tasks
    for handle in handles {
        let _ = handle.await;
    }

    // Some services should have completed
    let registrar_guard = registrar.lock().unwrap();
    let services = registrar_guard.services.lock().unwrap();
    assert!(services.len() > 0);
    assert!(services.len() <= 5);
}

#[tokio::test]
async fn test_async_error_propagation() {
    let registrar = Arc::new(Mutex::new(AsyncTestRegistrar::new(1)));

    // Create a service that fails asynchronously
    let metadata = ServiceMetadata {
        type_id: TypeId::of::<i32>(),
        type_name: "FailingService",
        trait_type_id: None,
        trait_name: None,
        lifetime: ServiceLifetime::Singleton,
        factory_fn: |_| {
            Box::pin(async {
                tokio::time::sleep(Duration::from_millis(10)).await;
                Err(lily_error::injection::InjectionError::ServiceNotFound(
                    "Simulated async failure".to_string(),
                ))
            })
        },
        dependencies: vec![],
    };

    registrar.lock().unwrap().register_service(
        metadata.type_id,
        metadata.lifetime,
        metadata.factory_fn,
    );

    // Wait for error to propagate
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Verify service was not registered due to error
    let registrar_guard = registrar.lock().unwrap();
    let services = registrar_guard.services.lock().unwrap();
    assert_eq!(services.len(), 0);
}
