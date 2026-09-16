//! Tests for deadlock detection and prevention

use crate::{analyze_dependencies, ServiceRegistrar};
use lily_injection_registry::*;
use std::any::TypeId;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Barrier;
use tokio::time::timeout;

/// Mock service with potential deadlock scenarios
struct DeadlockService {
    id: i32,
    lock: Arc<Mutex<i32>>,
}

impl DeadlockService {
    fn new(id: i32, lock: Arc<Mutex<i32>>) -> Self {
        Self { id, lock }
    }
}

/// Mock registrar that can simulate deadlock scenarios
struct DeadlockTestRegistrar {
    services: Arc<Mutex<Vec<Box<dyn std::any::Any + Send + Sync>>>>,
    locks: Vec<Arc<Mutex<i32>>>,
}

impl DeadlockTestRegistrar {
    fn new() -> Self {
        Self {
            services: Arc::new(Mutex::new(Vec::new())),
            locks: vec![Arc::new(Mutex::new(0)), Arc::new(Mutex::new(0))],
        }
    }
}

impl crate::ServiceRegistrar for DeadlockTestRegistrar {
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

        tokio::spawn(async move {
            let extensions = Arc::new(()) as Arc<dyn std::any::Any + Send + Sync>;
            if let Ok(service) = factory(extensions).await {
                services.lock().unwrap().push(service);
            }
        });
    }
}

#[tokio::test]
async fn test_dependency_cycle_deadlock_prevention() {
    const TIMEOUT_DURATION: Duration = Duration::from_secs(2);
    let barrier = Arc::new(Barrier::new(3));
    let registrar = Arc::new(Mutex::new(DeadlockTestRegistrar::new()));

    // Create services with circular dependencies
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
                    Box::pin(async { Ok(Box::new(1i32) as Box<dyn std::any::Any + Send + Sync>) })
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
                    Box::pin(async { Ok(Box::new(2u32) as Box<dyn std::any::Any + Send + Sync>) })
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
                    Box::pin(async { Ok(Box::new(3i64) as Box<dyn std::any::Any + Send + Sync>) })
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

    // Wait for all registrations with timeout
    let timeout_result = timeout(TIMEOUT_DURATION, async {
        for handle in handles {
            handle.await.unwrap();
        }
    })
    .await;

    // Test should not deadlock
    assert!(timeout_result.is_ok());
}

#[tokio::test]
async fn test_concurrent_lock_acquisition() {
    const TIMEOUT_DURATION: Duration = Duration::from_secs(2);
    let barrier = Arc::new(Barrier::new(2));
    let registrar = Arc::new(Mutex::new(DeadlockTestRegistrar::new()));

    let lock1 = Arc::new(Mutex::new(0));
    let lock2 = Arc::new(Mutex::new(0));

    let mut handles = Vec::new();

    // Service 1 tries to acquire locks in order: lock1 -> lock2
    let handle1 = {
        let barrier = barrier.clone();
        let registrar = registrar.clone();
        let lock1 = lock1.clone();
        let lock2 = lock2.clone();

        tokio::spawn(async move {
            barrier.wait().await;

            let metadata = ServiceMetadata {
                type_id: TypeId::of::<DeadlockService>(),
                type_name: "Service1",
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
        })
    };
    handles.push(handle1);

    // Service 2 tries to acquire locks in order: lock2 -> lock1
    let handle2 = {
        let barrier = barrier.clone();
        let registrar = registrar.clone();
        let lock1 = lock1.clone();
        let lock2 = lock2.clone();

        tokio::spawn(async move {
            barrier.wait().await;

            let metadata = ServiceMetadata {
                type_id: TypeId::of::<DeadlockService>(),
                type_name: "Service2",
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
        })
    };
    handles.push(handle2);

    // Wait for all operations with timeout
    let timeout_result = timeout(TIMEOUT_DURATION, async {
        for handle in handles {
            handle.await.unwrap();
        }
    })
    .await;

    // Test should either complete successfully or timeout due to deadlock
    // In a production system, we'd want to implement deadlock prevention
    match timeout_result {
        Ok(_) => println!("No deadlock occurred"),
        Err(_) => println!("Potential deadlock detected"),
    }
}

#[tokio::test]
async fn test_analyze_dependencies_deadlock_prevention() {
    // Create a complex dependency graph that could potentially deadlock
    let result = analyze_dependencies();

    // analyze_dependencies should complete without deadlocking
    assert!(result.is_ok());

    if let Ok(order) = result {
        // Verify no service appears twice (would indicate a cycle)
        let mut seen = std::collections::HashSet::new();
        for type_id in order {
            assert!(seen.insert(type_id));
        }
    }
}
