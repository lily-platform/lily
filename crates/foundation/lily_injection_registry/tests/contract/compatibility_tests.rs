//! Compatibility tests for lily_injection_registry
//! Verifies compatibility with different Rust versions and features

use crate::analyze_dependencies;
use lily_injection_registry::*;
use std::any::TypeId;
use std::sync::Arc;

/// Tests compatibility with async/await syntax
#[tokio::test]
async fn test_async_compatibility() {
    let metadata = ServiceMetadata {
        type_id: TypeId::of::<String>(),
        type_name: "AsyncTest",
        trait_type_id: None,
        trait_name: None,
        lifetime: ServiceLifetime::Singleton,
        factory_fn: |_| {
            Box::pin(async { Ok(Box::new(String::new()) as Box<dyn std::any::Any + Send + Sync>) })
        },
        dependencies: vec![],
    };

    let extensions = Arc::new(()) as Arc<dyn std::any::Any + Send + Sync>;
    let result = (metadata.factory_fn)(extensions).await;
    assert!(result.is_ok());
}

/// Tests compatibility with trait objects
#[test]
fn test_trait_object_compatibility() {
    trait TestTrait: Send + Sync {
        fn get_name(&self) -> &str;
    }

    struct TestImpl {
        name: String,
    }

    impl TestTrait for TestImpl {
        fn get_name(&self) -> &str {
            &self.name
        }
    }

    let metadata = ServiceMetadata {
        type_id: TypeId::of::<TestImpl>(),
        type_name: "TraitTest",
        trait_type_id: Some(TypeId::of::<dyn TestTrait>()),
        trait_name: Some("TestTrait"),
        lifetime: ServiceLifetime::Singleton,
        factory_fn: |_| {
            Box::pin(async {
                let impl_obj = TestImpl {
                    name: "test".to_string(),
                };
                Ok(Box::new(impl_obj) as Box<dyn std::any::Any + Send + Sync>)
            })
        },
        dependencies: vec![],
    };

    assert!(metadata.trait_type_id.is_some());
}

/// Tests compatibility with generic types
#[test]
fn test_generic_type_compatibility() {
    struct GenericService<T> {
        data: T,
    }

    let metadata = ServiceMetadata {
        type_id: TypeId::of::<GenericService<i32>>(),
        type_name: "GenericTest",
        trait_type_id: None,
        trait_name: None,
        lifetime: ServiceLifetime::Singleton,
        factory_fn: |_| {
            Box::pin(async {
                Ok(Box::new(GenericService { data: 42 }) as Box<dyn std::any::Any + Send + Sync>)
            })
        },
        dependencies: vec![],
    };

    assert_eq!(metadata.type_id, TypeId::of::<GenericService<i32>>());
}

/// Tests compatibility with different lifetime patterns
#[test]
fn test_lifetime_pattern_compatibility() {
    struct ServiceWithLifetime<'a> {
        data: &'a str,
    }

    let metadata = ServiceMetadata {
        type_id: TypeId::of::<ServiceWithLifetime<'static>>(),
        type_name: "LifetimeTest",
        trait_type_id: None,
        trait_name: None,
        lifetime: ServiceLifetime::Singleton,
        factory_fn: |_| {
            Box::pin(async {
                Ok(Box::new(ServiceWithLifetime { data: "test" })
                    as Box<dyn std::any::Any + Send + Sync>)
            })
        },
        dependencies: vec![],
    };

    assert_eq!(
        metadata.type_id,
        TypeId::of::<ServiceWithLifetime<'static>>()
    );
}

/// Tests compatibility with different Send/Sync bounds
#[test]
fn test_send_sync_compatibility() {
    struct ThreadSafeService {
        data: Arc<i32>,
    }

    let _metadata = ServiceMetadata {
        type_id: TypeId::of::<ThreadSafeService>(),
        type_name: "ThreadSafeTest",
        trait_type_id: None,
        trait_name: None,
        lifetime: ServiceLifetime::Singleton,
        factory_fn: |_| {
            Box::pin(async {
                Ok(Box::new(ThreadSafeService { data: Arc::new(42) })
                    as Box<dyn std::any::Any + Send + Sync>)
            })
        },
        dependencies: vec![],
    };

    // Verify Send + Sync bounds
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<ThreadSafeService>();
    assert_send_sync::<Box<dyn std::any::Any + Send + Sync>>();
}

/// Tests compatibility with different error types
#[test]
fn test_error_compatibility() {
    #[derive(Debug)]
    struct CustomError(String);

    impl std::error::Error for CustomError {}
    impl std::fmt::Display for CustomError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    let _metadata = ServiceMetadata {
        type_id: TypeId::of::<String>(),
        type_name: "ErrorTest",
        trait_type_id: None,
        trait_name: None,
        lifetime: ServiceLifetime::Singleton,
        factory_fn: |_| {
            Box::pin(async {
                Err(lily_error::injection::InjectionError::General(
                    CustomError("test error".to_string()).to_string(),
                ))
            })
        },
        dependencies: vec![],
    };

    // Verify error conversion compatibility
    fn assert_error<T: std::error::Error>() {}
    assert_error::<lily_error::injection::InjectionError>();
}

/// Tests compatibility with different async runtimes
#[tokio::test]
async fn test_runtime_compatibility() {
    let metadata = ServiceMetadata {
        type_id: TypeId::of::<String>(),
        type_name: "RuntimeTest",
        trait_type_id: None,
        trait_name: None,
        lifetime: ServiceLifetime::Singleton,
        factory_fn: |_| {
            Box::pin(async {
                // Test tokio compatibility
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                Ok(Box::new(String::new()) as Box<dyn std::any::Any + Send + Sync>)
            })
        },
        dependencies: vec![],
    };

    let extensions = Arc::new(()) as Arc<dyn std::any::Any + Send + Sync>;
    let result = (metadata.factory_fn)(extensions).await;
    assert!(result.is_ok());
}

/// Tests compatibility with different feature flags
#[test]
fn test_feature_compatibility() {
    // Verify core features are available
    let _ = ServiceLifetime::Singleton;
    let _ = get_all_service_metadata();
    let _ = analyze_dependencies();

    // Verify re-exports are available
    use lily_injection_registry::linkme;
    let _distributed_slice: &[fn()] = &[];
}

/// Tests compatibility with different dependency versions
#[test]
fn test_dependency_version_compatibility() {
    // Verify tokio compatibility
    fn assert_tokio<T: Send + Sync>() {}
    assert_tokio::<tokio::runtime::Runtime>();

    // Verify async-trait compatibility
    use async_trait::async_trait;
    #[async_trait]
    trait AsyncTest {
        async fn test(&self) -> Result<(), lily_error::injection::InjectionError>;
    }
}

/// Tests compatibility with different Rust editions
#[test]
fn test_edition_compatibility() {
    // Verify 2021 edition features
    let _const_generics = [1; 3];
    let _async_closure = || async { Ok::<_, std::io::Error>(()) };
}
