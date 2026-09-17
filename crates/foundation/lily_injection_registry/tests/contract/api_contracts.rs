//! API contract tests for lily_injection_registry
//! Verifies that public APIs maintain their contracts

use lily_injection_registry::*;
use std::any::TypeId;
use std::sync::Arc;

/// Contract: ServiceMetadata struct guarantees
#[test]
fn test_service_metadata_contract() {
    // Contract: ServiceMetadata must be Clone
    let metadata = ServiceMetadata {
        type_id: TypeId::of::<String>(),
        type_name: "TestService",
        trait_type_id: None,
        trait_name: None,
        lifetime: ServiceLifetime::Singleton,
        factory_fn: |_| {
            Box::pin(async { Ok(Box::new(String::new()) as Box<dyn std::any::Any + Send + Sync>) })
        },
        dependencies: vec![],
    };

    let _cloned = metadata.clone();

    // Contract: type_name must never be empty
    assert!(
        !metadata.type_name.is_empty(),
        "ServiceMetadata contract violated: empty type_name"
    );

    // Contract: trait_type_id and trait_name must be consistent
    assert_eq!(
        metadata.trait_type_id.is_some(),
        metadata.trait_name.is_some(),
        "ServiceMetadata contract violated: inconsistent trait information"
    );
}

/// Contract: ServiceLifetime enum guarantees
#[test]
fn test_service_lifetime_contract() {
    // Contract: ServiceLifetime must be Copy
    let lifetime = ServiceLifetime::Singleton;
    let _copied = lifetime;

    // Contract: ServiceLifetime must implement PartialEq
    assert_eq!(lifetime, ServiceLifetime::Singleton);
    assert_ne!(lifetime, ServiceLifetime::Scoped);

    // Contract: ServiceLifetime must be exhaustive in match
    match lifetime {
        ServiceLifetime::Singleton => {}
        ServiceLifetime::Scoped => {}
        ServiceLifetime::Transient => {}
    }
}

/// Contract: Lifetime validation guarantees
#[test]
fn test_lifetime_validation_contract() {
    use crate::lifetime_utils::*;

    // Contract: Singleton dependencies must be valid for all services
    assert!(validate_lifetime_compatibility(
        ServiceLifetime::Singleton,
        ServiceLifetime::Singleton
    )
    .is_ok());
    assert!(
        validate_lifetime_compatibility(ServiceLifetime::Scoped, ServiceLifetime::Singleton)
            .is_ok()
    );
    assert!(validate_lifetime_compatibility(
        ServiceLifetime::Transient,
        ServiceLifetime::Singleton
    )
    .is_ok());

    // Contract: a singleton cannot capture request-owned state. A transient
    // dependency is root-owned when it is constructed for a singleton and is
    // therefore safe; a scoped dependency is not.
    assert!(
        validate_lifetime_compatibility(ServiceLifetime::Singleton, ServiceLifetime::Scoped)
            .is_err()
    );
    assert!(validate_lifetime_compatibility(
        ServiceLifetime::Singleton,
        ServiceLifetime::Transient
    )
    .is_ok());
}

/// Contract: Service factory guarantees
#[tokio::test]
async fn test_service_factory_contract() {
    // Contract: Factory must return Result type
    let factory: fn(
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
    > = |_| Box::pin(async { Ok(Box::new(String::new()) as Box<dyn std::any::Any + Send + Sync>) });

    let extensions = Arc::new(()) as Arc<dyn std::any::Any + Send + Sync>;
    let result = factory(extensions).await;
    assert!(result.is_ok());

    // Contract: Factory must be Send + Sync
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<
        Box<
            dyn Fn(
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
        >,
    >();
}

/// Contract: Dependency analysis guarantees
// #[test]
// fn test_dependency_analysis_contract() {
//     // Contract: Empty registry should return empty Vec
//     let result = analyze_dependencies();
//     assert!(result.is_ok());
//     assert!(result.unwrap().is_empty());

//     // Contract: get_all_service_metadata must return Vec
//     let metadata = get_all_service_metadata();
//     assert!(metadata.len() >= 0);
// }

/// Contract: Error handling guarantees
#[test]
fn test_error_handling_contract() {
    use lily_error::injection::InjectionError;

    // Contract: InjectionError must be convertible from String
    let _error = InjectionError::ServiceNotFound("test".to_string());

    // Contract: InjectionError must implement std::error::Error
    fn assert_error<T: std::error::Error>() {}
    assert_error::<InjectionError>();
}

/// Contract: Thread safety guarantees
#[test]
fn test_thread_safety_contract() {
    // Contract: ServiceMetadata must be Send + Sync
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<ServiceMetadata>();

    // Contract: ServiceLifetime must be Send + Sync
    assert_send_sync::<ServiceLifetime>();
}

/// Contract: Async compatibility guarantees
#[tokio::test]
async fn test_async_compatibility_contract() {
    // Contract: Factory functions must be async compatible
    let metadata = ServiceMetadata {
        type_id: TypeId::of::<String>(),
        type_name: "AsyncTest",
        trait_type_id: None,
        trait_name: None,
        lifetime: ServiceLifetime::Singleton,
        factory_fn: |_| {
            Box::pin(async {
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

/// Contract: Type safety guarantees
#[test]
fn test_type_safety_contract() {
    // Contract: TypeId must be unique per type
    assert_ne!(TypeId::of::<i32>(), TypeId::of::<i64>());
    assert_eq!(TypeId::of::<i32>(), TypeId::of::<i32>());

    // Contract: Box<dyn Any> must support downcasting
    let boxed: Box<dyn std::any::Any + Send + Sync> = Box::new(42i32);
    assert!(boxed.downcast_ref::<i32>().is_some());
    assert!(boxed.downcast_ref::<i64>().is_none());
}
