//! Deterministic metadata contract variations.

use crate::lifetime_utils;
use lily_injection_registry::*;
use std::any::TypeId;
use std::sync::Arc;

/// Mutation types that can be applied to ServiceMetadata
#[derive(Debug, Clone)]
enum Mutation {
    // Lifetime mutations
    ChangeLifetime(ServiceLifetime),

    // Dependency mutations
    AddDependency(TypeId),
    RemoveDependency,
    ReplaceDependency(TypeId, TypeId),

    // Type mutations
    ChangeTypeId(TypeId),
    AddTraitImplementation(TypeId),
    RemoveTraitImplementation,

    // Factory mutations
    ModifyFactory(bool), // true = success, false = failure
}

/// Applies mutations to ServiceMetadata
fn apply_mutation(metadata: &mut ServiceMetadata, mutation: Mutation) {
    match mutation {
        Mutation::ChangeLifetime(lifetime) => {
            metadata.lifetime = lifetime;
        }

        Mutation::AddDependency(type_id) => {
            if !metadata.dependencies.contains(&type_id) {
                metadata.dependencies.push(type_id);
            }
        }

        Mutation::RemoveDependency => {
            metadata.dependencies.pop();
        }

        Mutation::ReplaceDependency(old_id, new_id) => {
            if let Some(pos) = metadata.dependencies.iter().position(|&id| id == old_id) {
                metadata.dependencies[pos] = new_id;
            }
        }

        Mutation::ChangeTypeId(type_id) => {
            metadata.type_id = type_id;
        }

        Mutation::AddTraitImplementation(trait_id) => {
            metadata.trait_type_id = Some(trait_id);
            metadata.trait_name = Some("MutatedTrait");
        }

        Mutation::RemoveTraitImplementation => {
            metadata.trait_type_id = None;
            metadata.trait_name = None;
        }

        Mutation::ModifyFactory(success) => {
            // Create non-capturing factory functions
            metadata.factory_fn = if success {
                |_| {
                    Box::pin(async {
                        Ok(Box::new(String::new()) as Box<dyn std::any::Any + Send + Sync>)
                    })
                }
            } else {
                |_| {
                    Box::pin(async {
                        Err(lily_error::injection::InjectionError::ServiceNotFound(
                            "Mutation-induced failure".to_string(),
                        ))
                    })
                }
            };
        }
    }
}

#[tokio::test]
async fn test_lifetime_mutations() {
    // Test all possible lifetime mutations
    let lifetimes = vec![
        ServiceLifetime::Singleton,
        ServiceLifetime::Scoped,
        ServiceLifetime::Transient,
    ];

    let mut metadata = ServiceMetadata {
        type_id: TypeId::of::<String>(),
        type_name: "MutationTest",
        trait_type_id: None,
        trait_name: None,
        lifetime: ServiceLifetime::Singleton,
        factory_fn: |_| {
            Box::pin(async { Ok(Box::new(String::new()) as Box<dyn std::any::Any + Send + Sync>) })
        },
        dependencies: vec![],
    };

    for lifetime in lifetimes {
        apply_mutation(&mut metadata, Mutation::ChangeLifetime(lifetime));
        assert_eq!(metadata.lifetime, lifetime);

        // Verify lifetime compatibility
        if let Some(_dep_id) = metadata.dependencies.first() {
            let result = lifetime_utils::validate_lifetime_compatibility(
                metadata.lifetime,
                ServiceLifetime::Singleton,
            );
            match metadata.lifetime {
                ServiceLifetime::Singleton => {
                    // Singleton can only depend on other singletons
                    assert_eq!(result.is_ok(), true);
                }
                _ => {
                    // Other lifetimes can depend on singletons
                    assert!(result.is_ok());
                }
            }
        }
    }
}

#[tokio::test]
async fn test_dependency_mutations() {
    let mut metadata = ServiceMetadata {
        type_id: TypeId::of::<String>(),
        type_name: "DependencyTest",
        trait_type_id: None,
        trait_name: None,
        lifetime: ServiceLifetime::Singleton,
        factory_fn: |_| {
            Box::pin(async { Ok(Box::new(String::new()) as Box<dyn std::any::Any + Send + Sync>) })
        },
        dependencies: vec![],
    };

    // Test adding dependencies
    let dep_types = vec![
        TypeId::of::<i32>(),
        TypeId::of::<u64>(),
        TypeId::of::<bool>(),
    ];

    for dep_type in &dep_types {
        apply_mutation(&mut metadata, Mutation::AddDependency(*dep_type));
        assert!(metadata.dependencies.contains(dep_type));
    }

    // Test replacing dependencies
    let new_type = TypeId::of::<f64>();
    if let Some(&first_dep) = metadata.dependencies.first() {
        apply_mutation(
            &mut metadata,
            Mutation::ReplaceDependency(first_dep, new_type),
        );
        assert!(metadata.dependencies.contains(&new_type));
    }

    // Test removing dependencies
    let initial_count = metadata.dependencies.len();
    apply_mutation(&mut metadata, Mutation::RemoveDependency);
    assert_eq!(metadata.dependencies.len(), initial_count - 1);
}

#[tokio::test]
async fn test_trait_mutations() {
    let mut metadata = ServiceMetadata {
        type_id: TypeId::of::<String>(),
        type_name: "TraitTest",
        trait_type_id: None,
        trait_name: None,
        lifetime: ServiceLifetime::Singleton,
        factory_fn: |_| {
            Box::pin(async { Ok(Box::new(String::new()) as Box<dyn std::any::Any + Send + Sync>) })
        },
        dependencies: vec![],
    };

    // Test adding trait implementation
    let trait_id = TypeId::of::<i32>();
    apply_mutation(&mut metadata, Mutation::AddTraitImplementation(trait_id));
    assert_eq!(metadata.trait_type_id, Some(trait_id));
    assert_eq!(metadata.trait_name, Some("MutatedTrait"));

    // Test removing trait implementation
    apply_mutation(&mut metadata, Mutation::RemoveTraitImplementation);
    assert_eq!(metadata.trait_type_id, None);
    assert_eq!(metadata.trait_name, None);
}

#[tokio::test]
async fn test_factory_mutations() {
    let mut metadata = ServiceMetadata {
        type_id: TypeId::of::<String>(),
        type_name: "FactoryTest",
        trait_type_id: None,
        trait_name: None,
        lifetime: ServiceLifetime::Singleton,
        factory_fn: |_| {
            Box::pin(async { Ok(Box::new(String::new()) as Box<dyn std::any::Any + Send + Sync>) })
        },
        dependencies: vec![],
    };

    // Test successful factory
    apply_mutation(&mut metadata, Mutation::ModifyFactory(true));
    let extensions = Arc::new(()) as Arc<dyn std::any::Any + Send + Sync>;
    let result = (metadata.factory_fn)(extensions).await;
    assert!(result.is_ok());

    // Test failing factory
    apply_mutation(&mut metadata, Mutation::ModifyFactory(false));
    let extensions = Arc::new(()) as Arc<dyn std::any::Any + Send + Sync>;
    let result = (metadata.factory_fn)(extensions).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_combined_mutations() {
    let mut metadata = ServiceMetadata {
        type_id: TypeId::of::<String>(),
        type_name: "CombinedTest",
        trait_type_id: None,
        trait_name: None,
        lifetime: ServiceLifetime::Singleton,
        factory_fn: |_| {
            Box::pin(async { Ok(Box::new(String::new()) as Box<dyn std::any::Any + Send + Sync>) })
        },
        dependencies: vec![],
    };

    // Apply multiple mutations in sequence
    let mutations = vec![
        Mutation::ChangeLifetime(ServiceLifetime::Scoped),
        Mutation::AddDependency(TypeId::of::<i32>()),
        Mutation::AddTraitImplementation(TypeId::of::<u64>()),
        Mutation::ModifyFactory(true),
    ];

    for mutation in mutations {
        apply_mutation(&mut metadata, mutation.clone());
    }

    // Verify final state
    assert_eq!(metadata.lifetime, ServiceLifetime::Scoped);
    assert!(!metadata.dependencies.is_empty());
    assert!(metadata.trait_type_id.is_some());

    let extensions = Arc::new(()) as Arc<dyn std::any::Any + Send + Sync>;
    let result = (metadata.factory_fn)(extensions).await;
    assert!(result.is_ok());
}
