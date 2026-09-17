//! Deterministic registration-sequence input variations.

use crate::{analyze_dependencies, lifetime_utils, ServiceRegistrar};
use arbitrary::Arbitrary;
use lily_injection_registry::*;
use std::any::TypeId;
use std::sync::{Arc, Mutex};

#[derive(Arbitrary, Debug)]
enum RegistrationAction {
    RegisterService {
        name: String,
        lifetime_id: u8,
        dependency_count: u8,
    },
    ValidateLifetime {
        service_lifetime_id: u8,
        dependency_lifetime_id: u8,
    },
    AnalyzeDependencies,
}

struct RecordingRegistrar {
    services: Arc<Mutex<Vec<ServiceMetadata>>>,
}

impl RecordingRegistrar {
    fn new() -> Self {
        Self {
            services: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl crate::ServiceRegistrar for RecordingRegistrar {
    fn register_service<F>(&mut self, type_id: TypeId, lifetime: ServiceLifetime, factory: F)
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
        // The deterministic scenario uses a non-capturing factory.
        let metadata = ServiceMetadata {
            type_id,
            type_name: "VariationService",
            trait_type_id: None,
            trait_name: None,
            lifetime,
            factory_fn: |_| {
                Box::pin(async {
                    Ok(Box::new(String::new()) as Box<dyn std::any::Any + Send + Sync>)
                })
            },
            dependencies: vec![],
        };

        self.services.lock().unwrap().push(metadata);
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
    fn recording_registrar_records_registration() {
        let mut registrar = RecordingRegistrar::new();

        // Test basic registration
        registrar.register_service(TypeId::of::<String>(), ServiceLifetime::Singleton, |_| {
            Box::pin(async { Ok(Box::new(String::new()) as Box<dyn std::any::Any + Send + Sync>) })
        });

        assert_eq!(registrar.services.lock().unwrap().len(), 1);
    }

    #[test]
    fn test_lifetime_conversion() {
        assert_eq!(lifetime_from_id(0), ServiceLifetime::Singleton);
        assert_eq!(lifetime_from_id(1), ServiceLifetime::Scoped);
        assert_eq!(lifetime_from_id(2), ServiceLifetime::Transient);
        assert_eq!(lifetime_from_id(3), ServiceLifetime::Singleton);
    }

    #[test]
    fn registration_actions_are_processed() {
        let actions = vec![
            RegistrationAction::RegisterService {
                name: "TestService".to_string(),
                lifetime_id: 0,
                dependency_count: 2,
            },
            RegistrationAction::ValidateLifetime {
                service_lifetime_id: 0,
                dependency_lifetime_id: 0,
            },
            RegistrationAction::AnalyzeDependencies,
        ];

        // Verify actions can be processed without panicking
        let mut registrar = RecordingRegistrar::new();
        for action in actions {
            match action {
                RegistrationAction::RegisterService {
                    name: _,
                    lifetime_id,
                    dependency_count,
                } => {
                    let lifetime = lifetime_from_id(lifetime_id);
                    let dependencies = (0..dependency_count % 5)
                        .map(|_| TypeId::of::<i32>())
                        .collect();

                    let metadata = ServiceMetadata {
                        type_id: TypeId::of::<String>(),
                        type_name: "VariationService",
                        trait_type_id: None,
                        trait_name: None,
                        lifetime,
                        factory_fn: |_| {
                            Box::pin(async {
                                Ok(Box::new(String::new()) as Box<dyn std::any::Any + Send + Sync>)
                            })
                        },
                        dependencies,
                    };

                    registrar.register_service(
                        metadata.type_id,
                        metadata.lifetime,
                        metadata.factory_fn,
                    );
                }
                RegistrationAction::ValidateLifetime {
                    service_lifetime_id,
                    dependency_lifetime_id,
                } => {
                    let service_lifetime = lifetime_from_id(service_lifetime_id);
                    let dependency_lifetime = lifetime_from_id(dependency_lifetime_id);
                    let _ = lifetime_utils::validate_lifetime_compatibility(
                        service_lifetime,
                        dependency_lifetime,
                    );
                }
                RegistrationAction::AnalyzeDependencies => {
                    let _ = analyze_dependencies();
                }
            }
        }
    }
}
