// These suites intentionally define many compile-contract fixtures and
// deterministic input models. Keep those narrowly test-only diagnostics from
// obscuring actionable production lints while still running Clippy over every
// target.
#![allow(dead_code, deprecated, non_local_definitions, unexpected_cfgs)]
#![allow(unnameable_test_items, unused_imports, unused_variables)]
#![allow(
    clippy::await_holding_lock,
    clippy::bool_assert_comparison,
    clippy::clone_on_copy,
    clippy::empty_line_after_doc_comments,
    clippy::enum_variant_names,
    clippy::len_zero,
    clippy::map_entry,
    clippy::new_without_default,
    clippy::redundant_pattern_matching,
    clippy::single_match,
    clippy::type_complexity,
    clippy::uninlined_format_args,
    clippy::useless_vec
)]

pub mod concurrency;
pub mod contract;
pub mod contract_variation;
pub mod fixtures;
pub mod input_variation;
pub mod integration;
pub mod property_based;
pub mod state_transition;
pub mod unit;

/// Test-only registration sink used by legacy stress fixtures.
///
/// Production registration is link-time metadata consumed directly by
/// `lily_injection`; this trait is deliberately not exported by the crate.
pub trait ServiceRegistrar {
    fn register_service<F>(
        &mut self,
        type_id: std::any::TypeId,
        lifetime: lily_injection_registry::ServiceLifetime,
        factory: F,
    ) where
        F: Fn(
                std::sync::Arc<dyn std::any::Any + Send + Sync>,
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
            + 'static;
}

/// Test model for lifetime properties exercised by the legacy stress suites.
pub mod lifetime_utils {
    use lily_injection_registry::ServiceLifetime;

    pub fn validate_lifetime_compatibility(
        service: ServiceLifetime,
        dependency: ServiceLifetime,
    ) -> Result<(), String> {
        match (service, dependency) {
            (ServiceLifetime::Singleton, ServiceLifetime::Scoped) => Err(
                "Singleton service cannot depend on Scoped service. Use factory pattern or make dependency Singleton."
                    .to_string(),
            ),
            _ => Ok(()),
        }
    }

    pub fn requires_context(lifetime: ServiceLifetime) -> bool {
        matches!(lifetime, ServiceLifetime::Scoped)
    }

    pub fn lifetime_description(lifetime: ServiceLifetime) -> &'static str {
        match lifetime {
            ServiceLifetime::Singleton => "Singleton (one instance per application)",
            ServiceLifetime::Scoped => "Scoped (one instance per request/process)",
            ServiceLifetime::Transient => "Transient (new instance every time)",
        }
    }
}

/// Test-only convenience over the production metadata and graph validators.
pub fn analyze_dependencies() -> Result<Vec<std::any::TypeId>, lily_error::injection::InjectionError>
{
    let routes = lily_injection_registry::get_all_service_route_metadata();
    lily_injection_registry::validate_service_route_metadata(&routes)?;
    let metadata = lily_injection_registry::get_all_service_metadata();
    lily_injection_registry::analyze_service_graph(&metadata)
}
