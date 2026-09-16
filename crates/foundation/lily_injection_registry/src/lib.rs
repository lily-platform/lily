#![cfg_attr(test, allow(clippy::items_after_test_module, clippy::type_complexity))]
#![deny(rustdoc::broken_intra_doc_links)]

//! Link-time registration ABI for Lily dependency injection.
//!
//! This crate connects `lily_injectable_derive` output to the
//! `lily_injection` runtime. It is intentionally **not** an application-facing
//! service container API. Application code should:
//!
//! - declare services with `lily_injection::Injectable`;
//! - implement `lily_injection::ServiceTrait`;
//! - resolve from `lily_injection::Extensions` (or a framework-provided typed
//!   service extractor).
//!
//! Do not construct metadata, inspect distributed slices or register factories
//! manually. Those public-but-hidden symbols form a proc-macro/runtime ABI:
//! they must be externally reachable because generated code is compiled in the
//! consuming crate. Normal HTTP, WebSocket and consumer applications should
//! also let their application builder own the single DI container.
//!

use std::any::TypeId;
use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use lily_error::injection::InjectionError;
// Proc-macro ABI: generated code must be able to name this path without
// exposing linkme as part of the application-facing documentation.
#[doc(hidden)]
pub use linkme;

mod lifetime_validation;
use lifetime_validation::lifetime_utils;

/// Service lifetime encoded in derive-generated registration metadata.
///
/// Application code should normally use the re-export at
/// `lily_injection::ServiceLifetime` or select a lifetime through
/// `#[service(lifetime = "...")]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceLifetime {
    /// One instance shared by one application container.
    Singleton,
    /// One instance per managed request/job scope.
    Scoped,
    /// One instance for every resolution.
    Transient,
}

/// Service metadata for compile-time registration
/// Contains all information needed to create and manage a service
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct ServiceMetadata {
    /// Unique type identifier for the service
    pub type_id: TypeId,
    /// Human-readable type name for debugging
    pub type_name: &'static str,
    /// Optional trait type ID for trait-based resolution
    pub trait_type_id: Option<TypeId>,
    /// Optional trait name for debugging
    pub trait_name: Option<&'static str>,
    /// Service lifetime management strategy
    pub lifetime: ServiceLifetime,
    /// Lifecycle-aware factory invoked by the owning application container.
    /// Takes that container's provider as `Arc<dyn Any>` to avoid a crate
    /// dependency cycle. Generated factories return only initialized services;
    /// initialization failures are returned to the composition root.
    #[allow(clippy::type_complexity)]
    pub factory_fn: fn(
        Arc<dyn std::any::Any + Send + Sync>,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        Box<dyn std::any::Any + Send + Sync>,
                        lily_error::injection::InjectionError,
                    >,
                > + Send
                + 'static,
        >,
    >,
    /// Dependencies of this service (TypeIds of injected services)
    pub dependencies: Vec<TypeId>,
}

/// Converts the concrete instance owned by a service descriptor into the
/// requested public service shape.
///
/// The returned `Box` contains an `Arc<TRequested>`. Keeping projection
/// separate from construction lets concrete and interface resolutions share
/// one descriptor, cache entry and lifecycle owner.
#[doc(hidden)]
pub type ServiceProjectionFn = fn(
    Arc<dyn std::any::Any + Send + Sync>,
) -> Result<Box<dyn std::any::Any + Send + Sync>, InjectionError>;

/// Identifies whether a route exposes the implementation itself or one of its
/// business interfaces.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceRouteKind {
    /// Route resolving the concrete implementation type.
    Concrete,
    /// Route projecting the implementation to a declared trait object.
    Interface,
}

/// Link-time route from a requested type to one concrete implementation.
///
/// Routes never own factories, lifetimes or disposal callbacks. Those remain
/// attached exclusively to `implementation_type_id`.
#[doc(hidden)]
#[derive(Clone, Copy)]
pub struct ServiceRouteMetadata {
    /// Type requested by the resolution caller.
    pub requested_type_id: TypeId,
    /// Human-readable requested type name.
    pub requested_type_name: &'static str,
    /// Concrete implementation owning the instance and lifecycle.
    pub implementation_type_id: TypeId,
    /// Human-readable concrete implementation name.
    pub implementation_type_name: &'static str,
    /// Whether the route is concrete or an interface projection.
    pub kind: ServiceRouteKind,
    /// Safe projection from the concrete allocation to the requested shape.
    pub project_fn: ServiceProjectionFn,
}

impl std::fmt::Debug for ServiceRouteMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServiceRouteMetadata")
            .field("requested_type_id", &self.requested_type_id)
            .field("requested_type_name", &self.requested_type_name)
            .field("implementation_type_id", &self.implementation_type_id)
            .field("implementation_type_name", &self.implementation_type_name)
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

#[linkme::distributed_slice]
#[doc(hidden)]
pub static SERVICE_ROUTE_GETTERS: [fn() -> &'static ServiceRouteMetadata] = [..];

/// Collect every concrete/interface route emitted by active derives.
#[doc(hidden)]
pub fn get_all_service_route_metadata() -> Vec<&'static ServiceRouteMetadata> {
    SERVICE_ROUTE_GETTERS
        .iter()
        .map(|getter| getter())
        .collect()
}

/// Validate an explicit route set without mutating the distributed registry.
#[doc(hidden)]
pub fn validate_service_route_metadata(
    routes: &[&ServiceRouteMetadata],
) -> Result<(), InjectionError> {
    let mut registrations: HashMap<TypeId, &ServiceRouteMetadata> = HashMap::new();

    for route in routes {
        if let Some(existing) = registrations.insert(route.requested_type_id, route) {
            let mut implementations = vec![
                existing.implementation_type_name.to_string(),
                route.implementation_type_name.to_string(),
            ];
            implementations.sort();
            implementations.dedup();

            if existing.kind == ServiceRouteKind::Interface
                && route.kind == ServiceRouteKind::Interface
            {
                return Err(InjectionError::DuplicateInterfaceBinding {
                    interface_type_id: route.requested_type_id,
                    interface: route.requested_type_name.to_string(),
                    implementations,
                });
            }

            return Err(InjectionError::AmbiguousRegistration {
                type_id: route.requested_type_id,
                services: implementations,
            });
        }
    }

    Ok(())
}

/// Type-erased lifecycle callback generated for every injectable service.
///
/// The callback receives the final `Arc` owned by a scope, recovers the
/// concrete service and invokes its asynchronous `ServiceTrait::dispose`
/// hook. It is kept in a separate distributed registry so manually-created
/// `ServiceMetadata` values remain source-compatible.
#[doc(hidden)]
pub type ServiceDisposeFn =
    fn(
        Arc<dyn std::any::Any + Send + Sync>,
    ) -> Pin<Box<dyn Future<Output = Result<(), InjectionError>> + Send + 'static>>;

/// Proc-macro ABI describing one generated service disposer.
#[doc(hidden)]
#[derive(Clone, Copy)]
pub struct ServiceDisposerMetadata {
    /// Concrete service type owning this disposer.
    pub type_id: TypeId,
    /// Type-erased disposer generated for the concrete service.
    pub dispose_fn: ServiceDisposeFn,
}

#[linkme::distributed_slice]
#[doc(hidden)]
pub static SERVICE_DISPOSER_GETTERS: [fn() -> &'static ServiceDisposerMetadata] = [..];

/// Find the lifecycle callback associated with a concrete service type.
#[doc(hidden)]
pub fn get_service_disposer(type_id: TypeId) -> Option<ServiceDisposeFn> {
    SERVICE_DISPOSER_GETTERS
        .iter()
        .map(|getter| getter())
        .find(|metadata| metadata.type_id == type_id)
        .map(|metadata| metadata.dispose_fn)
}

/// Distributed slice for metadata getter functions (compile-time collection)
/// This allows derive macros to automatically register services at compile time
#[linkme::distributed_slice]
#[doc(hidden)]
pub static SERVICE_METADATA_GETTERS: [fn() -> &'static ServiceMetadata] = [..];

/// Get all registered service metadata
/// This function collects metadata from all services registered via derive macros
#[doc(hidden)]
pub fn get_all_service_metadata() -> Vec<&'static ServiceMetadata> {
    SERVICE_METADATA_GETTERS
        .iter()
        .map(|getter| getter())
        .collect()
}

/// Validate an explicit metadata set.
///
/// This entry point keeps graph validation deterministic and directly
/// testable without mutating the link-time distributed registry.
#[doc(hidden)]
pub fn analyze_service_graph(metadata: &[&ServiceMetadata]) -> Result<Vec<TypeId>, InjectionError> {
    if metadata.is_empty() {
        return Ok(Vec::new());
    }

    // Both concrete service IDs and trait aliases participate in dependency
    // resolution. Each resolvable ID must map to exactly one implementation.
    let mut registrations: HashMap<TypeId, usize> = HashMap::new();
    for (index, service) in metadata.iter().enumerate() {
        register_resolvable_type(&mut registrations, metadata, service.type_id, index)?;
    }
    for (index, service) in metadata.iter().enumerate() {
        if let Some(trait_type_id) = service.trait_type_id {
            register_resolvable_type(&mut registrations, metadata, trait_type_id, index)?;
        }
    }

    // Graph edges are stored as service -> concrete dependency. Duplicate
    // injected fields of the same type count as one graph edge.
    let mut dependencies: Vec<Vec<usize>> = vec![Vec::new(); metadata.len()];
    for (service_index, service) in metadata.iter().enumerate() {
        let mut unique_dependencies = HashSet::new();
        for dependency_type_id in &service.dependencies {
            let Some(&dependency_index) = registrations.get(dependency_type_id) else {
                return Err(InjectionError::MissingDependency {
                    service: service.type_name.to_string(),
                    dependency_type_id: *dependency_type_id,
                });
            };

            let dependency = metadata[dependency_index];
            if lifetime_utils::validate_lifetime_compatibility(
                service.lifetime,
                dependency.lifetime,
            )
            .is_err()
            {
                return Err(InjectionError::LifetimeMismatch {
                    service: service.type_name.to_string(),
                    service_lifetime: format!("{:?}", service.lifetime),
                    dependency: dependency.type_name.to_string(),
                    dependency_lifetime: format!("{:?}", dependency.lifetime),
                });
            }

            if unique_dependencies.insert(dependency_index) {
                dependencies[service_index].push(dependency_index);
            }
        }
    }

    if let Some(cycle) = find_dependency_cycle(&dependencies) {
        let mut names: Vec<String> = cycle
            .into_iter()
            .map(|index| metadata[index].type_name.to_string())
            .collect();
        if let Some(first) = names.first().cloned() {
            names.push(first);
        }
        return Err(InjectionError::CircularDependency { cycle: names });
    }

    // Kahn's algorithm over the complete graph gives dependencies-first
    // ordering. Only singletons are eagerly started by the container.
    let mut reverse_graph: Vec<Vec<usize>> = vec![Vec::new(); metadata.len()];
    let mut in_degree = vec![0usize; metadata.len()];
    for (service_index, service_dependencies) in dependencies.iter().enumerate() {
        in_degree[service_index] = service_dependencies.len();
        for dependency_index in service_dependencies {
            reverse_graph[*dependency_index].push(service_index);
        }
    }

    let mut queue = VecDeque::new();
    for (index, degree) in in_degree.iter().enumerate() {
        if *degree == 0 {
            queue.push_back(index);
        }
    }

    let mut topological_order = Vec::with_capacity(metadata.len());
    while let Some(current) = queue.pop_front() {
        topological_order.push(current);
        for dependent in &reverse_graph[current] {
            in_degree[*dependent] -= 1;
            if in_degree[*dependent] == 0 {
                queue.push_back(*dependent);
            }
        }
    }

    debug_assert_eq!(topological_order.len(), metadata.len());
    Ok(topological_order
        .into_iter()
        .filter_map(|index| {
            let service = metadata[index];
            matches!(service.lifetime, ServiceLifetime::Singleton).then_some(service.type_id)
        })
        .collect())
}

fn register_resolvable_type(
    registrations: &mut HashMap<TypeId, usize>,
    metadata: &[&ServiceMetadata],
    type_id: TypeId,
    service_index: usize,
) -> Result<(), InjectionError> {
    if let Some(existing_index) = registrations.insert(type_id, service_index) {
        let mut services = vec![
            metadata[existing_index].type_name.to_string(),
            metadata[service_index].type_name.to_string(),
        ];
        services.sort();
        services.dedup();
        return Err(InjectionError::AmbiguousRegistration { type_id, services });
    }
    Ok(())
}

fn find_dependency_cycle(dependencies: &[Vec<usize>]) -> Option<Vec<usize>> {
    fn visit(
        current: usize,
        dependencies: &[Vec<usize>],
        states: &mut [u8],
        stack: &mut Vec<usize>,
    ) -> Option<Vec<usize>> {
        states[current] = 1;
        stack.push(current);

        for dependency in &dependencies[current] {
            match states[*dependency] {
                0 => {
                    if let Some(cycle) = visit(*dependency, dependencies, states, stack) {
                        return Some(cycle);
                    }
                }
                1 => {
                    let start = stack
                        .iter()
                        .position(|node| node == dependency)
                        .unwrap_or(0);
                    return Some(stack[start..].to_vec());
                }
                _ => {}
            }
        }

        stack.pop();
        states[current] = 2;
        None
    }

    let mut states = vec![0u8; dependencies.len()];
    let mut stack = Vec::new();
    for index in 0..dependencies.len() {
        if states[index] == 0 {
            if let Some(cycle) = visit(index, dependencies, &mut states, &mut stack) {
                return Some(cycle);
            }
        }
    }
    None
}

#[cfg(test)]
mod graph_validation_tests {
    use super::*;

    struct Database;
    struct Repository;
    struct RequestHandler;
    struct Missing;
    struct CycleA;
    struct CycleB;
    struct SingletonOwner;
    struct ScopedDependency;
    struct FirstImplementation;
    struct SecondImplementation;
    struct InterfaceConsumer;
    trait SharedInterface: Send + Sync {}
    impl SharedInterface for FirstImplementation {}
    impl SharedInterface for SecondImplementation {}

    fn project_first(
        instance: Arc<dyn std::any::Any + Send + Sync>,
    ) -> Result<Box<dyn std::any::Any + Send + Sync>, InjectionError> {
        let concrete = instance.downcast::<FirstImplementation>().map_err(|_| {
            InjectionError::ServiceResolutionFailed("invalid test projection".to_string())
        })?;
        let interface: Arc<dyn SharedInterface> = concrete;
        Ok(Box::new(interface))
    }

    fn project_second(
        instance: Arc<dyn std::any::Any + Send + Sync>,
    ) -> Result<Box<dyn std::any::Any + Send + Sync>, InjectionError> {
        let concrete = instance.downcast::<SecondImplementation>().map_err(|_| {
            InjectionError::ServiceResolutionFailed("invalid test projection".to_string())
        })?;
        let interface: Arc<dyn SharedInterface> = concrete;
        Ok(Box::new(interface))
    }

    fn factory(
        _provider: Arc<dyn std::any::Any + Send + Sync>,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Box<dyn std::any::Any + Send + Sync>, InjectionError>>
                + Send
                + 'static,
        >,
    > {
        Box::pin(async { Ok(Box::new(()) as Box<dyn std::any::Any + Send + Sync>) })
    }

    fn metadata<T: 'static>(
        name: &'static str,
        lifetime: ServiceLifetime,
        dependencies: Vec<TypeId>,
    ) -> ServiceMetadata {
        ServiceMetadata {
            type_id: TypeId::of::<T>(),
            type_name: name,
            trait_type_id: None,
            trait_name: None,
            lifetime,
            factory_fn: factory,
            dependencies,
        }
    }

    #[test]
    fn validates_complete_graph_and_orders_singletons_dependencies_first() {
        let database = metadata::<Database>("Database", ServiceLifetime::Singleton, vec![]);
        let repository = metadata::<Repository>(
            "Repository",
            ServiceLifetime::Singleton,
            vec![TypeId::of::<Database>()],
        );
        let request_handler = metadata::<RequestHandler>(
            "RequestHandler",
            ServiceLifetime::Scoped,
            vec![TypeId::of::<Repository>()],
        );

        let order = analyze_service_graph(&[&request_handler, &repository, &database]).unwrap();
        assert_eq!(
            order,
            vec![TypeId::of::<Database>(), TypeId::of::<Repository>()]
        );
    }

    #[test]
    fn rejects_missing_dependency_before_startup() {
        let service = metadata::<Repository>(
            "Repository",
            ServiceLifetime::Singleton,
            vec![TypeId::of::<Missing>()],
        );

        assert!(matches!(
            analyze_service_graph(&[&service]),
            Err(InjectionError::MissingDependency { service, dependency_type_id })
                if service == "Repository" && dependency_type_id == TypeId::of::<Missing>()
        ));
    }

    #[test]
    fn rejects_cycles_for_non_singleton_services() {
        let first = metadata::<CycleA>(
            "CycleA",
            ServiceLifetime::Scoped,
            vec![TypeId::of::<CycleB>()],
        );
        let second = metadata::<CycleB>(
            "CycleB",
            ServiceLifetime::Scoped,
            vec![TypeId::of::<CycleA>()],
        );

        assert!(matches!(
            analyze_service_graph(&[&first, &second]),
            Err(InjectionError::CircularDependency { cycle })
                if cycle == vec!["CycleA", "CycleB", "CycleA"]
        ));
    }

    #[test]
    fn rejects_captive_lifetime_dependencies() {
        let owner = metadata::<SingletonOwner>(
            "SingletonOwner",
            ServiceLifetime::Singleton,
            vec![TypeId::of::<ScopedDependency>()],
        );
        let dependency =
            metadata::<ScopedDependency>("ScopedDependency", ServiceLifetime::Scoped, vec![]);

        assert!(matches!(
            analyze_service_graph(&[&owner, &dependency]),
            Err(InjectionError::LifetimeMismatch { service, dependency, .. })
                if service == "SingletonOwner" && dependency == "ScopedDependency"
        ));
    }

    #[test]
    fn rejects_ambiguous_trait_aliases() {
        let mut first = metadata::<FirstImplementation>(
            "FirstImplementation",
            ServiceLifetime::Singleton,
            vec![],
        );
        first.trait_type_id = Some(TypeId::of::<dyn SharedInterface>());
        first.trait_name = Some("SharedInterface");

        let mut second = metadata::<SecondImplementation>(
            "SecondImplementation",
            ServiceLifetime::Singleton,
            vec![],
        );
        second.trait_type_id = Some(TypeId::of::<dyn SharedInterface>());
        second.trait_name = Some("SharedInterface");

        assert!(matches!(
            analyze_service_graph(&[&first, &second]),
            Err(InjectionError::AmbiguousRegistration { services, .. })
                if services == vec!["FirstImplementation", "SecondImplementation"]
        ));
    }

    #[test]
    fn resolves_trait_aliases_during_graph_analysis() {
        let mut implementation = metadata::<FirstImplementation>(
            "FirstImplementation",
            ServiceLifetime::Singleton,
            vec![],
        );
        implementation.trait_type_id = Some(TypeId::of::<dyn SharedInterface>());
        implementation.trait_name = Some("SharedInterface");
        let consumer = metadata::<InterfaceConsumer>(
            "InterfaceConsumer",
            ServiceLifetime::Singleton,
            vec![TypeId::of::<dyn SharedInterface>()],
        );

        let order = analyze_service_graph(&[&consumer, &implementation]).unwrap();
        assert_eq!(
            order,
            vec![
                TypeId::of::<FirstImplementation>(),
                TypeId::of::<InterfaceConsumer>()
            ]
        );
    }

    #[test]
    fn trait_alias_uses_implementation_lifetime_for_captive_dependency_validation() {
        let mut implementation =
            metadata::<FirstImplementation>("FirstImplementation", ServiceLifetime::Scoped, vec![]);
        implementation.trait_type_id = Some(TypeId::of::<dyn SharedInterface>());
        implementation.trait_name = Some("SharedInterface");
        let consumer = metadata::<InterfaceConsumer>(
            "InterfaceConsumer",
            ServiceLifetime::Singleton,
            vec![TypeId::of::<dyn SharedInterface>()],
        );

        assert!(matches!(
            analyze_service_graph(&[&consumer, &implementation]),
            Err(InjectionError::LifetimeMismatch { service, dependency, .. })
                if service == "InterfaceConsumer" && dependency == "FirstImplementation"
        ));
    }

    #[test]
    fn trait_alias_edges_participate_in_cycle_detection() {
        let mut implementation = metadata::<FirstImplementation>(
            "FirstImplementation",
            ServiceLifetime::Scoped,
            vec![TypeId::of::<InterfaceConsumer>()],
        );
        implementation.trait_type_id = Some(TypeId::of::<dyn SharedInterface>());
        implementation.trait_name = Some("SharedInterface");
        let consumer = metadata::<InterfaceConsumer>(
            "InterfaceConsumer",
            ServiceLifetime::Scoped,
            vec![TypeId::of::<dyn SharedInterface>()],
        );

        assert!(matches!(
            analyze_service_graph(&[&consumer, &implementation]),
            Err(InjectionError::CircularDependency { cycle })
                if cycle == vec!["InterfaceConsumer", "FirstImplementation", "InterfaceConsumer"]
        ));
    }

    #[test]
    fn rejects_duplicate_active_interface_routes_with_typed_error() {
        let first = ServiceRouteMetadata {
            requested_type_id: TypeId::of::<dyn SharedInterface>(),
            requested_type_name: "dyn SharedInterface",
            implementation_type_id: TypeId::of::<FirstImplementation>(),
            implementation_type_name: "FirstImplementation",
            kind: ServiceRouteKind::Interface,
            project_fn: project_first,
        };
        let second = ServiceRouteMetadata {
            requested_type_id: TypeId::of::<dyn SharedInterface>(),
            requested_type_name: "dyn SharedInterface",
            implementation_type_id: TypeId::of::<SecondImplementation>(),
            implementation_type_name: "SecondImplementation",
            kind: ServiceRouteKind::Interface,
            project_fn: project_second,
        };

        assert!(matches!(
            validate_service_route_metadata(&[&second, &first]),
            Err(InjectionError::DuplicateInterfaceBinding {
                interface,
                implementations,
                ..
            }) if interface == "dyn SharedInterface"
                && implementations == vec!["FirstImplementation", "SecondImplementation"]
        ));
    }

    #[test]
    fn interface_projector_preserves_the_concrete_arc_allocation() {
        let concrete = Arc::new(FirstImplementation);
        let erased: Arc<dyn std::any::Any + Send + Sync> = concrete.clone();
        let projected = project_first(erased).unwrap();
        let interface = *projected
            .downcast::<Arc<dyn SharedInterface>>()
            .expect("projector must box Arc<dyn SharedInterface>");

        assert_eq!(
            Arc::as_ptr(&concrete) as *const (),
            Arc::as_ptr(&interface) as *const ()
        );
    }

    proptest::proptest! {
        #[test]
        fn forward_only_graphs_are_always_acyclic(
            node_count in 1usize..32,
            raw_edges in proptest::collection::vec((0u8..64, 0u8..64), 0..256),
        ) {
            let mut graph = vec![Vec::new(); node_count];
            for (left, right) in raw_edges {
                let left = usize::from(left) % node_count;
                let right = usize::from(right) % node_count;
                let high = left.max(right);
                let low = left.min(right);
                if high != low && !graph[high].contains(&low) {
                    graph[high].push(low);
                }
            }
            proptest::prop_assert!(find_dependency_cycle(&graph).is_none());
        }

        #[test]
        fn ring_graphs_are_always_rejected(node_count in 2usize..32) {
            let mut graph = vec![Vec::new(); node_count];
            for (index, dependencies) in graph.iter_mut().enumerate() {
                dependencies.push((index + 1) % node_count);
            }
            proptest::prop_assert!(find_dependency_cycle(&graph).is_some());
        }
    }
}

// Tests are now in the tests/ directory
