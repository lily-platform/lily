use lily_error::injection::InjectionError;
use lily_injection_registry::{
    ServiceDisposeFn, ServiceMetadata, ServiceRouteKind, ServiceRouteMetadata,
    analyze_service_graph, get_all_service_metadata, get_all_service_route_metadata,
    get_service_disposer, validate_service_route_metadata,
};
use std::any::TypeId;
use std::collections::HashMap;

/// One immutable, application-owned view of the link-time DI declarations.
///
/// Link-time slices are discovery inputs only. Every container snapshots and
/// validates them before it creates a descriptor or starts a service. Runtime
/// resolution therefore cannot observe a graph different from the one that
/// passed startup validation.
pub(crate) struct RegistrationPlan {
    registrations: HashMap<TypeId, PlannedService>,
    routes: HashMap<TypeId, ServiceRouteMetadata>,
    singleton_initialization_order: Vec<TypeId>,
}

#[derive(Clone, Copy)]
pub(crate) struct PlannedService {
    pub metadata: &'static ServiceMetadata,
    pub dispose_fn: Option<ServiceDisposeFn>,
}

impl RegistrationPlan {
    pub(crate) fn discover() -> Result<Self, InjectionError> {
        let metadata = get_all_service_metadata();
        let route_metadata = get_all_service_route_metadata();

        // Validate the exact snapshots that this application will use.
        validate_service_route_metadata(&route_metadata)?;
        let singleton_initialization_order = analyze_service_graph(&metadata)?;

        let mut registrations = HashMap::with_capacity(metadata.len());
        for service in metadata {
            if let Some(existing) = registrations.insert(
                service.type_id,
                PlannedService {
                    metadata: service,
                    dispose_fn: get_service_disposer(service.type_id),
                },
            ) {
                let mut services = vec![
                    existing.metadata.type_name.to_string(),
                    service.type_name.to_string(),
                ];
                services.sort();
                services.dedup();
                return Err(InjectionError::AmbiguousRegistration {
                    type_id: service.type_id,
                    services,
                });
            }
        }

        let mut routes = HashMap::with_capacity(route_metadata.len());
        for route in route_metadata {
            let implementation = registrations
                .get(&route.implementation_type_id)
                .ok_or_else(|| {
                    InjectionError::InvalidRegistrationPlan(format!(
                        "route '{}' points to unregistered implementation '{}'",
                        route.requested_type_name, route.implementation_type_name
                    ))
                })?;

            validate_route_contract(route, implementation)?;

            // Duplicate requested IDs were already rejected by the registry
            // validator; inserting here only materializes the immutable index.
            routes.insert(route.requested_type_id, *route);
        }

        // Every active derive must publish a concrete route. This is what lets
        // the same public get_service method support both sized concrete types
        // and unsized trait objects without unsafe casts.
        for service in registrations.values() {
            let route = routes.get(&service.metadata.type_id).ok_or_else(|| {
                InjectionError::InvalidRegistrationPlan(format!(
                    "service '{}' did not publish its concrete resolution route",
                    service.metadata.type_name
                ))
            })?;
            if route.kind != ServiceRouteKind::Concrete
                || route.implementation_type_id != service.metadata.type_id
            {
                return Err(InjectionError::InvalidRegistrationPlan(format!(
                    "service '{}' has an invalid concrete resolution route",
                    service.metadata.type_name
                )));
            }

            if let Some(interface_type_id) = service.metadata.trait_type_id {
                let route = routes.get(&interface_type_id).ok_or_else(|| {
                    InjectionError::InvalidRegistrationPlan(format!(
                        "service '{}' declares an interface without a projection route",
                        service.metadata.type_name
                    ))
                })?;
                if route.kind != ServiceRouteKind::Interface
                    || route.implementation_type_id != service.metadata.type_id
                {
                    return Err(InjectionError::InvalidRegistrationPlan(format!(
                        "service '{}' has an inconsistent interface route",
                        service.metadata.type_name
                    )));
                }
            }
        }

        Ok(Self {
            registrations,
            routes,
            singleton_initialization_order,
        })
    }

    pub(crate) fn registrations(&self) -> &HashMap<TypeId, PlannedService> {
        &self.registrations
    }

    pub(crate) fn routes(&self) -> &HashMap<TypeId, ServiceRouteMetadata> {
        &self.routes
    }

    pub(crate) fn singleton_initialization_order(&self) -> &[TypeId] {
        &self.singleton_initialization_order
    }
}

fn validate_route_contract(
    route: &ServiceRouteMetadata,
    implementation: &PlannedService,
) -> Result<(), InjectionError> {
    if route.kind == ServiceRouteKind::Concrete
        && route.requested_type_id != route.implementation_type_id
    {
        return Err(InjectionError::InvalidRegistrationPlan(format!(
            "concrete route '{}' does not point to itself",
            route.requested_type_name
        )));
    }

    if route.kind == ServiceRouteKind::Interface
        && implementation.metadata.trait_type_id != Some(route.requested_type_id)
    {
        return Err(InjectionError::InvalidRegistrationPlan(format!(
            "interface route '{}' is not declared by implementation '{}'",
            route.requested_type_name, implementation.metadata.type_name
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use lily_injection_registry::ServiceLifetime;
    use std::{any::Any, future::Future, pin::Pin, sync::Arc};

    struct Implementation;
    trait UndeclaredInterface: Send + Sync {}

    type TestFactoryFuture = Pin<
        Box<
            dyn Future<Output = Result<Box<dyn Any + Send + Sync>, InjectionError>>
                + Send
                + 'static,
        >,
    >;

    fn factory(_provider: Arc<dyn Any + Send + Sync>) -> TestFactoryFuture {
        Box::pin(async { Ok(Box::new(Implementation) as Box<dyn Any + Send + Sync>) })
    }

    fn projection(
        instance: Arc<dyn Any + Send + Sync>,
    ) -> Result<Box<dyn Any + Send + Sync>, InjectionError> {
        Ok(Box::new(instance))
    }

    #[test]
    fn undeclared_interface_route_cannot_enter_the_application_plan() {
        let metadata = Box::leak(Box::new(ServiceMetadata {
            type_id: TypeId::of::<Implementation>(),
            type_name: std::any::type_name::<Implementation>(),
            trait_type_id: None,
            trait_name: None,
            lifetime: ServiceLifetime::Singleton,
            factory_fn: factory,
            dependencies: Vec::new(),
        }));
        let implementation = PlannedService {
            metadata,
            dispose_fn: None,
        };
        let route = ServiceRouteMetadata {
            requested_type_id: TypeId::of::<dyn UndeclaredInterface>(),
            requested_type_name: std::any::type_name::<dyn UndeclaredInterface>(),
            implementation_type_id: TypeId::of::<Implementation>(),
            implementation_type_name: std::any::type_name::<Implementation>(),
            kind: ServiceRouteKind::Interface,
            project_fn: projection,
        };

        assert!(matches!(
            validate_route_contract(&route, &implementation),
            Err(InjectionError::InvalidRegistrationPlan(message))
                if message.contains("not declared")
        ));
    }
}
