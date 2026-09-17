// =============================================================================
// 2. ASYNC REGISTRY IMPLEMENTATION
// =============================================================================

use linkme::distributed_slice;
use std::collections::{HashMap, HashSet};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

use lily_injection::Extensions;

use crate::controller::ControllerMaterializationError;
use crate::handler::Handler;
use crate::private::{
    ControllerActionRegistration, ControllerRegistration, CorsRoutePolicyRegistration,
    ErasedController, HttpMiddlewareRegistration, OpenApiOperationFactory,
};

use super::openapi::{
    ControllerRouteMaterialization, MaterializedControllerRoute, OpenApiRouteMetadataStatus,
    OpenApiRouteRegistration,
};

/// App-owned route information produced by struct-controller materialization.
#[derive(Clone)]
pub struct RouteInfo {
    /// Normalized HTTP method token.
    pub method: String,
    /// Canonical route template.
    pub path: String,
    pub(crate) handler: Handler,
    /// Fully qualified controller action name used in diagnostics.
    pub handler_name: String,
    /// Ordered guard type identifiers attached to this action.
    pub guard_type_ids: Vec<std::any::TypeId>, // Guards associated with this route
    pub(crate) middleware_registrations: Vec<HttpMiddlewareRegistration>,
    pub(crate) cors_policy_registration: CorsRoutePolicyRegistration,
    pub(crate) route_plan_id: usize,
}

impl std::fmt::Debug for RouteInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncRouteInfo")
            .field("method", &self.method)
            .field("path", &self.path)
            .field("handler", &"<async function>")
            .field("handler_name", &self.handler_name)
            .finish()
    }
}

impl RouteInfo {
    /// Macro-contract inspection seam used by compile and inventory tests.
    #[doc(hidden)]
    pub fn middleware_type_ids(&self) -> impl Iterator<Item = std::any::TypeId> + '_ {
        self.middleware_registrations
            .iter()
            .map(|registration| registration.type_id())
    }
}

/// Static route definition produced for one struct-controller action.
///
/// Unlike [`RouteInfo`], this value has no ready handler and therefore cannot
/// own an App-specific controller instance. It is converted into `RouteInfo`
/// only after the application's Extensions are available.
#[doc(hidden)]
#[derive(Clone)]
pub struct PendingControllerRoute {
    method: String,
    path: String,
    handler_name: &'static str,
    guard_type_ids: Vec<std::any::TypeId>,
    middleware_registrations: Vec<HttpMiddlewareRegistration>,
    cors_policy_registration: CorsRoutePolicyRegistration,
    action_registration: ControllerActionRegistration,
    openapi_registration: OpenApiRouteRegistration,
}

impl PendingControllerRoute {
    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        method: &str,
        path: impl Into<String>,
        handler_name: &'static str,
        guard_type_ids: Vec<std::any::TypeId>,
        middleware_registrations: Vec<HttpMiddlewareRegistration>,
        cors_policy_registration: CorsRoutePolicyRegistration,
        action_registration: ControllerActionRegistration,
    ) -> Self {
        Self {
            method: method.to_uppercase(),
            path: path.into(),
            handler_name,
            guard_type_ids,
            middleware_registrations,
            cors_policy_registration,
            action_registration,
            openapi_registration: OpenApiRouteRegistration::Unspecified,
        }
    }

    /// Attach the documented action's monomorphized OpenAPI factory to this
    /// exact route registration.
    #[doc(hidden)]
    pub fn with_openapi_operation_factory(mut self, factory: OpenApiOperationFactory) -> Self {
        self.openapi_registration = OpenApiRouteRegistration::Documented(factory);
        self
    }

    /// Mark this route as intentionally excluded from the generated OpenAPI
    /// document. This remains distinguishable from a route that has not made
    /// a documentation decision.
    #[doc(hidden)]
    pub fn with_openapi_skipped(mut self) -> Self {
        self.openapi_registration = OpenApiRouteRegistration::Skipped;
        self
    }

    #[doc(hidden)]
    pub fn method(&self) -> &str {
        &self.method
    }

    #[doc(hidden)]
    pub fn path(&self) -> &str {
        &self.path
    }

    #[doc(hidden)]
    pub const fn handler_name(&self) -> &'static str {
        self.handler_name
    }

    #[doc(hidden)]
    pub fn guard_type_ids(&self) -> impl Iterator<Item = std::any::TypeId> + '_ {
        self.guard_type_ids.iter().copied()
    }

    #[doc(hidden)]
    pub fn middleware_type_ids(&self) -> impl Iterator<Item = std::any::TypeId> + '_ {
        self.middleware_registrations
            .iter()
            .map(|registration| registration.type_id())
    }

    #[doc(hidden)]
    pub fn cors_policy_type_id(&self) -> Option<std::any::TypeId> {
        self.cors_policy_registration
            .provider_registration()
            .map(|registration| registration.type_id())
    }

    #[doc(hidden)]
    pub const fn controller_type_id(&self) -> std::any::TypeId {
        self.action_registration.controller_type_id()
    }

    #[doc(hidden)]
    pub const fn controller_type_name(&self) -> &'static str {
        self.action_registration.controller_type_name()
    }

    #[doc(hidden)]
    pub const fn openapi_operation_factory(&self) -> Option<OpenApiOperationFactory> {
        self.openapi_registration.factory()
    }

    #[doc(hidden)]
    pub const fn openapi_metadata_status(&self) -> OpenApiRouteMetadataStatus {
        self.openapi_registration.status()
    }
}

impl std::fmt::Debug for PendingControllerRoute {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PendingControllerRoute")
            .field("method", &self.method)
            .field("path", &self.path)
            .field("handler_name", &self.handler_name)
            .field(
                "controller",
                &self.action_registration.controller_type_name(),
            )
            .field("openapi", &self.openapi_registration.status())
            .finish()
    }
}

/// Function pointer emitted once for each struct controller.
#[doc(hidden)]
pub type StructControllerRegistrationFn = fn() -> ControllerRegistration;

/// Link-time controller initializer metadata. No live instance can be stored
/// in this slice.
#[doc(hidden)]
#[distributed_slice]
pub static STRUCT_CONTROLLER_REGISTRATIONS: [StructControllerRegistrationFn] = [..];

/// Function pointer emitted once for each struct-controller action.
#[doc(hidden)]
pub type PendingControllerRouteRegistrationFn = fn() -> PendingControllerRoute;

/// Link-time pending action metadata. Each function creates an owned static
/// definition; no App-specific instance is retained globally.
#[doc(hidden)]
#[distributed_slice]
pub static PENDING_CONTROLLER_ROUTE_REGISTRATIONS: [PendingControllerRouteRegistrationFn] = [..];

#[doc(hidden)]
pub fn get_struct_controller_registrations() -> Vec<ControllerRegistration> {
    STRUCT_CONTROLLER_REGISTRATIONS
        .iter()
        .map(|registration| registration())
        .collect()
}

#[doc(hidden)]
pub fn get_pending_controller_routes() -> Vec<PendingControllerRoute> {
    PENDING_CONTROLLER_ROUTE_REGISTRATIONS
        .iter()
        .map(|registration| registration())
        .collect()
}

/// Materialize static struct-controller metadata for exactly one App build.
///
/// The instance map is build-local. Once each binder has produced its handler,
/// the handlers are the only owners of their concrete controller references.
#[doc(hidden)]
pub async fn materialize_controller_routes(
    mut registrations: Vec<ControllerRegistration>,
    pending_routes: Vec<PendingControllerRoute>,
    extensions: Arc<Extensions>,
) -> Result<ControllerRouteMaterialization, ControllerMaterializationError> {
    registrations.sort_by(|left, right| left.type_name().cmp(right.type_name()));

    let mut registrations_by_type = HashMap::with_capacity(registrations.len());
    for registration in &registrations {
        if registrations_by_type
            .insert(registration.type_id(), *registration)
            .is_some()
        {
            return Err(ControllerMaterializationError::DuplicateRegistration {
                controller: registration.type_name(),
            });
        }
    }

    let mut required_controller_types = HashSet::with_capacity(pending_routes.len());
    for route in &pending_routes {
        let action = route.action_registration;
        let Some(registration) = registrations_by_type.get(&action.controller_type_id()) else {
            return Err(ControllerMaterializationError::MissingRegistration {
                controller: action.controller_type_name(),
                action: route.handler_name,
            });
        };
        if registration.type_name() != action.controller_type_name() {
            return Err(ControllerMaterializationError::MetadataMismatch {
                registered_controller: registration.type_name(),
                action_controller: action.controller_type_name(),
                action: route.handler_name,
            });
        }
        required_controller_types.insert(action.controller_type_id());
    }

    let mut instances: HashMap<std::any::TypeId, ErasedController> =
        HashMap::with_capacity(required_controller_types.len());
    for registration in registrations
        .into_iter()
        .filter(|registration| required_controller_types.contains(&registration.type_id()))
    {
        let controller = registration
            .instantiate(Arc::clone(&extensions))
            .await
            .map_err(|source| ControllerMaterializationError::Initialization {
                controller: registration.type_name(),
                source,
            })?;
        instances.insert(registration.type_id(), controller);
    }

    let mut routes = Vec::with_capacity(pending_routes.len());
    for pending in pending_routes {
        let controller = Arc::clone(
            instances
                .get(&pending.action_registration.controller_type_id())
                .expect("controller metadata was validated before initialization"),
        );
        let handler = match catch_unwind(AssertUnwindSafe(|| {
            pending.action_registration.bind(controller)
        })) {
            Ok(Ok(handler)) => handler,
            Ok(Err(source)) => {
                return Err(ControllerMaterializationError::Binding {
                    controller: pending.action_registration.controller_type_name(),
                    action: pending.handler_name,
                    source,
                });
            }
            Err(_) => {
                return Err(ControllerMaterializationError::BindingPanicked {
                    controller: pending.action_registration.controller_type_name(),
                    action: pending.handler_name,
                });
            }
        };

        let route = RouteInfo {
            method: pending.method,
            path: pending.path,
            handler,
            handler_name: pending.handler_name.to_string(),
            guard_type_ids: pending.guard_type_ids,
            middleware_registrations: pending.middleware_registrations,
            cors_policy_registration: pending.cors_policy_registration,
            route_plan_id: 0,
        };
        routes.push(MaterializedControllerRoute::new(
            route,
            pending.openapi_registration,
        ));
    }

    Ok(ControllerRouteMaterialization::new(routes))
}
