//! Private module for internal framework utilities
//!
//! This module contains utilities that are used internally by the framework
//! and are not intended to be used directly by users.

use std::future::Future;
use std::marker::PhantomData;
use std::{
    any::{Any, TypeId},
    collections::BTreeMap,
    panic::{catch_unwind, AssertUnwindSafe},
    pin::Pin,
    sync::Arc,
};

use futures::FutureExt;
use lily_injection::Extensions;
use lily_middleware::{
    CorsPolicy, CorsPolicyProvider, HttpMiddleware, HttpMiddlewareInitError, MiddlewareConfigError,
    MiddlewareErrorCode,
};

#[doc(hidden)]
pub use crate::controller::ControllerBindingError;
use crate::controller::{ControllerInitError, ControllerTrait};
#[doc(hidden)]
pub use crate::extractor::{
    extract_request, extract_request_parts, extract_service, extract_terminal_request,
    multipart_file_field, multipart_text_field, reserve_multipart_slot, FromMultipartForm,
    MultipartFormOpenApi, ServiceRequestExtractor,
};
use crate::handler::Handler;
#[doc(hidden)]
pub use lily_web_core::MultipartField;
#[doc(hidden)]
pub use lily_web_core::{
    write_error_response, write_passthrough_response, PassthroughResponseState,
};

/// Build-time OpenAPI operation and component metadata emitted for one action.
///
/// This narrow wrapper intentionally carries `utoipa`'s framework-agnostic
/// model instead of introducing a Lily-owned specification model. It is a
/// macro integration contract and is not part of the user-facing API.
#[doc(hidden)]
#[derive(Clone)]
pub struct OpenApiOperationMetadata {
    operation: utoipa::openapi::path::Operation,
    components: utoipa::openapi::schema::Components,
    schema_owners: BTreeMap<String, &'static str>,
    response_owners: BTreeMap<String, &'static str>,
    response_status_owners: BTreeMap<String, &'static str>,
    issues: Vec<OpenApiOperationMetadataIssue>,
}

#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenApiMetadataComponentKind {
    Schema,
    Response,
}

#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpenApiOperationMetadataIssue {
    ComponentCollision {
        kind: OpenApiMetadataComponentKind,
        name: String,
        first_type: &'static str,
        duplicate_type: &'static str,
    },
    ResponseStatusCollision {
        status: String,
        first_source: &'static str,
        duplicate_source: &'static str,
    },
}

impl OpenApiOperationMetadata {
    #[doc(hidden)]
    pub fn new(
        operation: utoipa::openapi::path::Operation,
        components: utoipa::openapi::schema::Components,
    ) -> Self {
        let response_status_owners = operation
            .responses
            .responses
            .keys()
            .cloned()
            .map(|status| (status, "inferred or inline response"))
            .collect();
        Self {
            operation,
            components,
            schema_owners: BTreeMap::new(),
            response_owners: BTreeMap::new(),
            response_status_owners,
            issues: Vec::new(),
        }
    }

    #[doc(hidden)]
    pub const fn operation(&self) -> &utoipa::openapi::path::Operation {
        &self.operation
    }

    #[doc(hidden)]
    pub const fn components(&self) -> &utoipa::openapi::schema::Components {
        &self.components
    }

    #[doc(hidden)]
    pub fn operation_mut(&mut self) -> &mut utoipa::openapi::path::Operation {
        &mut self.operation
    }

    #[doc(hidden)]
    pub fn issues(&self) -> &[OpenApiOperationMetadataIssue] {
        &self.issues
    }

    #[doc(hidden)]
    pub fn collect_schema<T>(&mut self)
    where
        T: utoipa::ToSchema,
    {
        let owner = std::any::type_name::<T>();
        let mut schemas = Vec::new();
        collect_openapi_schema::<T>(&mut schemas);
        for (name, schema) in schemas {
            self.insert_schema(name, schema, owner);
        }
    }

    #[doc(hidden)]
    pub fn collect_multipart_schema<T>(&mut self)
    where
        T: MultipartFormOpenApi,
    {
        self.insert_schema(
            T::openapi_schema_name().into_owned(),
            T::openapi_schema(),
            std::any::type_name::<T>(),
        );
    }

    #[doc(hidden)]
    pub fn register_reusable_response<T>(&mut self, status: &str)
    where
        T: utoipa::ToSchema,
        for<'response> T: utoipa::ToResponse<'response>,
    {
        self.collect_schema::<T>();
        let owner = std::any::type_name::<T>();
        let (name, response) = <T as utoipa::ToResponse<'static>>::response();
        self.insert_response_component(name.to_owned(), response, owner);
        self.insert_response_status(
            status.to_owned(),
            utoipa::openapi::Ref::from_response_name(name).into(),
            owner,
            false,
        );
    }

    #[doc(hidden)]
    pub fn register_default_reusable_response<T>(&mut self, status: &str)
    where
        T: utoipa::ToSchema,
        for<'response> T: utoipa::ToResponse<'response>,
    {
        if self.operation.responses.responses.contains_key(status) {
            return;
        }
        self.collect_schema::<T>();
        let owner = std::any::type_name::<T>();
        let (name, response) = <T as utoipa::ToResponse<'static>>::response();
        self.insert_response_component(name.to_owned(), response, owner);
        self.insert_response_status(
            status.to_owned(),
            utoipa::openapi::Ref::from_response_name(name).into(),
            owner,
            true,
        );
    }

    #[doc(hidden)]
    pub fn extend_responses<T>(&mut self)
    where
        T: utoipa::IntoResponses + utoipa::ToSchema,
    {
        self.collect_schema::<T>();
        let owner = std::any::type_name::<T>();
        for (status, response) in T::responses() {
            self.insert_response_status(status, response, owner, false);
        }
    }

    #[doc(hidden)]
    pub fn extend_default_responses<T>(&mut self)
    where
        T: utoipa::IntoResponses + utoipa::ToSchema,
    {
        self.collect_schema::<T>();
        let owner = std::any::type_name::<T>();
        for (status, response) in T::responses() {
            if !self.operation.responses.responses.contains_key(&status) {
                self.insert_response_status(status, response, owner, true);
            }
        }
    }

    fn insert_schema(
        &mut self,
        name: String,
        schema: utoipa::openapi::RefOr<utoipa::openapi::schema::Schema>,
        owner: &'static str,
    ) {
        if let Some(existing) = self.components.schemas.get(&name) {
            if existing != &schema {
                let first_type = self
                    .schema_owners
                    .get(&name)
                    .copied()
                    .unwrap_or("<unknown>");
                self.issues
                    .push(OpenApiOperationMetadataIssue::ComponentCollision {
                        kind: OpenApiMetadataComponentKind::Schema,
                        name,
                        first_type,
                        duplicate_type: owner,
                    });
            }
            return;
        }
        self.schema_owners.insert(name.clone(), owner);
        self.components.schemas.insert(name, schema);
    }

    fn insert_response_component(
        &mut self,
        name: String,
        response: utoipa::openapi::RefOr<utoipa::openapi::response::Response>,
        owner: &'static str,
    ) {
        if let Some(existing) = self.components.responses.get(&name) {
            if existing != &response {
                let first_type = self
                    .response_owners
                    .get(&name)
                    .copied()
                    .unwrap_or("<unknown>");
                self.issues
                    .push(OpenApiOperationMetadataIssue::ComponentCollision {
                        kind: OpenApiMetadataComponentKind::Response,
                        name,
                        first_type,
                        duplicate_type: owner,
                    });
            }
            return;
        }
        self.response_owners.insert(name.clone(), owner);
        self.components.responses.insert(name, response);
    }

    fn insert_response_status(
        &mut self,
        status: String,
        response: utoipa::openapi::RefOr<utoipa::openapi::response::Response>,
        source: &'static str,
        only_if_missing: bool,
    ) {
        if self.operation.responses.responses.contains_key(&status) {
            if !only_if_missing {
                self.issues
                    .push(OpenApiOperationMetadataIssue::ResponseStatusCollision {
                        status: status.clone(),
                        first_source: self
                            .response_status_owners
                            .get(&status)
                            .copied()
                            .unwrap_or("<unknown>"),
                        duplicate_source: source,
                    });
            }
            return;
        }
        self.response_status_owners.insert(status.clone(), source);
        self.operation.responses.responses.insert(status, response);
    }

    #[doc(hidden)]
    pub fn into_parts(
        self,
    ) -> (
        utoipa::openapi::path::Operation,
        utoipa::openapi::schema::Components,
    ) {
        (self.operation, self.components)
    }
}

/// Monomorphized operation factory generated for one documented action.
///
/// The pointer is stored with the action's pending route metadata and is only
/// intended to run during an OpenAPI-enabled App build. It is never invoked on
/// the request path.
#[doc(hidden)]
pub type OpenApiOperationFactory = fn() -> OpenApiOperationMetadata;

/// Collect one root schema and every recursively referenced schema emitted by
/// `utoipa`. The generated call exists only for an OpenAPI-documented action,
/// so undocumented actions acquire no `ToSchema` bound.
#[doc(hidden)]
pub fn collect_openapi_schema<T>(
    schemas: &mut Vec<(
        String,
        utoipa::openapi::RefOr<utoipa::openapi::schema::Schema>,
    )>,
) where
    T: utoipa::ToSchema,
{
    schemas.push((T::name().into_owned(), T::schema()));
    T::schemas(schemas);
}

/// Build a reusable component reference for a documented schema type.
#[doc(hidden)]
pub fn openapi_schema_ref<T>() -> utoipa::openapi::Ref
where
    T: utoipa::ToSchema,
{
    utoipa::openapi::Ref::from_schema_name(T::name().into_owned())
}

/// Build a component reference for a Lily multipart DTO.
#[doc(hidden)]
pub fn openapi_multipart_schema_ref<T>() -> utoipa::openapi::Ref
where
    T: MultipartFormOpenApi,
{
    utoipa::openapi::Ref::from_schema_name(T::openapi_schema_name().into_owned())
}

/// Build an inline schema with an explicit OpenAPI format override.
///
/// Format is meaningful for scalar/object schemas. Composite schemas retain
/// their upstream representation because applying a scalar format to a
/// oneOf/allOf/array contract would be invalid.
#[doc(hidden)]
pub fn openapi_schema_with_format<T>(
    format: &str,
) -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema>
where
    T: utoipa::ToSchema,
{
    let mut schema = T::schema();
    if let utoipa::openapi::RefOr::T(utoipa::openapi::schema::Schema::Object(object)) = &mut schema
    {
        object.format = Some(utoipa::openapi::schema::SchemaFormat::Custom(
            format.to_owned(),
        ));
    }
    schema
}

/// App-local, type-erased controller instance used only during route
/// materialization.
#[doc(hidden)]
pub type ErasedController = Arc<dyn Any + Send + Sync>;

type ControllerInitializationFuture =
    Pin<Box<dyn Future<Output = Result<ErasedController, ControllerInitError>> + Send + 'static>>;

/// Type-erased controller initializer stored in link-time metadata.
///
/// This value contains only a `TypeId`, a static type name and a function
/// pointer. It never owns a live controller or an application's Extensions.
#[doc(hidden)]
#[derive(Clone, Copy)]
pub struct ControllerRegistration {
    type_id: TypeId,
    type_name: &'static str,
    initialize: fn(Arc<Extensions>) -> ControllerInitializationFuture,
}

impl ControllerRegistration {
    #[doc(hidden)]
    pub fn of<C>() -> Self
    where
        C: ControllerTrait,
    {
        Self {
            type_id: TypeId::of::<C>(),
            type_name: std::any::type_name::<C>(),
            initialize: initialize_controller::<C>,
        }
    }

    #[doc(hidden)]
    pub const fn type_id(self) -> TypeId {
        self.type_id
    }

    #[doc(hidden)]
    pub const fn type_name(self) -> &'static str {
        self.type_name
    }

    #[doc(hidden)]
    pub async fn instantiate(
        self,
        extensions: Arc<Extensions>,
    ) -> Result<ErasedController, ControllerInitError> {
        (self.initialize)(extensions).await
    }
}

fn initialize_controller<C>(extensions: Arc<Extensions>) -> ControllerInitializationFuture
where
    C: ControllerTrait,
{
    Box::pin(async move {
        match AssertUnwindSafe(C::new(extensions)).catch_unwind().await {
            Ok(Ok(controller)) => Ok(Arc::new(controller) as ErasedController),
            Ok(Err(error)) => Err(error),
            Err(_) => Err(ControllerInitError::Internal),
        }
    })
}

/// Generated action-specific function that converts one erased controller
/// instance into a ready request handler.
#[doc(hidden)]
pub type ControllerActionBinder = fn(ErasedController) -> Result<Handler, ControllerBindingError>;

/// Static controller/action binding metadata.
#[doc(hidden)]
#[derive(Clone, Copy)]
pub struct ControllerActionRegistration {
    controller_type_id: TypeId,
    controller_type_name: &'static str,
    bind: ControllerActionBinder,
}

impl ControllerActionRegistration {
    #[doc(hidden)]
    pub fn of<C>(bind: ControllerActionBinder) -> Self
    where
        C: ControllerTrait,
    {
        Self {
            controller_type_id: TypeId::of::<C>(),
            controller_type_name: std::any::type_name::<C>(),
            bind,
        }
    }

    #[doc(hidden)]
    pub const fn controller_type_id(self) -> TypeId {
        self.controller_type_id
    }

    #[doc(hidden)]
    pub const fn controller_type_name(self) -> &'static str {
        self.controller_type_name
    }

    #[doc(hidden)]
    pub fn bind(self, controller: ErasedController) -> Result<Handler, ControllerBindingError> {
        (self.bind)(controller)
    }
}

/// Downcast helper used by generated action binders exactly once at App build.
#[doc(hidden)]
pub fn downcast_controller<C>(
    controller: ErasedController,
) -> Result<Arc<C>, ControllerBindingError>
where
    C: ControllerTrait,
{
    controller
        .downcast::<C>()
        .map_err(|_| ControllerBindingError::type_mismatch(std::any::type_name::<C>()))
}

/// Controller-level metadata generated by `#[derive(Controller)]`.
///
/// The action macro reads this contract without inspecting the original
/// struct, which lets derive and inherent-impl expansion remain independent.
#[doc(hidden)]
pub trait StructControllerDefinition: ControllerTrait {
    fn base_path() -> &'static str;

    fn middleware_registrations() -> Vec<HttpMiddlewareRegistration>;

    fn cors_policy_registration() -> CorsRoutePolicyRegistration;

    /// Whether actions without their own `#[openapi(...)]` declaration inherit
    /// documented status from this controller.
    #[doc(hidden)]
    fn openapi_documented_by_default() -> bool;

    /// Apply controller-level OpenAPI defaults after action-level inference and
    /// overrides have been constructed. Implementations may only fill missing
    /// operation values; action metadata remains authoritative.
    #[doc(hidden)]
    fn apply_openapi_defaults(metadata: &mut OpenApiOperationMetadata);
}

type HttpMiddlewareInitializationFuture = Pin<
    Box<
        dyn Future<Output = Result<Arc<dyn HttpMiddleware>, HttpMiddlewareInitError>>
            + Send
            + 'static,
    >,
>;

/// Type-erased middleware constructor embedded in generated route metadata.
///
/// This is public only so macros expanded in downstream crates can construct
/// metadata. Applications must register middleware through the documented
/// controller and CRUD syntax rather than using this type directly.
#[doc(hidden)]
#[derive(Clone, Copy)]
pub struct HttpMiddlewareRegistration {
    type_id: TypeId,
    type_name: &'static str,
    initialize: fn(Arc<Extensions>) -> HttpMiddlewareInitializationFuture,
}

impl HttpMiddlewareRegistration {
    #[doc(hidden)]
    pub fn of<M>() -> Self
    where
        M: HttpMiddleware,
    {
        Self {
            type_id: TypeId::of::<M>(),
            type_name: std::any::type_name::<M>(),
            initialize: initialize_http_middleware::<M>,
        }
    }

    pub(crate) const fn type_id(self) -> TypeId {
        self.type_id
    }

    pub(crate) const fn type_name(self) -> &'static str {
        self.type_name
    }

    pub(crate) async fn instantiate(
        self,
        extensions: Arc<Extensions>,
    ) -> Result<Arc<dyn HttpMiddleware>, HttpMiddlewareInitError> {
        (self.initialize)(extensions).await
    }
}

fn initialize_http_middleware<M>(extensions: Arc<Extensions>) -> HttpMiddlewareInitializationFuture
where
    M: HttpMiddleware,
{
    Box::pin(async move {
        match AssertUnwindSafe(M::new(extensions)).catch_unwind().await {
            Ok(Ok(middleware)) => Ok(Arc::new(middleware) as Arc<dyn HttpMiddleware>),
            Ok(Err(error)) => Err(error),
            Err(_) => Err(HttpMiddlewareInitError::internal(
                MiddlewareErrorCode::new("MIDDLEWARE_INIT_PANICKED")
                    .expect("the built-in middleware initialization code is valid"),
            )),
        }
    })
}

/// Type-erased, stateless CORS policy provider embedded in generated route metadata.
///
/// Public only for macros expanded in downstream crates. Applications select a provider through
/// controller/CRUD metadata and never construct this registration directly.
#[doc(hidden)]
#[derive(Clone, Copy)]
pub struct CorsPolicyProviderRegistration {
    type_id: TypeId,
    type_name: &'static str,
    provide: fn() -> Result<CorsPolicy, MiddlewareConfigError>,
}

impl CorsPolicyProviderRegistration {
    #[doc(hidden)]
    pub fn of<P>() -> Self
    where
        P: CorsPolicyProvider,
    {
        Self {
            type_id: TypeId::of::<P>(),
            type_name: std::any::type_name::<P>(),
            provide: provide_cors_policy::<P>,
        }
    }

    pub(crate) const fn type_id(self) -> TypeId {
        self.type_id
    }

    pub(crate) const fn type_name(self) -> &'static str {
        self.type_name
    }

    pub(crate) fn policy(self) -> Result<CorsPolicy, MiddlewareConfigError> {
        (self.provide)()
    }
}

fn provide_cors_policy<P>() -> Result<CorsPolicy, MiddlewareConfigError>
where
    P: CorsPolicyProvider,
{
    catch_unwind(AssertUnwindSafe(P::policy)).map_err(|_| {
        MiddlewareConfigError::middleware(
            MiddlewareErrorCode::new("CORS_POLICY_PROVIDER_PANICKED")
                .expect("built-in CORS provider panic code is valid"),
        )
    })
}

/// Build-time CORS inheritance metadata carried by one generated route.
#[doc(hidden)]
#[derive(Clone, Copy)]
pub enum CorsRoutePolicyRegistration {
    Inherit,
    Provider(CorsPolicyProviderRegistration),
}

impl CorsRoutePolicyRegistration {
    #[doc(hidden)]
    pub const fn inherit() -> Self {
        Self::Inherit
    }

    #[doc(hidden)]
    pub fn provider<P>() -> Self
    where
        P: CorsPolicyProvider,
    {
        Self::Provider(CorsPolicyProviderRegistration::of::<P>())
    }

    pub(crate) const fn provider_registration(self) -> Option<CorsPolicyProviderRegistration> {
        match self {
            Self::Inherit => None,
            Self::Provider(registration) => Some(registration),
        }
    }
}

/// Trait to detect if a function returns a Future
pub trait IsAsyncFn<Args> {
    /// Return type produced by the probed callable.
    type Output;
    /// Invokes the callable for generated compile-time signature validation.
    fn call_and_check(&self, args: Args) -> Self::Output;
}

/// Implementation for async functions (returns Future)
impl<F, Args, Fut, T> IsAsyncFn<Args> for F
where
    F: Fn(Args) -> Fut,
    Fut: Future<Output = T>,
{
    type Output = Option<bool>;

    fn call_and_check(&self, _args: Args) -> Self::Output {
        // This is never actually called, it's just used for type checking
        Some(true)
    }
}

/// Helper function to detect if a function is async
///
/// This function is used by the controller macro to detect if a handler
/// function is async or not. It uses type inference to determine if the
/// function returns a Future.
///
/// Note: This function is never actually called at runtime. It's only used
/// for its type signature during compilation.
#[inline(always)]
pub fn is_async_fn<F, Args, R>(_f: &F) -> Option<bool>
where
    F: IsAsyncFn<Args, Output = Option<bool>>,
{
    // This is never actually called, it's just used for type checking
    None
}

/// Marker struct for detecting async functions
pub struct AsyncFnDetector<F, Args>(PhantomData<(F, Args)>);

/// Helper macro to detect if a function is async
#[macro_export]
macro_rules! detect_async {
    ($func:expr) => {
        $crate::private::is_async_fn(&$func).is_some()
    };
}

// Re-export for use in macros
pub use detect_async;

/// Joins one public controller base path and one action path without silently
/// adding an `/api` prefix. Macro callers use this through `__private`; it is
/// kept as a regular function so route inventory tests exercise exactly the
/// runtime value registered in the immutable route table.
#[doc(hidden)]
pub fn join_controller_action_path(base_path: &str, action_path: &str) -> String {
    let base = if base_path == "/" {
        ""
    } else {
        base_path.trim_end_matches('/')
    };
    let action = action_path.trim_start_matches('/');
    match (base.is_empty(), action.is_empty()) {
        (true, true) => "/".to_string(),
        (true, false) => format!("/{action}"),
        (false, true) => base.to_string(),
        (false, false) => format!("{base}/{action}"),
    }
}

#[cfg(test)]
mod route_path_tests {
    use super::join_controller_action_path;

    #[test]
    fn explicit_controller_base_path_preserves_root_and_nested_templates() {
        assert_eq!(join_controller_action_path("/", "/health"), "/health");
        assert_eq!(
            join_controller_action_path("/", "/v1/orders/:order_id"),
            "/v1/orders/:order_id"
        );
        assert_eq!(
            join_controller_action_path("/api/orders/", "create"),
            "/api/orders/create"
        );
    }
}
