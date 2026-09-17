use std::collections::{BTreeMap, HashMap, HashSet};
use std::panic::{catch_unwind, AssertUnwindSafe};

use percent_encoding::percent_decode_str;
use serde::Serialize;
use utoipa::openapi::path::{HttpMethod, Operation, ParameterIn, Paths};
use utoipa::openapi::schema::{AdditionalProperties, ArrayItems, Components};
use utoipa::openapi::{RefOr, Schema};

use super::registry::RouteInfo;
use crate::openapi::{OpenApiConfig, OpenApiSecurityValidationError};
use crate::private::{
    OpenApiMetadataComponentKind, OpenApiOperationFactory, OpenApiOperationMetadataIssue,
};
use crate::route_table::{RouteTable, RouteTableBuildError};

/// Explicit documentation state carried by one struct-controller route.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenApiRouteMetadataStatus {
    Documented,
    Skipped,
    Unspecified,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum OpenApiRouteRegistration {
    Documented(OpenApiOperationFactory),
    Skipped,
    Unspecified,
}

impl OpenApiRouteRegistration {
    pub(super) const fn status(self) -> OpenApiRouteMetadataStatus {
        match self {
            Self::Documented(_) => OpenApiRouteMetadataStatus::Documented,
            Self::Skipped => OpenApiRouteMetadataStatus::Skipped,
            Self::Unspecified => OpenApiRouteMetadataStatus::Unspecified,
        }
    }

    pub(super) const fn factory(self) -> Option<OpenApiOperationFactory> {
        match self {
            Self::Documented(factory) => Some(factory),
            Self::Skipped | Self::Unspecified => None,
        }
    }
}

#[derive(Debug)]
pub(super) struct MaterializedControllerRoute {
    route: RouteInfo,
    openapi: OpenApiRouteRegistration,
}

impl MaterializedControllerRoute {
    pub(super) const fn new(route: RouteInfo, openapi: OpenApiRouteRegistration) -> Self {
        Self { route, openapi }
    }
}

/// One App build's controller routes before the authoritative RouteTable has
/// accepted them.
///
/// Each entry keeps the runtime route and its OpenAPI state together. The
/// normal App path calls [`Self::into_route_table`], which never executes an
/// operation factory. An OpenAPI-enabled App will instead call
/// [`Self::into_openapi_route_table`], which constructs the RouteTable first
/// and invokes factories only after that succeeds.
#[doc(hidden)]
#[derive(Debug)]
pub struct ControllerRouteMaterialization {
    routes: Vec<MaterializedControllerRoute>,
}

impl ControllerRouteMaterialization {
    pub(super) const fn new(routes: Vec<MaterializedControllerRoute>) -> Self {
        Self { routes }
    }

    #[doc(hidden)]
    pub fn len(&self) -> usize {
        self.routes.len()
    }

    #[doc(hidden)]
    pub fn is_empty(&self) -> bool {
        self.routes.is_empty()
    }

    #[doc(hidden)]
    pub fn routes(&self) -> impl Iterator<Item = &RouteInfo> {
        self.routes.iter().map(|route| &route.route)
    }

    /// Complete the normal, OpenAPI-disabled route build without executing
    /// any operation factory.
    #[doc(hidden)]
    pub fn into_route_table(self) -> Result<RouteTable, RouteTableBuildError> {
        RouteTable::from_routes(self.routes.into_iter().map(|route| route.route).collect())
    }

    /// Complete an OpenAPI-enabled route build.
    ///
    /// The RouteTable is constructed first. Duplicate, invalid or ambiguous
    /// routes therefore fail before any schema/operation code is invoked.
    #[doc(hidden)]
    pub fn into_openapi_route_table(
        self,
    ) -> Result<(RouteTable, OpenApiRouteRegistry), OpenApiRouteBuildError> {
        let mut candidates = Vec::with_capacity(self.routes.len());
        let mut routes = Vec::with_capacity(self.routes.len());
        for materialized in self.routes {
            candidates.push(OpenApiRouteCandidate {
                method: materialized.route.method.clone(),
                path: materialized.route.path.clone(),
                handler_name: materialized.route.handler_name.clone(),
                registration: materialized.openapi,
            });
            routes.push(materialized.route);
        }

        let route_table = RouteTable::from_routes(routes).map_err(OpenApiRouteBuildError::Route)?;
        let registry = OpenApiRouteRegistry::from_accepted_routes(&route_table, candidates)
            .map_err(|error| OpenApiRouteBuildError::Registry(Box::new(error)))?;
        Ok((route_table, registry))
    }
}

#[derive(Clone)]
struct OpenApiRouteCandidate {
    method: String,
    path: String,
    handler_name: String,
    registration: OpenApiRouteRegistration,
}

/// One accepted route's resolved OpenAPI state.
#[doc(hidden)]
#[derive(Clone)]
pub struct RegisteredOpenApiRoute {
    method: String,
    route_path: String,
    document_path: String,
    handler_name: String,
    status: OpenApiRouteMetadataStatus,
    operation: Option<Operation>,
}

impl RegisteredOpenApiRoute {
    #[doc(hidden)]
    pub fn method(&self) -> &str {
        &self.method
    }

    #[doc(hidden)]
    pub fn route_path(&self) -> &str {
        &self.route_path
    }

    #[doc(hidden)]
    pub fn document_path(&self) -> &str {
        &self.document_path
    }

    #[doc(hidden)]
    pub fn handler_name(&self) -> &str {
        &self.handler_name
    }

    #[doc(hidden)]
    pub const fn status(&self) -> OpenApiRouteMetadataStatus {
        self.status
    }

    #[doc(hidden)]
    pub const fn operation(&self) -> Option<&Operation> {
        self.operation.as_ref()
    }
}

/// Deterministic build-time OpenAPI inventory derived from accepted routes.
///
/// This is not a second registration mechanism. It is produced from the same
/// App-local controller route materialization after RouteTable acceptance and
/// is intended to be consumed once by the CAP-07E document owner.
#[doc(hidden)]
pub struct OpenApiRouteRegistry {
    routes: Vec<RegisteredOpenApiRoute>,
    paths: Paths,
    components: Components,
}

impl std::fmt::Debug for OpenApiRouteRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OpenApiRouteRegistry")
            .field("routes", &self.routes.len())
            .field("document_paths", &self.paths.paths.len())
            .field("schema_components", &self.components.schemas.len())
            .finish()
    }
}

impl OpenApiRouteRegistry {
    fn from_accepted_routes(
        route_table: &RouteTable,
        mut candidates: Vec<OpenApiRouteCandidate>,
    ) -> Result<Self, OpenApiRouteRegistryError> {
        reconcile_with_route_table(route_table, &candidates)?;

        candidates.sort_by(|left, right| {
            openapi_document_path(&left.path)
                .cmp(&openapi_document_path(&right.path))
                .then_with(|| left.method.cmp(&right.method))
                .then_with(|| left.handler_name.cmp(&right.handler_name))
        });

        if let Some(candidate) = candidates.iter().find(|candidate| {
            matches!(
                candidate.registration,
                OpenApiRouteRegistration::Unspecified
            )
        }) {
            return Err(OpenApiRouteRegistryError::UnspecifiedRoute {
                method: candidate.method.clone(),
                path: candidate.path.clone(),
                handler: candidate.handler_name.clone(),
            });
        }

        let mut routes = Vec::with_capacity(candidates.len());
        let mut paths = Paths::new();
        let mut components = Components::new();
        let mut operation_ids = HashMap::<String, RouteIdentity>::new();
        let mut document_operations = HashMap::<(String, String), RouteIdentity>::new();
        let mut component_owners = ComponentOwners::default();

        for candidate in candidates {
            let status = candidate.registration.status();
            let document_path = openapi_document_path(&candidate.path);
            let mut operation = None;

            if let OpenApiRouteRegistration::Documented(factory) = candidate.registration {
                let method = openapi_http_method(&candidate.method).ok_or_else(|| {
                    OpenApiRouteRegistryError::UnsupportedMethod {
                        method: candidate.method.clone(),
                        path: candidate.path.clone(),
                        handler: candidate.handler_name.clone(),
                    }
                })?;
                let identity = RouteIdentity::from_candidate(&candidate);
                let document_key = (candidate.method.clone(), document_path.clone());
                if let Some(first) = document_operations.insert(document_key, identity.clone()) {
                    return Err(OpenApiRouteRegistryError::DuplicateDocumentOperation(
                        Box::new(OpenApiDuplicateDocumentOperation {
                            method: candidate.method.clone(),
                            document_path,
                            first_route_path: first.path,
                            first_handler: first.handler,
                            duplicate_route_path: candidate.path,
                            duplicate_handler: candidate.handler_name,
                        }),
                    ));
                }

                let mut metadata = catch_unwind(AssertUnwindSafe(factory)).map_err(|_| {
                    OpenApiRouteRegistryError::OperationFactoryPanicked {
                        method: candidate.method.clone(),
                        path: candidate.path.clone(),
                        handler: candidate.handler_name.clone(),
                    }
                })?;
                if let Some(issue) = metadata.issues().first() {
                    return Err(match issue {
                        OpenApiOperationMetadataIssue::ComponentCollision {
                            kind,
                            name,
                            first_type,
                            duplicate_type,
                        } => OpenApiRouteRegistryError::OperationComponentCollision {
                            kind: match kind {
                                OpenApiMetadataComponentKind::Schema => {
                                    OpenApiComponentKind::Schema
                                }
                                OpenApiMetadataComponentKind::Response => {
                                    OpenApiComponentKind::Response
                                }
                            },
                            name: name.clone(),
                            first_type: (*first_type).to_owned(),
                            duplicate_type: (*duplicate_type).to_owned(),
                            handler: candidate.handler_name.clone(),
                        },
                        OpenApiOperationMetadataIssue::ResponseStatusCollision {
                            status,
                            first_source,
                            duplicate_source,
                        } => OpenApiRouteRegistryError::ResponseStatusCollision {
                            status: status.clone(),
                            first_source: (*first_source).to_owned(),
                            duplicate_source: (*duplicate_source).to_owned(),
                            handler: candidate.handler_name.clone(),
                        },
                    });
                }
                normalize_operation(metadata.operation_mut());
                validate_operation_contract(&candidate, metadata.operation())?;

                if let Some(operation_id) = metadata.operation().operation_id.as_ref() {
                    if let Some(first) = operation_ids.insert(operation_id.clone(), identity) {
                        return Err(OpenApiRouteRegistryError::DuplicateOperationId(Box::new(
                            OpenApiDuplicateOperationId {
                                operation_id: operation_id.clone(),
                                first_method: first.method,
                                first_path: first.path,
                                first_handler: first.handler,
                                duplicate_method: candidate.method.clone(),
                                duplicate_path: candidate.path.clone(),
                                duplicate_handler: candidate.handler_name.clone(),
                            },
                        )));
                    }
                }

                merge_components(
                    &mut components,
                    metadata.components(),
                    &candidate,
                    &mut component_owners,
                )?;
                let (resolved_operation, _) = metadata.into_parts();
                paths.add_path_operation(&document_path, vec![method], resolved_operation.clone());
                operation = Some(resolved_operation);
            }

            routes.push(RegisteredOpenApiRoute {
                method: candidate.method,
                route_path: candidate.path,
                document_path,
                handler_name: candidate.handler_name,
                status,
                operation,
            });
        }

        validate_final_response_contracts(&routes, &components)?;
        validate_local_reference_integrity(&paths, &components)?;

        Ok(Self {
            routes,
            paths,
            components,
        })
    }

    #[doc(hidden)]
    pub fn routes(&self) -> impl Iterator<Item = &RegisteredOpenApiRoute> {
        self.routes.iter()
    }

    #[doc(hidden)]
    pub const fn paths(&self) -> &Paths {
        &self.paths
    }

    #[doc(hidden)]
    pub const fn components(&self) -> &Components {
        &self.components
    }

    /// Validate every operation security reference against the application
    /// configuration and attach the validated schemes to components.
    ///
    /// CAP-07E calls this once while constructing the immutable document.
    #[doc(hidden)]
    pub fn apply_security_config(
        &mut self,
        config: &OpenApiConfig,
    ) -> Result<(), OpenApiSecurityValidationError> {
        for route in &self.routes {
            let Some(operation) = route.operation.as_ref() else {
                continue;
            };
            let Some(requirements) = operation.security.as_ref() else {
                continue;
            };
            for requirement in requirements {
                let value = serde_json::to_value(requirement).map_err(|_| {
                    OpenApiSecurityValidationError::InvalidRequirement {
                        method: route.method.clone(),
                        path: route.route_path.clone(),
                        handler: route.handler_name.clone(),
                    }
                })?;
                let object = value.as_object().ok_or_else(|| {
                    OpenApiSecurityValidationError::InvalidRequirement {
                        method: route.method.clone(),
                        path: route.route_path.clone(),
                        handler: route.handler_name.clone(),
                    }
                })?;
                for (name, scopes) in object {
                    let accepts_scopes = config.accepts_scopes(name).ok_or_else(|| {
                        OpenApiSecurityValidationError::UnknownScheme {
                            name: name.clone(),
                            method: route.method.clone(),
                            path: route.route_path.clone(),
                            handler: route.handler_name.clone(),
                        }
                    })?;
                    let scopes = scopes.as_array().ok_or_else(|| {
                        OpenApiSecurityValidationError::InvalidRequirement {
                            method: route.method.clone(),
                            path: route.route_path.clone(),
                            handler: route.handler_name.clone(),
                        }
                    })?;
                    if !accepts_scopes && !scopes.is_empty() {
                        return Err(OpenApiSecurityValidationError::SchemeTypeMismatch {
                            name: name.clone(),
                            method: route.method.clone(),
                            path: route.route_path.clone(),
                            handler: route.handler_name.clone(),
                        });
                    }
                }
            }
        }

        for (name, _) in config.security_schemes() {
            if self.components.security_schemes.contains_key(name) {
                return Err(OpenApiSecurityValidationError::ComponentCollision {
                    name: name.to_owned(),
                });
            }
        }
        for (name, scheme) in config.security_schemes() {
            self.components
                .security_schemes
                .insert(name.to_owned(), scheme.clone());
        }
        Ok(())
    }

    /// Serialize the deterministic paths/components fragment used by the
    /// final document owner. The field order is fixed and all specification
    /// maps are backed by sorted maps.
    #[doc(hidden)]
    pub fn canonical_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        #[derive(Serialize)]
        struct CanonicalFragment<'a> {
            paths: &'a Paths,
            components: &'a Components,
        }

        serde_json::to_vec(&CanonicalFragment {
            paths: &self.paths,
            components: &self.components,
        })
    }
}

/// Failure while constructing the runtime route table and its OpenAPI inventory.
///
/// Route acceptance always runs first. Operation factories and OpenAPI contract
/// validation run only after the authoritative runtime table has accepted every
/// route.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpenApiRouteBuildError {
    /// The authoritative runtime route table rejected a route definition.
    Route(RouteTableBuildError),
    /// Runtime routes were accepted, but their OpenAPI inventory was invalid.
    Registry(Box<OpenApiRouteRegistryError>),
}

impl std::fmt::Display for OpenApiRouteBuildError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Route(error) => error.fmt(formatter),
            Self::Registry(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for OpenApiRouteBuildError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Route(error) => Some(error),
            Self::Registry(error) => Some(error.as_ref()),
        }
    }
}

/// Typed OpenAPI route registry failures produced before App publication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpenApiRouteRegistryError {
    /// The number of OpenAPI metadata entries differs from the number of
    /// routes accepted by the authoritative runtime route table.
    MetadataCountMismatch {
        /// Number of routes accepted by the runtime route table.
        accepted_routes: usize,
        /// Number of route metadata entries supplied to OpenAPI reconciliation.
        metadata_routes: usize,
    },
    /// OpenAPI metadata identifies a route that is not present in the accepted
    /// runtime route table with the same method, path, and handler.
    MetadataMismatch {
        /// Runtime HTTP method recorded by the metadata entry.
        method: String,
        /// Lily runtime route path recorded by the metadata entry.
        path: String,
        /// Handler identity recorded by the metadata entry.
        handler: String,
    },
    /// More than one metadata entry claims the same runtime method and path.
    DuplicateMetadataRoute {
        /// Conflicting runtime HTTP method.
        method: String,
        /// Conflicting Lily runtime route path.
        path: String,
        /// Handler carried by the first metadata entry.
        first_handler: String,
        /// Handler carried by the duplicate metadata entry.
        duplicate_handler: String,
    },
    /// Two accepted Lily routes normalize to the same OpenAPI method and
    /// document path.
    DuplicateDocumentOperation(Box<OpenApiDuplicateDocumentOperation>),
    /// Two documented operations declare the same document-wide operation ID.
    DuplicateOperationId(Box<OpenApiDuplicateOperationId>),
    /// A documented route uses a custom HTTP method that the OpenAPI model
    /// cannot represent.
    UnsupportedMethod {
        /// Unsupported runtime HTTP method.
        method: String,
        /// Lily runtime route path.
        path: String,
        /// Handler owning the documented route.
        handler: String,
    },
    /// Generated OpenAPI metadata panicked while its operation factory ran.
    ///
    /// The panic payload is deliberately not retained or exposed.
    OperationFactoryPanicked {
        /// Runtime HTTP method of the failing operation.
        method: String,
        /// Lily runtime route path of the failing operation.
        path: String,
        /// Handler whose generated operation factory panicked.
        handler: String,
    },
    /// An OpenAPI-enabled application contains a route that is neither
    /// documented nor explicitly marked to be skipped.
    UnspecifiedRoute {
        /// Runtime HTTP method of the unspecified route.
        method: String,
        /// Lily runtime route path of the unspecified route.
        path: String,
        /// Handler owning the unspecified route.
        handler: String,
    },
    /// A Lily route path cannot be translated into an OpenAPI path template.
    InvalidPathTemplate {
        /// Runtime HTTP method of the invalid route.
        method: String,
        /// Invalid Lily runtime route path.
        path: String,
        /// Handler owning the invalid route.
        handler: String,
    },
    /// A Lily route path repeats the same placeholder name.
    DuplicatePathPlaceholder {
        /// Repeated placeholder name, without the `:` or `*` prefix.
        name: String,
        /// Runtime HTTP method of the invalid route.
        method: String,
        /// Lily runtime route path containing the duplicate placeholder.
        path: String,
        /// Handler owning the invalid route.
        handler: String,
    },
    /// One operation declares the same parameter name and OpenAPI location
    /// more than once.
    DuplicateOperationParameter {
        /// Duplicated parameter name.
        name: String,
        /// OpenAPI parameter location such as `path`, `query`, `header`, or
        /// `cookie`.
        location: String,
        /// Runtime HTTP method of the operation.
        method: String,
        /// Lily runtime route path of the operation.
        path: String,
        /// Handler owning the operation.
        handler: String,
    },
    /// A Lily route placeholder has no corresponding OpenAPI path parameter.
    MissingPathParameter {
        /// Undocumented route placeholder name.
        name: String,
        /// Runtime HTTP method of the operation.
        method: String,
        /// Lily runtime route path of the operation.
        path: String,
        /// Handler owning the operation.
        handler: String,
    },
    /// An OpenAPI path parameter does not correspond to a placeholder in the
    /// accepted Lily route.
    UnexpectedPathParameter {
        /// Unexpected OpenAPI path parameter name.
        name: String,
        /// Runtime HTTP method of the operation.
        method: String,
        /// Lily runtime route path of the operation.
        path: String,
        /// Handler owning the operation.
        handler: String,
    },
    /// A route placeholder is documented at a non-path parameter location.
    PathParameterSourceMismatch {
        /// Route placeholder and operation parameter name.
        name: String,
        /// Actual OpenAPI parameter location.
        actual: String,
        /// Runtime HTTP method of the operation.
        method: String,
        /// Lily runtime route path of the operation.
        path: String,
        /// Handler owning the operation.
        handler: String,
    },
    /// An OpenAPI path parameter is not marked as required.
    PathParameterNotRequired {
        /// Non-required path parameter name.
        name: String,
        /// Runtime HTTP method of the operation.
        method: String,
        /// Lily runtime route path of the operation.
        path: String,
        /// Handler owning the operation.
        handler: String,
    },
    /// An OpenAPI path parameter has no schema contract.
    PathParameterMissingSchema {
        /// Path parameter lacking a schema.
        name: String,
        /// Runtime HTTP method of the operation.
        method: String,
        /// Lily runtime route path of the operation.
        path: String,
        /// Handler owning the operation.
        handler: String,
    },
    /// An operation declares a request body without any media-type entry.
    EmptyRequestBody {
        /// Runtime HTTP method of the operation.
        method: String,
        /// Lily runtime route path of the operation.
        path: String,
        /// Handler owning the operation.
        handler: String,
    },
    /// A request-body media type is empty or does not define a schema.
    InvalidRequestBodyContent {
        /// Invalid request-body media-type key.
        content_type: String,
        /// Runtime HTTP method of the operation.
        method: String,
        /// Lily runtime route path of the operation.
        path: String,
        /// Handler owning the operation.
        handler: String,
    },
    /// A documented operation contains no response contract.
    MissingResponses {
        /// Runtime HTTP method of the operation.
        method: String,
        /// Lily runtime route path of the operation.
        path: String,
        /// Handler owning the operation.
        handler: String,
    },
    /// A response key is neither `default` nor an HTTP status from 100 through
    /// 599.
    InvalidResponseStatus {
        /// Invalid OpenAPI response status key.
        status: String,
        /// Runtime HTTP method of the operation.
        method: String,
        /// Lily runtime route path of the operation.
        path: String,
        /// Handler owning the operation.
        handler: String,
    },
    /// An inline or referenced response resolves to an empty description.
    MissingResponseDescription {
        /// Response status whose description is missing.
        status: String,
        /// Runtime HTTP method of the operation.
        method: String,
        /// Lily runtime route path of the operation.
        path: String,
        /// Handler owning the operation.
        handler: String,
    },
    /// A response component reference is unresolved, malformed, or cyclic.
    InvalidResponseReference {
        /// Response status using the invalid reference.
        status: String,
        /// Invalid OpenAPI response reference.
        reference: String,
        /// Runtime HTTP method of the operation.
        method: String,
        /// Lily runtime route path of the operation.
        path: String,
        /// Handler owning the operation.
        handler: String,
    },
    /// A direct local schema or response component reference does not resolve
    /// in the final merged document, or a response component chain is cyclic.
    InvalidLocalReference {
        /// Component namespace required by the reference site.
        kind: OpenApiComponentKind,
        /// Invalid local OpenAPI reference.
        reference: String,
    },
    /// Two operations contribute different definitions for the same global
    /// OpenAPI component name.
    ComponentCollision {
        /// Kind of component whose name collided.
        kind: OpenApiComponentKind,
        /// Conflicting OpenAPI component name.
        name: String,
        /// Handler that first registered the component definition.
        first_handler: String,
        /// Handler that attempted to register the conflicting definition.
        duplicate_handler: String,
    },
    /// Two Rust types within one generated operation contribute different
    /// definitions for the same component name.
    OperationComponentCollision {
        /// Kind of component whose name collided.
        kind: OpenApiComponentKind,
        /// Conflicting OpenAPI component name.
        name: String,
        /// Rust type that first contributed the component definition.
        first_type: String,
        /// Rust type that contributed the conflicting definition.
        duplicate_type: String,
        /// Handler whose generated operation contains the collision.
        handler: String,
    },
    /// Two response metadata sources within one generated operation claim the
    /// same status key.
    ResponseStatusCollision {
        /// Conflicting OpenAPI response status key.
        status: String,
        /// Source that first contributed the status.
        first_source: String,
        /// Source that attempted to contribute the same status again.
        duplicate_source: String,
        /// Handler whose generated operation contains the collision.
        handler: String,
    },
}

/// Details of two accepted Lily routes that collapse to one OpenAPI operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenApiDuplicateDocumentOperation {
    /// HTTP method shared by both documented routes.
    pub method: String,
    /// Normalized OpenAPI path produced by both Lily route paths.
    pub document_path: String,
    /// Lily runtime route path accepted first.
    pub first_route_path: String,
    /// Handler owning the first route.
    pub first_handler: String,
    /// Lily runtime route path that produced the duplicate operation.
    pub duplicate_route_path: String,
    /// Handler owning the duplicate route.
    pub duplicate_handler: String,
}

/// Details of two documented operations that declare the same operation ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenApiDuplicateOperationId {
    /// Duplicate document-wide OpenAPI operation ID.
    pub operation_id: String,
    /// HTTP method of the operation registered first.
    pub first_method: String,
    /// Lily runtime route path of the operation registered first.
    pub first_path: String,
    /// Handler owning the operation registered first.
    pub first_handler: String,
    /// HTTP method of the operation that reused the ID.
    pub duplicate_method: String,
    /// Lily runtime route path of the operation that reused the ID.
    pub duplicate_path: String,
    /// Handler owning the operation that reused the ID.
    pub duplicate_handler: String,
}

impl std::fmt::Display for OpenApiRouteRegistryError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MetadataCountMismatch {
                accepted_routes,
                metadata_routes,
            } => write!(
                formatter,
                "OpenAPI route metadata count ({metadata_routes}) does not match accepted route count ({accepted_routes})"
            ),
            Self::MetadataMismatch {
                method,
                path,
                handler,
            } => write!(
                formatter,
                "OpenAPI metadata for {method} {path} (handler '{handler}') has no matching accepted route"
            ),
            Self::DuplicateMetadataRoute {
                method,
                path,
                first_handler,
                duplicate_handler,
            } => write!(
                formatter,
                "duplicate OpenAPI route metadata for {method} {path}: '{first_handler}' and '{duplicate_handler}'"
            ),
            Self::DuplicateDocumentOperation(error) => write!(
                formatter,
                "routes {} {} ('{}') and {} {} ('{}') both map to OpenAPI operation {} {}",
                error.method,
                error.first_route_path,
                error.first_handler,
                error.method,
                error.duplicate_route_path,
                error.duplicate_handler,
                error.method,
                error.document_path,
            ),
            Self::DuplicateOperationId(error) => write!(
                formatter,
                "duplicate OpenAPI operation_id '{}' on {} {} ('{}') and {} {} ('{}')",
                error.operation_id,
                error.first_method,
                error.first_path,
                error.first_handler,
                error.duplicate_method,
                error.duplicate_path,
                error.duplicate_handler,
            ),
            Self::UnsupportedMethod {
                method,
                path,
                handler,
            } => write!(
                formatter,
                "documented route {method} {path} (handler '{handler}') uses an HTTP method OpenAPI cannot represent"
            ),
            Self::OperationFactoryPanicked {
                method,
                path,
                handler,
            } => write!(
                formatter,
                "OpenAPI operation factory panicked for {method} {path} (handler '{handler}')"
            ),
            Self::UnspecifiedRoute {
                method,
                path,
                handler,
            } => write!(
                formatter,
                "OpenAPI-enabled application requires {method} {path} (handler '{handler}') to be documented or explicitly skipped"
            ),
            Self::InvalidPathTemplate {
                method,
                path,
                handler,
            } => write!(
                formatter,
                "route {method} {path} (handler '{handler}') contains a path template that is not representable from Lily route placeholders"
            ),
            Self::DuplicatePathPlaceholder {
                name,
                method,
                path,
                handler,
            } => write!(
                formatter,
                "route {method} {path} (handler '{handler}') declares path placeholder '{name}' more than once"
            ),
            Self::DuplicateOperationParameter {
                name,
                location,
                method,
                path,
                handler,
            } => write!(
                formatter,
                "OpenAPI operation for {method} {path} (handler '{handler}') declares duplicate {location} parameter '{name}'"
            ),
            Self::MissingPathParameter {
                name,
                method,
                path,
                handler,
            } => write!(
                formatter,
                "OpenAPI operation for {method} {path} (handler '{handler}') does not document path placeholder '{name}'"
            ),
            Self::UnexpectedPathParameter {
                name,
                method,
                path,
                handler,
            } => write!(
                formatter,
                "OpenAPI operation for {method} {path} (handler '{handler}') documents path parameter '{name}' which is absent from the route"
            ),
            Self::PathParameterSourceMismatch {
                name,
                actual,
                method,
                path,
                handler,
            } => write!(
                formatter,
                "OpenAPI operation for {method} {path} (handler '{handler}') documents route placeholder '{name}' as a {actual} parameter instead of a path parameter"
            ),
            Self::PathParameterNotRequired {
                name,
                method,
                path,
                handler,
            } => write!(
                formatter,
                "OpenAPI path parameter '{name}' for {method} {path} (handler '{handler}') must be required"
            ),
            Self::PathParameterMissingSchema {
                name,
                method,
                path,
                handler,
            } => write!(
                formatter,
                "OpenAPI path parameter '{name}' for {method} {path} (handler '{handler}') must define a schema"
            ),
            Self::EmptyRequestBody {
                method,
                path,
                handler,
            } => write!(
                formatter,
                "OpenAPI request body for {method} {path} (handler '{handler}') has no media type and schema contract"
            ),
            Self::InvalidRequestBodyContent {
                content_type,
                method,
                path,
                handler,
            } => write!(
                formatter,
                "OpenAPI request body media type '{content_type}' for {method} {path} (handler '{handler}') is empty or has no schema"
            ),
            Self::MissingResponses {
                method,
                path,
                handler,
            } => write!(
                formatter,
                "OpenAPI operation for {method} {path} (handler '{handler}') must document at least one response"
            ),
            Self::InvalidResponseStatus {
                status,
                method,
                path,
                handler,
            } => write!(
                formatter,
                "OpenAPI response status '{status}' for {method} {path} (handler '{handler}') is not default or an HTTP status between 100 and 599"
            ),
            Self::MissingResponseDescription {
                status,
                method,
                path,
                handler,
            } => write!(
                formatter,
                "OpenAPI response '{status}' for {method} {path} (handler '{handler}') must resolve to a non-empty description"
            ),
            Self::InvalidResponseReference {
                status,
                reference,
                method,
                path,
                handler,
            } => write!(
                formatter,
                "OpenAPI response '{status}' for {method} {path} (handler '{handler}') has an unresolved or cyclic reference '{reference}'"
            ),
            Self::InvalidLocalReference { kind, reference } => write!(
                formatter,
                "OpenAPI {kind} reference '{reference}' does not resolve in the final document"
            ),
            Self::ComponentCollision {
                kind,
                name,
                first_handler,
                duplicate_handler,
            } => write!(
                formatter,
                "OpenAPI {kind} component '{name}' has conflicting definitions from '{first_handler}' and '{duplicate_handler}'"
            ),
            Self::OperationComponentCollision {
                kind,
                name,
                first_type,
                duplicate_type,
                handler,
            } => write!(
                formatter,
                "OpenAPI {kind} component '{name}' has conflicting definitions from Rust types '{first_type}' and '{duplicate_type}' in '{handler}'"
            ),
            Self::ResponseStatusCollision {
                status,
                first_source,
                duplicate_source,
                handler,
            } => write!(
                formatter,
                "OpenAPI response status '{status}' from '{duplicate_source}' conflicts with '{first_source}' in '{handler}'"
            ),
        }
    }
}

impl std::error::Error for OpenApiRouteRegistryError {}

/// OpenAPI component namespace used in registry diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenApiComponentKind {
    /// A reusable schema under `components.schemas`.
    Schema,
    /// A reusable response under `components.responses`.
    Response,
    /// A security scheme under `components.securitySchemes`.
    SecurityScheme,
}

impl std::fmt::Display for OpenApiComponentKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Schema => "schema",
            Self::Response => "response",
            Self::SecurityScheme => "security scheme",
        })
    }
}

#[derive(Clone)]
struct RouteIdentity {
    method: String,
    path: String,
    handler: String,
}

impl RouteIdentity {
    fn from_candidate(candidate: &OpenApiRouteCandidate) -> Self {
        Self {
            method: candidate.method.clone(),
            path: candidate.path.clone(),
            handler: candidate.handler_name.clone(),
        }
    }
}

fn reconcile_with_route_table(
    route_table: &RouteTable,
    candidates: &[OpenApiRouteCandidate],
) -> Result<(), OpenApiRouteRegistryError> {
    if route_table.route_count() != candidates.len() {
        return Err(OpenApiRouteRegistryError::MetadataCountMismatch {
            accepted_routes: route_table.route_count(),
            metadata_routes: candidates.len(),
        });
    }

    let accepted = route_table
        .routes()
        .map(|route| {
            (
                (route.method.as_str(), route.path.as_str()),
                route.handler_name.as_str(),
            )
        })
        .collect::<HashMap<_, _>>();
    let mut metadata_routes = HashMap::<(&str, &str), &str>::new();
    for candidate in candidates {
        let key = (candidate.method.as_str(), candidate.path.as_str());
        if let Some(first_handler) = metadata_routes.insert(key, &candidate.handler_name) {
            return Err(OpenApiRouteRegistryError::DuplicateMetadataRoute {
                method: candidate.method.clone(),
                path: candidate.path.clone(),
                first_handler: first_handler.to_owned(),
                duplicate_handler: candidate.handler_name.clone(),
            });
        }
        if accepted.get(&key).copied() != Some(candidate.handler_name.as_str()) {
            return Err(OpenApiRouteRegistryError::MetadataMismatch {
                method: candidate.method.clone(),
                path: candidate.path.clone(),
                handler: candidate.handler_name.clone(),
            });
        }
    }
    Ok(())
}

fn openapi_http_method(method: &str) -> Option<HttpMethod> {
    match method {
        "GET" => Some(HttpMethod::Get),
        "POST" => Some(HttpMethod::Post),
        "PUT" => Some(HttpMethod::Put),
        "DELETE" => Some(HttpMethod::Delete),
        "OPTIONS" => Some(HttpMethod::Options),
        "HEAD" => Some(HttpMethod::Head),
        "PATCH" => Some(HttpMethod::Patch),
        "TRACE" => Some(HttpMethod::Trace),
        _ => None,
    }
}

fn openapi_document_path(route_path: &str) -> String {
    route_path
        .split('/')
        .map(|segment| {
            segment
                .strip_prefix(':')
                .or_else(|| segment.strip_prefix('*'))
                .map_or_else(|| segment.to_owned(), |name| format!("{{{name}}}"))
        })
        .collect::<Vec<_>>()
        .join("/")
}

fn normalize_operation(operation: &mut Operation) {
    if let Some(parameters) = operation.parameters.as_mut() {
        parameters.sort_by(|left, right| {
            parameter_location_rank(&left.parameter_in)
                .cmp(&parameter_location_rank(&right.parameter_in))
                .then_with(|| left.name.cmp(&right.name))
        });
    }
    if let Some(tags) = operation.tags.as_mut() {
        tags.sort();
        tags.dedup();
    }
}

const fn parameter_location_rank(location: &ParameterIn) -> u8 {
    match location {
        ParameterIn::Path => 0,
        ParameterIn::Query => 1,
        ParameterIn::Header => 2,
        ParameterIn::Cookie => 3,
    }
}

fn validate_operation_contract(
    candidate: &OpenApiRouteCandidate,
    operation: &Operation,
) -> Result<(), OpenApiRouteRegistryError> {
    validate_path_parameters(candidate, operation)?;
    validate_request_body(candidate, operation)?;
    validate_response_shape(candidate, operation)
}

fn validate_path_parameters(
    candidate: &OpenApiRouteCandidate,
    operation: &Operation,
) -> Result<(), OpenApiRouteRegistryError> {
    let mut placeholders = Vec::new();
    let mut placeholder_names = HashSet::new();
    for segment in candidate.path.split('/') {
        if let Some(name) = segment
            .strip_prefix(':')
            .or_else(|| segment.strip_prefix('*'))
        {
            if name.is_empty() || name.contains('{') || name.contains('}') {
                return Err(path_error(candidate));
            }
            if !placeholder_names.insert(name.to_owned()) {
                return Err(OpenApiRouteRegistryError::DuplicatePathPlaceholder {
                    name: name.to_owned(),
                    method: candidate.method.clone(),
                    path: candidate.path.clone(),
                    handler: candidate.handler_name.clone(),
                });
            }
            placeholders.push(name.to_owned());
        } else if segment.contains('{') || segment.contains('}') {
            return Err(path_error(candidate));
        }
    }

    let mut identities = HashSet::new();
    let mut path_parameters = HashMap::new();
    for parameter in operation.parameters.iter().flatten() {
        let location = parameter_location_label(&parameter.parameter_in);
        if !identities.insert((location, parameter.name.as_str())) {
            return Err(OpenApiRouteRegistryError::DuplicateOperationParameter {
                name: parameter.name.clone(),
                location: location.to_owned(),
                method: candidate.method.clone(),
                path: candidate.path.clone(),
                handler: candidate.handler_name.clone(),
            });
        }
        if parameter.parameter_in == ParameterIn::Path {
            path_parameters.insert(parameter.name.as_str(), parameter);
        }
    }

    for placeholder in &placeholders {
        let Some(parameter) = path_parameters.get(placeholder.as_str()) else {
            if let Some(parameter) = operation.parameters.iter().flatten().find(|parameter| {
                parameter.name == *placeholder && parameter.parameter_in != ParameterIn::Path
            }) {
                return Err(OpenApiRouteRegistryError::PathParameterSourceMismatch {
                    name: placeholder.clone(),
                    actual: parameter_location_label(&parameter.parameter_in).to_owned(),
                    method: candidate.method.clone(),
                    path: candidate.path.clone(),
                    handler: candidate.handler_name.clone(),
                });
            }
            return Err(OpenApiRouteRegistryError::MissingPathParameter {
                name: placeholder.clone(),
                method: candidate.method.clone(),
                path: candidate.path.clone(),
                handler: candidate.handler_name.clone(),
            });
        };
        if parameter.required != utoipa::openapi::Required::True {
            return Err(OpenApiRouteRegistryError::PathParameterNotRequired {
                name: placeholder.clone(),
                method: candidate.method.clone(),
                path: candidate.path.clone(),
                handler: candidate.handler_name.clone(),
            });
        }
        if parameter.schema.is_none() {
            return Err(OpenApiRouteRegistryError::PathParameterMissingSchema {
                name: placeholder.clone(),
                method: candidate.method.clone(),
                path: candidate.path.clone(),
                handler: candidate.handler_name.clone(),
            });
        }
    }

    if let Some(unexpected) = path_parameters
        .keys()
        .find(|name| !placeholder_names.contains(**name))
    {
        return Err(OpenApiRouteRegistryError::UnexpectedPathParameter {
            name: (*unexpected).to_owned(),
            method: candidate.method.clone(),
            path: candidate.path.clone(),
            handler: candidate.handler_name.clone(),
        });
    }
    Ok(())
}

fn path_error(candidate: &OpenApiRouteCandidate) -> OpenApiRouteRegistryError {
    OpenApiRouteRegistryError::InvalidPathTemplate {
        method: candidate.method.clone(),
        path: candidate.path.clone(),
        handler: candidate.handler_name.clone(),
    }
}

fn parameter_location_label(location: &ParameterIn) -> &'static str {
    match location {
        ParameterIn::Path => "path",
        ParameterIn::Query => "query",
        ParameterIn::Header => "header",
        ParameterIn::Cookie => "cookie",
    }
}

fn validate_request_body(
    candidate: &OpenApiRouteCandidate,
    operation: &Operation,
) -> Result<(), OpenApiRouteRegistryError> {
    let Some(body) = operation.request_body.as_ref() else {
        return Ok(());
    };
    if body.content.is_empty() {
        return Err(OpenApiRouteRegistryError::EmptyRequestBody {
            method: candidate.method.clone(),
            path: candidate.path.clone(),
            handler: candidate.handler_name.clone(),
        });
    }
    for (content_type, content) in &body.content {
        if content_type.trim().is_empty() || content.schema.is_none() {
            return Err(OpenApiRouteRegistryError::InvalidRequestBodyContent {
                content_type: content_type.clone(),
                method: candidate.method.clone(),
                path: candidate.path.clone(),
                handler: candidate.handler_name.clone(),
            });
        }
    }
    Ok(())
}

fn validate_response_shape(
    candidate: &OpenApiRouteCandidate,
    operation: &Operation,
) -> Result<(), OpenApiRouteRegistryError> {
    if operation.responses.responses.is_empty() {
        return Err(OpenApiRouteRegistryError::MissingResponses {
            method: candidate.method.clone(),
            path: candidate.path.clone(),
            handler: candidate.handler_name.clone(),
        });
    }

    for status in operation.responses.responses.keys() {
        if !valid_response_status(status) {
            return Err(OpenApiRouteRegistryError::InvalidResponseStatus {
                status: status.clone(),
                method: candidate.method.clone(),
                path: candidate.path.clone(),
                handler: candidate.handler_name.clone(),
            });
        }
    }
    Ok(())
}

fn validate_final_response_contracts(
    routes: &[RegisteredOpenApiRoute],
    components: &Components,
) -> Result<(), OpenApiRouteRegistryError> {
    for route in routes {
        let Some(operation) = route.operation.as_ref() else {
            continue;
        };
        validate_response_descriptions(route, operation, components)?;
    }
    Ok(())
}

fn validate_response_descriptions(
    route: &RegisteredOpenApiRoute,
    operation: &Operation,
    components: &Components,
) -> Result<(), OpenApiRouteRegistryError> {
    for (status, response) in &operation.responses.responses {
        let mut visited = HashSet::new();
        match response_description(response, components, &mut visited) {
            Ok(description) if !description.trim().is_empty() => {}
            Ok(_) => {
                return Err(OpenApiRouteRegistryError::MissingResponseDescription {
                    status: status.clone(),
                    method: route.method.clone(),
                    path: route.route_path.clone(),
                    handler: route.handler_name.clone(),
                });
            }
            Err(reference) => {
                return Err(OpenApiRouteRegistryError::InvalidResponseReference {
                    status: status.clone(),
                    reference,
                    method: route.method.clone(),
                    path: route.route_path.clone(),
                    handler: route.handler_name.clone(),
                });
            }
        }
    }
    Ok(())
}

fn valid_response_status(status: &str) -> bool {
    status == "default"
        || (status.len() == 3
            && status.bytes().all(|byte| byte.is_ascii_digit())
            && status
                .parse::<u16>()
                .is_ok_and(|status| (100..=599).contains(&status)))
}

fn response_description<'a>(
    response: &'a utoipa::openapi::RefOr<utoipa::openapi::response::Response>,
    components: &'a Components,
    visited: &mut HashSet<String>,
) -> Result<&'a str, String> {
    match response {
        utoipa::openapi::RefOr::T(response) => Ok(response.description.as_str()),
        utoipa::openapi::RefOr::Ref(reference) => {
            let name = match direct_component_reference_name(
                &reference.ref_location,
                LOCAL_RESPONSE_REFERENCE_PREFIX,
            ) {
                Ok(Some(name)) => name,
                Ok(None) => {
                    if has_direct_component_shape(
                        &reference.ref_location,
                        LOCAL_SCHEMA_REFERENCE_PREFIX,
                    ) || reference.description.trim().is_empty()
                    {
                        return Err(reference.ref_location.clone());
                    }
                    return Ok(reference.description.as_str());
                }
                Err(()) => return Err(reference.ref_location.clone()),
            };
            if !visited.insert(name.clone()) {
                return Err(reference.ref_location.clone());
            }
            let Some(response) = components.responses.get(&name) else {
                return Err(reference.ref_location.clone());
            };
            let target_description = response_description(response, components, visited)?;
            if reference.description.trim().is_empty() {
                Ok(target_description)
            } else {
                Ok(reference.description.as_str())
            }
        }
    }
}

const LOCAL_SCHEMA_REFERENCE_PREFIX: &str = "#/components/schemas/";
const LOCAL_RESPONSE_REFERENCE_PREFIX: &str = "#/components/responses/";

/// Return the component name for the direct-reference shape Lily emits.
///
/// Other local JSON Pointers remain valid OpenAPI input but are outside this
/// bounded component resolver. Empty direct references are always malformed.
fn direct_component_reference_name(reference: &str, prefix: &str) -> Result<Option<String>, ()> {
    let Some(name) = reference.strip_prefix(prefix) else {
        return Ok(None);
    };
    if name.is_empty() {
        return Err(());
    }
    let name = percent_decode_str(name).decode_utf8().map_err(|_| ())?;
    if name.contains('/') {
        return Ok(None);
    }

    let mut decoded = String::with_capacity(name.len());
    let mut characters = name.chars();
    while let Some(character) = characters.next() {
        if character != '~' {
            decoded.push(character);
            continue;
        }
        decoded.push(match characters.next() {
            Some('0') => '~',
            Some('1') => '/',
            _ => return Err(()),
        });
    }
    Ok(Some(decoded))
}

fn has_direct_component_shape(reference: &str, prefix: &str) -> bool {
    !matches!(direct_component_reference_name(reference, prefix), Ok(None))
}

fn has_wrong_direct_component_kind(reference: &str, kind: OpenApiComponentKind) -> bool {
    match kind {
        OpenApiComponentKind::Schema => {
            has_direct_component_shape(reference, LOCAL_RESPONSE_REFERENCE_PREFIX)
        }
        OpenApiComponentKind::Response => {
            has_direct_component_shape(reference, LOCAL_SCHEMA_REFERENCE_PREFIX)
        }
        OpenApiComponentKind::SecurityScheme => false,
    }
}

/// Validate typed reference sites from the final merged paths and components.
///
/// Every schema component is visited as a root. References only verify that
/// their target exists, so legal self-recursive and mutually recursive schema
/// graphs terminate without being classified as response-style resolution
/// cycles. Examples and extensions are deliberately not inspected as schema.
fn validate_local_reference_integrity(
    paths: &Paths,
    components: &Components,
) -> Result<(), OpenApiRouteRegistryError> {
    for schema in components.schemas.values() {
        validate_schema_reference(schema, components)?;
    }
    let mut visiting_responses = HashSet::new();
    let mut resolved_responses = HashSet::new();
    for name in components.responses.keys() {
        validate_response_component(
            name,
            components,
            &mut visiting_responses,
            &mut resolved_responses,
        )?;
    }

    for path_item in paths.paths.values() {
        for parameter in path_item.parameters.iter().flatten() {
            if let Some(schema) = parameter.schema.as_ref() {
                validate_schema_reference(schema, components)?;
            }
        }
        for operation in [
            path_item.get.as_ref(),
            path_item.put.as_ref(),
            path_item.post.as_ref(),
            path_item.delete.as_ref(),
            path_item.options.as_ref(),
            path_item.head.as_ref(),
            path_item.patch.as_ref(),
            path_item.trace.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            validate_operation_references(operation, components)?;
        }
    }
    Ok(())
}

fn validate_operation_references(
    operation: &Operation,
    components: &Components,
) -> Result<(), OpenApiRouteRegistryError> {
    for parameter in operation.parameters.iter().flatten() {
        if let Some(schema) = parameter.schema.as_ref() {
            validate_schema_reference(schema, components)?;
        }
    }
    if let Some(request_body) = operation.request_body.as_ref() {
        for content in request_body.content.values() {
            validate_content_references(content, components)?;
        }
    }
    for response in operation.responses.responses.values() {
        validate_response_reference(response, components)?;
    }
    Ok(())
}

fn validate_response_reference(
    response: &RefOr<utoipa::openapi::response::Response>,
    components: &Components,
) -> Result<(), OpenApiRouteRegistryError> {
    match response {
        RefOr::Ref(reference) => validate_local_component_reference(
            &reference.ref_location,
            LOCAL_RESPONSE_REFERENCE_PREFIX,
            OpenApiComponentKind::Response,
            &components.responses,
        ),
        RefOr::T(response) => validate_inline_response_references(response, components),
    }
}

fn validate_response_component(
    name: &str,
    components: &Components,
    visiting: &mut HashSet<String>,
    resolved: &mut HashSet<String>,
) -> Result<(), OpenApiRouteRegistryError> {
    if resolved.contains(name) {
        return Ok(());
    }
    if !visiting.insert(name.to_owned()) {
        return Err(OpenApiRouteRegistryError::InvalidLocalReference {
            kind: OpenApiComponentKind::Response,
            reference: format!("{LOCAL_RESPONSE_REFERENCE_PREFIX}{name}"),
        });
    }

    let response = components
        .responses
        .get(name)
        .expect("response component name originated from the same map");
    match response {
        RefOr::Ref(reference) => {
            validate_local_component_reference(
                &reference.ref_location,
                LOCAL_RESPONSE_REFERENCE_PREFIX,
                OpenApiComponentKind::Response,
                &components.responses,
            )?;
            if let Ok(Some(target)) = direct_component_reference_name(
                &reference.ref_location,
                LOCAL_RESPONSE_REFERENCE_PREFIX,
            ) {
                validate_response_component(&target, components, visiting, resolved)?;
            }
        }
        RefOr::T(response) => validate_inline_response_references(response, components)?,
    }

    visiting.remove(name);
    resolved.insert(name.to_owned());
    Ok(())
}

fn validate_inline_response_references(
    response: &utoipa::openapi::response::Response,
    components: &Components,
) -> Result<(), OpenApiRouteRegistryError> {
    for header in response.headers.values() {
        validate_schema_reference(&header.schema, components)?;
    }
    for content in response.content.values() {
        validate_content_references(content, components)?;
    }
    Ok(())
}

fn validate_content_references(
    content: &utoipa::openapi::Content,
    components: &Components,
) -> Result<(), OpenApiRouteRegistryError> {
    if let Some(schema) = content.schema.as_ref() {
        validate_schema_reference(schema, components)?;
    }
    for encoding in content.encoding.values() {
        for header in encoding.headers.values() {
            validate_schema_reference(&header.schema, components)?;
        }
    }
    Ok(())
}

fn validate_schema_reference(
    schema: &RefOr<Schema>,
    components: &Components,
) -> Result<(), OpenApiRouteRegistryError> {
    match schema {
        RefOr::Ref(reference) => validate_local_component_reference(
            &reference.ref_location,
            LOCAL_SCHEMA_REFERENCE_PREFIX,
            OpenApiComponentKind::Schema,
            &components.schemas,
        ),
        RefOr::T(schema) => validate_inline_schema(schema, components),
    }
}

fn validate_inline_schema(
    schema: &Schema,
    components: &Components,
) -> Result<(), OpenApiRouteRegistryError> {
    match schema {
        Schema::Array(array) => {
            if let ArrayItems::RefOrSchema(items) = &array.items {
                validate_schema_reference(items, components)?;
            }
            for schema in &array.prefix_items {
                validate_inline_schema(schema, components)?;
            }
        }
        Schema::Object(object) => {
            for schema in object.properties.values() {
                validate_schema_reference(schema, components)?;
            }
            if let Some(AdditionalProperties::RefOr(schema)) =
                object.additional_properties.as_deref()
            {
                validate_schema_reference(schema, components)?;
            }
            if let Some(property_names) = object.property_names.as_deref() {
                validate_inline_schema(property_names, components)?;
            }
        }
        Schema::OneOf(one_of) => {
            validate_schema_items(&one_of.items, components)?;
        }
        Schema::AllOf(all_of) => {
            validate_schema_items(&all_of.items, components)?;
        }
        Schema::AnyOf(any_of) => {
            validate_schema_items(&any_of.items, components)?;
        }
        _ => {}
    }
    Ok(())
}

fn validate_schema_items(
    items: &[RefOr<Schema>],
    components: &Components,
) -> Result<(), OpenApiRouteRegistryError> {
    for schema in items {
        validate_schema_reference(schema, components)?;
    }
    Ok(())
}

fn validate_local_component_reference<T>(
    reference: &str,
    prefix: &str,
    kind: OpenApiComponentKind,
    components: &BTreeMap<String, T>,
) -> Result<(), OpenApiRouteRegistryError> {
    match direct_component_reference_name(reference, prefix) {
        Ok(Some(name)) if !components.contains_key(&name) => {
            Err(OpenApiRouteRegistryError::InvalidLocalReference {
                kind,
                reference: reference.to_owned(),
            })
        }
        Ok(None) if has_wrong_direct_component_kind(reference, kind) => {
            Err(OpenApiRouteRegistryError::InvalidLocalReference {
                kind,
                reference: reference.to_owned(),
            })
        }
        Err(()) => Err(OpenApiRouteRegistryError::InvalidLocalReference {
            kind,
            reference: reference.to_owned(),
        }),
        Ok(Some(_) | None) => Ok(()),
    }
}

#[derive(Default)]
struct ComponentOwners {
    schemas: BTreeMap<String, String>,
    responses: BTreeMap<String, String>,
    security_schemes: BTreeMap<String, String>,
}

fn merge_components(
    target: &mut Components,
    source: &Components,
    candidate: &OpenApiRouteCandidate,
    owners: &mut ComponentOwners,
) -> Result<(), OpenApiRouteRegistryError> {
    merge_component_map(
        &mut target.schemas,
        &source.schemas,
        &mut owners.schemas,
        OpenApiComponentKind::Schema,
        candidate,
    )?;
    merge_component_map(
        &mut target.responses,
        &source.responses,
        &mut owners.responses,
        OpenApiComponentKind::Response,
        candidate,
    )?;
    merge_component_map(
        &mut target.security_schemes,
        &source.security_schemes,
        &mut owners.security_schemes,
        OpenApiComponentKind::SecurityScheme,
        candidate,
    )?;
    Ok(())
}

fn merge_component_map<T: Clone + PartialEq>(
    target: &mut BTreeMap<String, T>,
    source: &BTreeMap<String, T>,
    owners: &mut BTreeMap<String, String>,
    kind: OpenApiComponentKind,
    candidate: &OpenApiRouteCandidate,
) -> Result<(), OpenApiRouteRegistryError> {
    for (name, value) in source {
        if let Some(existing) = target.get(name) {
            if existing != value {
                return Err(OpenApiRouteRegistryError::ComponentCollision {
                    kind,
                    name: name.clone(),
                    first_handler: owners
                        .get(name)
                        .cloned()
                        .unwrap_or_else(|| "<unknown>".to_owned()),
                    duplicate_handler: candidate.handler_name.clone(),
                });
            }
            continue;
        }
        target.insert(name.clone(), value.clone());
        owners.insert(name.clone(), candidate.handler_name.clone());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::handler::Handler;
    use crate::private::{CorsRoutePolicyRegistration, OpenApiOperationMetadata};

    fn route(method: &str, path: &str, handler_name: &str) -> RouteInfo {
        RouteInfo {
            method: method.to_owned(),
            path: path.to_owned(),
            handler: Handler::new(Arc::new(|_, _, _| Box::pin(async { Ok(()) })), false),
            handler_name: handler_name.to_owned(),
            guard_type_ids: Vec::new(),
            middleware_registrations: Vec::new(),
            cors_policy_registration: CorsRoutePolicyRegistration::inherit(),
            route_plan_id: 0,
        }
    }

    fn panicking_factory() -> OpenApiOperationMetadata {
        panic!("fixture panic must be contained")
    }

    fn valid_responses() -> utoipa::openapi::response::Responses {
        utoipa::openapi::response::ResponsesBuilder::new()
            .response("200", utoipa::openapi::response::Response::new("Success"))
            .build()
    }

    fn missing_path_parameter_factory() -> OpenApiOperationMetadata {
        OpenApiOperationMetadata::new(
            utoipa::openapi::path::OperationBuilder::new()
                .responses(valid_responses())
                .build(),
            Components::new(),
        )
    }

    fn empty_request_body_factory() -> OpenApiOperationMetadata {
        OpenApiOperationMetadata::new(
            utoipa::openapi::path::OperationBuilder::new()
                .request_body(Some(utoipa::openapi::request_body::RequestBody::new()))
                .responses(valid_responses())
                .build(),
            Components::new(),
        )
    }

    fn missing_responses_factory() -> OpenApiOperationMetadata {
        OpenApiOperationMetadata::new(utoipa::openapi::path::Operation::new(), Components::new())
    }

    fn empty_response_description_factory() -> OpenApiOperationMetadata {
        let responses = utoipa::openapi::response::ResponsesBuilder::new()
            .response("200", utoipa::openapi::response::Response::new(" "))
            .build();
        OpenApiOperationMetadata::new(
            utoipa::openapi::path::OperationBuilder::new()
                .responses(responses)
                .build(),
            Components::new(),
        )
    }

    fn described_dangling_response_factory() -> OpenApiOperationMetadata {
        let mut reference = utoipa::openapi::Ref::from_response_name("MissingResponse");
        reference.description = "Documented override".to_owned();
        let responses = utoipa::openapi::response::ResponsesBuilder::new()
            .response("200", reference)
            .build();
        OpenApiOperationMetadata::new(
            utoipa::openapi::path::OperationBuilder::new()
                .responses(responses)
                .build(),
            Components::new(),
        )
    }

    fn described_response_with_empty_target_factory() -> OpenApiOperationMetadata {
        let mut reference = utoipa::openapi::Ref::from_response_name("SharedResponse");
        reference.description = "Documented override".to_owned();
        let responses = utoipa::openapi::response::ResponsesBuilder::new()
            .response("200", reference)
            .build();
        let mut components = Components::new();
        components.responses.insert(
            "SharedResponse".to_owned(),
            RefOr::T(utoipa::openapi::response::Response::new(" ")),
        );
        OpenApiOperationMetadata::new(
            utoipa::openapi::path::OperationBuilder::new()
                .responses(responses)
                .build(),
            components,
        )
    }

    fn forward_response_reference_factory() -> OpenApiOperationMetadata {
        let responses = utoipa::openapi::response::ResponsesBuilder::new()
            .response(
                "200",
                utoipa::openapi::Ref::from_response_name("ForwardResponse"),
            )
            .build();
        OpenApiOperationMetadata::new(
            utoipa::openapi::path::OperationBuilder::new()
                .responses(responses)
                .build(),
            Components::new(),
        )
    }

    fn forward_response_provider_factory() -> OpenApiOperationMetadata {
        let mut components = Components::new();
        components.responses.insert(
            "ForwardResponse".to_owned(),
            RefOr::T(utoipa::openapi::response::Response::new("Forward response")),
        );
        OpenApiOperationMetadata::new(
            utoipa::openapi::path::OperationBuilder::new()
                .responses(valid_responses())
                .build(),
            components,
        )
    }

    fn dangling_schema_reference_factory() -> OpenApiOperationMetadata {
        let mut components = Components::new();
        components.schemas.insert(
            "BrokenSchema".to_owned(),
            RefOr::Ref(utoipa::openapi::Ref::from_schema_name("MissingSchema")),
        );
        OpenApiOperationMetadata::new(
            utoipa::openapi::path::OperationBuilder::new()
                .responses(valid_responses())
                .build(),
            components,
        )
    }

    fn schema_reference_to_response_component_factory() -> OpenApiOperationMetadata {
        let mut components = Components::new();
        components.schemas.insert(
            "WrongSchema".to_owned(),
            RefOr::Ref(utoipa::openapi::Ref::from_response_name("ExistingResponse")),
        );
        components.responses.insert(
            "ExistingResponse".to_owned(),
            RefOr::T(utoipa::openapi::response::Response::new(
                "Existing response",
            )),
        );
        OpenApiOperationMetadata::new(
            utoipa::openapi::path::OperationBuilder::new()
                .responses(valid_responses())
                .build(),
            components,
        )
    }

    fn response_reference_to_schema_component_factory() -> OpenApiOperationMetadata {
        let mut reference = utoipa::openapi::Ref::from_schema_name("ExistingSchema");
        reference.description = "Wrong-kind response".to_owned();
        let responses = utoipa::openapi::response::ResponsesBuilder::new()
            .response("200", reference)
            .build();
        let mut components = Components::new();
        components.schemas.insert(
            "ExistingSchema".to_owned(),
            RefOr::T(Schema::Object(utoipa::openapi::Object::new())),
        );
        OpenApiOperationMetadata::new(
            utoipa::openapi::path::OperationBuilder::new()
                .responses(responses)
                .build(),
            components,
        )
    }

    fn unused_dangling_response_factory() -> OpenApiOperationMetadata {
        let mut components = Components::new();
        components.responses.insert(
            "UnusedResponse".to_owned(),
            RefOr::Ref(utoipa::openapi::Ref::from_response_name("MissingResponse")),
        );
        OpenApiOperationMetadata::new(
            utoipa::openapi::path::OperationBuilder::new()
                .responses(valid_responses())
                .build(),
            components,
        )
    }

    fn unused_response_cycle_factory() -> OpenApiOperationMetadata {
        let mut components = Components::new();
        components.responses.insert(
            "ResponseA".to_owned(),
            RefOr::Ref(utoipa::openapi::Ref::from_response_name("ResponseB")),
        );
        components.responses.insert(
            "ResponseB".to_owned(),
            RefOr::Ref(utoipa::openapi::Ref::from_response_name("ResponseA")),
        );
        OpenApiOperationMetadata::new(
            utoipa::openapi::path::OperationBuilder::new()
                .responses(valid_responses())
                .build(),
            components,
        )
    }

    fn recursive_schema_factory() -> OpenApiOperationMetadata {
        let recursive = utoipa::openapi::ObjectBuilder::new()
            .schema_type(utoipa::openapi::Type::Object)
            .property(
                "child",
                utoipa::openapi::Ref::from_schema_name("RecursiveSchema"),
            )
            .build();
        let mut components = Components::new();
        components.schemas.insert(
            "RecursiveSchema".to_owned(),
            RefOr::T(Schema::Object(recursive)),
        );
        components.schemas.insert(
            "NestedSchema".to_owned(),
            RefOr::T(Schema::Object(
                utoipa::openapi::ObjectBuilder::new()
                    .property("id", utoipa::openapi::Object::new())
                    .build(),
            )),
        );
        components.schemas.insert(
            "NestedProperty".to_owned(),
            RefOr::Ref(utoipa::openapi::Ref::new(
                "#/components/schemas/NestedSchema/properties/id",
            )),
        );
        components.responses.insert(
            "PathResponse".to_owned(),
            RefOr::Ref(utoipa::openapi::Ref::new(
                "#/paths/~1recursive-schema/get/responses/200",
            )),
        );
        OpenApiOperationMetadata::new(
            utoipa::openapi::path::OperationBuilder::new()
                .responses(valid_responses())
                .build(),
            components,
        )
    }

    #[test]
    fn route_normalization_cannot_desynchronize_openapi_metadata() {
        let materialized =
            ControllerRouteMaterialization::new(vec![MaterializedControllerRoute::new(
                route("get", "/metadata-mismatch", "fixture::mismatch"),
                OpenApiRouteRegistration::Unspecified,
            )]);

        assert!(matches!(
            materialized.into_openapi_route_table(),
            Err(OpenApiRouteBuildError::Registry(error))
                if matches!(error.as_ref(), OpenApiRouteRegistryError::MetadataMismatch { .. })
        ));
    }

    #[test]
    fn factory_panic_is_a_typed_registry_failure() {
        let materialized =
            ControllerRouteMaterialization::new(vec![MaterializedControllerRoute::new(
                route("GET", "/factory-panic", "fixture::panic"),
                OpenApiRouteRegistration::Documented(panicking_factory),
            )]);

        assert!(matches!(
            materialized.into_openapi_route_table(),
            Err(OpenApiRouteBuildError::Registry(error))
                if matches!(error.as_ref(), OpenApiRouteRegistryError::OperationFactoryPanicked { .. })
        ));
    }

    #[test]
    fn documented_method_must_be_representable_by_openapi() {
        let materialized =
            ControllerRouteMaterialization::new(vec![MaterializedControllerRoute::new(
                route("CONNECT", "/tunnel", "fixture::connect"),
                OpenApiRouteRegistration::Documented(panicking_factory),
            )]);

        assert!(matches!(
            materialized.into_openapi_route_table(),
            Err(OpenApiRouteBuildError::Registry(error))
                if matches!(error.as_ref(), OpenApiRouteRegistryError::UnsupportedMethod { .. })
        ));
    }

    #[test]
    fn operation_path_parameters_must_match_the_route_exactly() {
        let materialized =
            ControllerRouteMaterialization::new(vec![MaterializedControllerRoute::new(
                route("GET", "/users/:id", "fixture::missing_path"),
                OpenApiRouteRegistration::Documented(missing_path_parameter_factory),
            )]);

        assert!(matches!(
            materialized.into_openapi_route_table(),
            Err(OpenApiRouteBuildError::Registry(error))
                if matches!(error.as_ref(), OpenApiRouteRegistryError::MissingPathParameter { name, .. } if name == "id")
        ));
    }

    #[test]
    fn request_body_must_contain_at_least_one_media_type_and_schema() {
        let materialized =
            ControllerRouteMaterialization::new(vec![MaterializedControllerRoute::new(
                route("POST", "/payload", "fixture::empty_body"),
                OpenApiRouteRegistration::Documented(empty_request_body_factory),
            )]);

        assert!(matches!(
            materialized.into_openapi_route_table(),
            Err(OpenApiRouteBuildError::Registry(error))
                if matches!(error.as_ref(), OpenApiRouteRegistryError::EmptyRequestBody { .. })
        ));
    }

    #[test]
    fn operation_requires_a_described_response() {
        let missing = ControllerRouteMaterialization::new(vec![MaterializedControllerRoute::new(
            route("GET", "/missing-response", "fixture::missing_response"),
            OpenApiRouteRegistration::Documented(missing_responses_factory),
        )]);
        assert!(matches!(
            missing.into_openapi_route_table(),
            Err(OpenApiRouteBuildError::Registry(error))
                if matches!(error.as_ref(), OpenApiRouteRegistryError::MissingResponses { .. })
        ));

        let empty = ControllerRouteMaterialization::new(vec![MaterializedControllerRoute::new(
            route("GET", "/empty-description", "fixture::empty_description"),
            OpenApiRouteRegistration::Documented(empty_response_description_factory),
        )]);
        assert!(matches!(
            empty.into_openapi_route_table(),
            Err(OpenApiRouteBuildError::Registry(error))
                if matches!(error.as_ref(), OpenApiRouteRegistryError::MissingResponseDescription { status, .. } if status == "200")
        ));
    }

    #[test]
    fn response_description_does_not_bypass_local_reference_resolution() {
        let materialized =
            ControllerRouteMaterialization::new(vec![MaterializedControllerRoute::new(
                route("GET", "/dangling-response", "fixture::dangling_response"),
                OpenApiRouteRegistration::Documented(described_dangling_response_factory),
            )]);

        assert!(matches!(
            materialized.into_openapi_route_table(),
            Err(OpenApiRouteBuildError::Registry(error))
                if matches!(
                    error.as_ref(),
                    OpenApiRouteRegistryError::InvalidResponseReference { reference, .. }
                        if reference == "#/components/responses/MissingResponse"
                )
        ));
    }

    #[test]
    fn response_description_override_is_applied_after_target_resolution() {
        let materialized =
            ControllerRouteMaterialization::new(vec![MaterializedControllerRoute::new(
                route("GET", "/response-override", "fixture::response_override"),
                OpenApiRouteRegistration::Documented(described_response_with_empty_target_factory),
            )]);

        assert!(materialized.into_openapi_route_table().is_ok());
    }

    #[test]
    fn response_references_resolve_against_the_final_merged_components() {
        let materialized = ControllerRouteMaterialization::new(vec![
            MaterializedControllerRoute::new(
                route("GET", "/a-consumer", "fixture::response_consumer"),
                OpenApiRouteRegistration::Documented(forward_response_reference_factory),
            ),
            MaterializedControllerRoute::new(
                route("GET", "/z-provider", "fixture::response_provider"),
                OpenApiRouteRegistration::Documented(forward_response_provider_factory),
            ),
        ]);

        assert!(materialized.into_openapi_route_table().is_ok());
    }

    #[test]
    fn final_document_rejects_dangling_schema_references() {
        let materialized =
            ControllerRouteMaterialization::new(vec![MaterializedControllerRoute::new(
                route("GET", "/dangling-schema", "fixture::dangling_schema"),
                OpenApiRouteRegistration::Documented(dangling_schema_reference_factory),
            )]);

        assert!(matches!(
            materialized.into_openapi_route_table(),
            Err(OpenApiRouteBuildError::Registry(error))
                if matches!(
                    error.as_ref(),
                    OpenApiRouteRegistryError::InvalidLocalReference {
                        kind: OpenApiComponentKind::Schema,
                        reference,
                    } if reference == "#/components/schemas/MissingSchema"
                )
        ));
    }

    #[test]
    fn final_document_rejects_wrong_component_kinds() {
        let schema_to_response =
            ControllerRouteMaterialization::new(vec![MaterializedControllerRoute::new(
                route("GET", "/wrong-schema-kind", "fixture::wrong_schema_kind"),
                OpenApiRouteRegistration::Documented(
                    schema_reference_to_response_component_factory,
                ),
            )]);
        assert!(matches!(
            schema_to_response.into_openapi_route_table(),
            Err(OpenApiRouteBuildError::Registry(error))
                if matches!(
                    error.as_ref(),
                    OpenApiRouteRegistryError::InvalidLocalReference {
                        kind: OpenApiComponentKind::Schema,
                        reference,
                    } if reference == "#/components/responses/ExistingResponse"
                )
        ));

        let response_to_schema =
            ControllerRouteMaterialization::new(vec![MaterializedControllerRoute::new(
                route(
                    "GET",
                    "/wrong-response-kind",
                    "fixture::wrong_response_kind",
                ),
                OpenApiRouteRegistration::Documented(
                    response_reference_to_schema_component_factory,
                ),
            )]);
        assert!(matches!(
            response_to_schema.into_openapi_route_table(),
            Err(OpenApiRouteBuildError::Registry(error))
                if matches!(
                    error.as_ref(),
                    OpenApiRouteRegistryError::InvalidResponseReference { reference, .. }
                        if reference == "#/components/schemas/ExistingSchema"
                )
        ));
    }

    #[test]
    fn final_document_accepts_recursive_schema_references() {
        let materialized =
            ControllerRouteMaterialization::new(vec![MaterializedControllerRoute::new(
                route("GET", "/recursive-schema", "fixture::recursive_schema"),
                OpenApiRouteRegistration::Documented(recursive_schema_factory),
            )]);

        assert!(materialized.into_openapi_route_table().is_ok());
    }

    #[test]
    fn final_document_validates_unused_response_component_graphs() {
        let dangling = ControllerRouteMaterialization::new(vec![MaterializedControllerRoute::new(
            route("GET", "/unused-dangling", "fixture::unused_dangling"),
            OpenApiRouteRegistration::Documented(unused_dangling_response_factory),
        )]);
        assert!(matches!(
            dangling.into_openapi_route_table(),
            Err(OpenApiRouteBuildError::Registry(error))
                if matches!(
                    error.as_ref(),
                    OpenApiRouteRegistryError::InvalidLocalReference {
                        kind: OpenApiComponentKind::Response,
                        reference,
                    } if reference == "#/components/responses/MissingResponse"
                )
        ));

        let cycle = ControllerRouteMaterialization::new(vec![MaterializedControllerRoute::new(
            route("GET", "/unused-cycle", "fixture::unused_cycle"),
            OpenApiRouteRegistration::Documented(unused_response_cycle_factory),
        )]);
        assert!(matches!(
            cycle.into_openapi_route_table(),
            Err(OpenApiRouteBuildError::Registry(error))
                if matches!(
                    error.as_ref(),
                    OpenApiRouteRegistryError::InvalidLocalReference {
                        kind: OpenApiComponentKind::Response,
                        ..
                    }
                )
        ));
    }
}
