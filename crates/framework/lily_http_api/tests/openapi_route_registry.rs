use std::any::TypeId;
use std::sync::Arc;

use lily_http_api::__private::{
    get_pending_controller_routes, get_struct_controller_registrations,
    materialize_controller_routes, ControllerRouteMaterialization, OpenApiRouteBuildError,
    OpenApiRouteMetadataStatus, OpenApiRouteRegistryError,
};
use lily_http_api::async_trait::async_trait;
use lily_http_api::utoipa::{self, IntoParams, ToSchema};
use lily_http_api::AppBuildError;
use lily_http_api::{
    controller, ApplicationContainer, Controller, ControllerInitError, ControllerTrait, Extensions,
    HttpApiError, Path, Query,
};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, IntoParams)]
struct RegistryPath {
    id: String,
}

#[derive(Deserialize, IntoParams)]
struct RegistryQuery {
    page: Option<u32>,
}

#[derive(Serialize, ToSchema)]
struct RegistryView {
    id: String,
}

#[derive(Controller)]
#[base_path("/cap07c")]
#[openapi(tag = "Registry")]
struct RegistryController;

#[async_trait]
impl ControllerTrait for RegistryController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        Ok(Self)
    }
}

#[controller]
impl RegistryController {
    #[get("/zeta")]
    async fn zeta(&self) -> Result<RegistryView, HttpApiError> {
        Ok(RegistryView {
            id: "zeta".to_owned(),
        })
    }

    #[get("/alpha/:id")]
    async fn alpha(
        &self,
        Query(query): Query<RegistryQuery>,
        Path(path): Path<RegistryPath>,
    ) -> Result<RegistryView, HttpApiError> {
        let _ = query.page;
        Ok(RegistryView { id: path.id })
    }

    #[get("/internal")]
    #[openapi(skip)]
    async fn internal(&self) -> String {
        "internal".to_owned()
    }
}

#[derive(Controller)]
#[base_path("/cap07c-unspecified")]
struct UnspecifiedController;

#[async_trait]
impl ControllerTrait for UnspecifiedController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        Ok(Self)
    }
}

#[controller]
impl UnspecifiedController {
    #[get("/")]
    async fn action(&self) -> String {
        "unspecified".to_owned()
    }
}

#[derive(Controller)]
#[base_path("/cap07c-duplicate-id")]
#[openapi(tag = "Duplicate")]
struct DuplicateOperationIdController;

#[async_trait]
impl ControllerTrait for DuplicateOperationIdController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        Ok(Self)
    }
}

#[controller]
impl DuplicateOperationIdController {
    #[get("/one")]
    #[openapi(operation_id = "duplicate.operation")]
    async fn one(&self) -> Result<RegistryView, HttpApiError> {
        Ok(RegistryView {
            id: "one".to_owned(),
        })
    }

    #[get("/two")]
    #[openapi(operation_id = "duplicate.operation")]
    async fn two(&self) -> Result<RegistryView, HttpApiError> {
        Ok(RegistryView {
            id: "two".to_owned(),
        })
    }
}

#[derive(Controller)]
#[base_path("/cap07c-document-collision")]
#[openapi(tag = "Collision")]
struct DocumentPathCollisionController;

#[async_trait]
impl ControllerTrait for DocumentPathCollisionController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        Ok(Self)
    }
}

#[controller]
impl DocumentPathCollisionController {
    #[get("/:id")]
    async fn parameterized(
        &self,
        Path(path): Path<RegistryPath>,
    ) -> Result<RegistryView, HttpApiError> {
        Ok(RegistryView { id: path.id })
    }

    #[get("/{id}")]
    async fn literal(&self) -> Result<RegistryView, HttpApiError> {
        Ok(RegistryView {
            id: "literal".to_owned(),
        })
    }
}

async fn materialize(
    container: &ApplicationContainer,
    controller_types: &[TypeId],
    reversed: bool,
) -> ControllerRouteMaterialization {
    let mut registrations = get_struct_controller_registrations()
        .into_iter()
        .filter(|registration| controller_types.contains(&registration.type_id()))
        .collect::<Vec<_>>();
    let mut routes = get_pending_controller_routes()
        .into_iter()
        .filter(|route| controller_types.contains(&route.controller_type_id()))
        .collect::<Vec<_>>();
    if reversed {
        registrations.reverse();
        routes.reverse();
    }
    materialize_controller_routes(registrations, routes, container.services())
        .await
        .expect("selected routes materialize")
}

#[tokio::test]
async fn accepted_routes_produce_a_sorted_byte_stable_registry() {
    let container = ApplicationContainer::build()
        .await
        .expect("application container builds");
    let selected = [TypeId::of::<RegistryController>()];

    let first = materialize(&container, &selected, false)
        .await
        .into_openapi_route_table()
        .expect("first OpenAPI route registry builds")
        .1;
    let second = materialize(&container, &selected, true)
        .await
        .into_openapi_route_table()
        .expect("second OpenAPI route registry builds")
        .1;

    assert_eq!(
        first.canonical_json().unwrap(),
        second.canonical_json().unwrap()
    );
    let routes = first.routes().collect::<Vec<_>>();
    assert_eq!(routes.len(), 3);
    assert!(routes.windows(2).all(|pair| {
        (
            pair[0].document_path(),
            pair[0].method(),
            pair[0].handler_name(),
        ) <= (
            pair[1].document_path(),
            pair[1].method(),
            pair[1].handler_name(),
        )
    }));
    assert_eq!(
        routes
            .iter()
            .find(|route| route.route_path() == "/cap07c/internal")
            .expect("skipped route is retained")
            .status(),
        OpenApiRouteMetadataStatus::Skipped
    );
    assert!(first.paths().paths.contains_key("/cap07c/alpha/{id}"));

    let alpha = routes
        .iter()
        .find(|route| route.route_path() == "/cap07c/alpha/:id")
        .and_then(|route| route.operation())
        .expect("documented alpha operation exists");
    let parameters = alpha.parameters.as_ref().expect("parameters exist");
    assert_eq!(parameters[0].name, "id");
    assert_eq!(parameters[1].name, "page");

    container.close().await.expect("container closes");
}

#[tokio::test]
async fn unspecified_route_is_allowed_without_openapi_and_rejected_when_enabled() {
    let container = ApplicationContainer::build()
        .await
        .expect("application container builds");
    let selected = [TypeId::of::<UnspecifiedController>()];

    materialize(&container, &selected, false)
        .await
        .into_route_table()
        .expect("normal route build does not require OpenAPI metadata");

    let error = materialize(&container, &selected, false)
        .await
        .into_openapi_route_table()
        .expect_err("OpenAPI-enabled route build must fail closed");
    assert!(matches!(
        error,
        OpenApiRouteBuildError::Registry(error)
            if matches!(error.as_ref(), OpenApiRouteRegistryError::UnspecifiedRoute { path, .. } if path == "/cap07c-unspecified")
    ));

    container.close().await.expect("container closes");
}

#[tokio::test]
async fn duplicate_operation_id_is_a_typed_app_build_error() {
    let container = ApplicationContainer::build()
        .await
        .expect("application container builds");
    let error = materialize(
        &container,
        &[TypeId::of::<DuplicateOperationIdController>()],
        false,
    )
    .await
    .into_openapi_route_table()
    .expect_err("duplicate operation IDs must fail");
    let OpenApiRouteBuildError::Registry(error) = error else {
        panic!("unexpected error: {error}");
    };
    assert!(matches!(
        error.as_ref(),
        OpenApiRouteRegistryError::DuplicateOperationId(_)
    ));
    assert!(matches!(
        AppBuildError::from((*error).clone()),
        AppBuildError::OpenApiRouteRegistry(error)
            if matches!(error.as_ref(), OpenApiRouteRegistryError::DuplicateOperationId(_))
    ));
    container.close().await.expect("container closes");
}

#[tokio::test]
async fn literal_openapi_template_cannot_bypass_lily_placeholder_authority() {
    let container = ApplicationContainer::build()
        .await
        .expect("application container builds");
    let error = materialize(
        &container,
        &[TypeId::of::<DocumentPathCollisionController>()],
        false,
    )
    .await
    .into_openapi_route_table()
    .expect_err("literal OpenAPI template syntax must fail");
    assert!(matches!(
        error,
        OpenApiRouteBuildError::Registry(error)
            if matches!(error.as_ref(), OpenApiRouteRegistryError::InvalidPathTemplate { path, .. } if path == "/cap07c-document-collision/{id}")
    ));
    container.close().await.expect("container closes");
}
