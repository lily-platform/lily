use std::any::TypeId;
use std::sync::Arc;

use lily_http_api::__private::{
    get_pending_controller_routes, get_struct_controller_registrations,
    materialize_controller_routes, ControllerRouteMaterialization, OpenApiRouteBuildError,
    OpenApiRouteRegistry, OpenApiRouteRegistryError,
};
use lily_http_api::async_trait::async_trait;
use lily_http_api::utoipa::{self, IntoResponses, ToResponse, ToSchema};
use lily_http_api::AppBuildError;
use lily_http_api::{
    controller, ApplicationContainer, Controller, ControllerInitError, ControllerTrait, Extensions,
    FormFile, HttpApiError, Json, MultipartForm, NoContent, OpenApiApiKeyLocation, OpenApiConfig,
    OpenApiSecurityScheme, OpenApiSecurityValidationError,
};
use serde::{Deserialize, Serialize};

#[derive(MultipartForm)]
#[schema(as = uploads::CreateAsset)]
struct UploadForm {
    title: String,
    note: Option<String>,
    tags: Vec<String>,
    #[form_file]
    asset: FormFile,
    #[form_file]
    attachments: Vec<FormFile>,
}

#[derive(Serialize, ToSchema)]
struct RecursiveNode {
    value: String,
    #[schema(no_recursion)]
    child: Option<Box<RecursiveNode>>,
}

#[derive(Serialize, ToSchema, ToResponse)]
#[response(description = "Reusable problem response")]
struct ProblemResponse {
    code: String,
}

#[derive(Serialize, ToSchema, IntoResponses)]
#[allow(dead_code)]
enum CommonResponses {
    #[response(status = 400, description = "Bad request")]
    BadRequest { code: String },
    #[response(status = 404, description = "Not found")]
    NotFound,
}

#[derive(Controller)]
#[base_path("/cap07d")]
#[openapi(tag = "SchemaSecurity")]
struct SchemaSecurityController;

#[async_trait]
impl ControllerTrait for SchemaSecurityController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        Ok(Self)
    }
}

#[controller]
impl SchemaSecurityController {
    #[post("/upload")]
    async fn upload(
        &self,
        MultipartForm(input): MultipartForm<UploadForm>,
    ) -> Result<NoContent, HttpApiError> {
        let _ = (
            input.title,
            input.note,
            input.tags,
            input.asset,
            input.attachments,
        );
        Ok(NoContent)
    }

    #[get("/recursive")]
    async fn recursive(&self) -> Result<RecursiveNode, HttpApiError> {
        Ok(RecursiveNode {
            value: "root".to_owned(),
            child: None,
        })
    }

    #[get("/reusable")]
    #[openapi(responses((status = 400, response = ProblemResponse)))]
    async fn reusable(&self) -> Result<NoContent, HttpApiError> {
        Ok(NoContent)
    }

    #[get("/multiple")]
    #[openapi(responses(CommonResponses))]
    async fn multiple(&self) -> Result<NoContent, HttpApiError> {
        Ok(NoContent)
    }

    #[get("/bearer")]
    #[openapi(security(("bearer" = [])))]
    async fn bearer(&self) -> Result<NoContent, HttpApiError> {
        Ok(NoContent)
    }

    #[get("/oidc")]
    #[openapi(security(("oidc" = ["assets:read"])))]
    async fn oidc(&self) -> Result<NoContent, HttpApiError> {
        Ok(NoContent)
    }

    #[get("/api-key")]
    #[openapi(security(("api-key" = [])))]
    async fn api_key(&self) -> Result<NoContent, HttpApiError> {
        Ok(NoContent)
    }

    #[get("/cookie-key")]
    #[openapi(security(("session-cookie" = [])))]
    async fn cookie_key(&self) -> Result<NoContent, HttpApiError> {
        Ok(NoContent)
    }
}

#[derive(Deserialize, Serialize, ToSchema)]
#[schema(as = collision::Shared)]
struct FirstCollision {
    first: String,
}

#[derive(Serialize, ToSchema)]
#[schema(as = collision::Shared)]
struct SecondCollision {
    second: u64,
}

#[derive(Controller)]
#[base_path("/cap07d-collision")]
#[openapi]
struct CollisionController;

#[async_trait]
impl ControllerTrait for CollisionController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        Ok(Self)
    }
}

#[controller]
impl CollisionController {
    #[post("/")]
    async fn collision(
        &self,
        Json(_input): Json<FirstCollision>,
    ) -> Result<SecondCollision, HttpApiError> {
        Ok(SecondCollision { second: 1 })
    }
}

#[derive(ToSchema, IntoResponses)]
#[allow(dead_code)]
enum StatusCollisionResponses {
    #[response(status = 204, description = "Duplicate no-content response")]
    DuplicateNoContent,
}

#[derive(Controller)]
#[base_path("/cap07d-status-collision")]
#[openapi]
struct StatusCollisionController;

#[async_trait]
impl ControllerTrait for StatusCollisionController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        Ok(Self)
    }
}

#[controller]
impl StatusCollisionController {
    #[get("/")]
    #[openapi(responses(StatusCollisionResponses))]
    async fn collision(&self) -> Result<NoContent, HttpApiError> {
        Ok(NoContent)
    }
}

async fn materialize(
    container: &ApplicationContainer,
    controller_type: TypeId,
) -> ControllerRouteMaterialization {
    let registrations = get_struct_controller_registrations()
        .into_iter()
        .filter(|registration| registration.type_id() == controller_type)
        .collect();
    let routes = get_pending_controller_routes()
        .into_iter()
        .filter(|route| route.controller_type_id() == controller_type)
        .collect();
    materialize_controller_routes(registrations, routes, container.services())
        .await
        .expect("selected controller routes materialize")
}

async fn schema_security_registry(container: &ApplicationContainer) -> OpenApiRouteRegistry {
    materialize(container, TypeId::of::<SchemaSecurityController>())
        .await
        .into_openapi_route_table()
        .expect("CAP-07D registry builds")
        .1
}

fn security_config() -> OpenApiConfig {
    let mut config = OpenApiConfig::new("CAP-07D API", "1.0.0").unwrap();
    config
        .register_security_scheme(
            OpenApiSecurityScheme::bearer("bearer", Some("JWT"), None)
                .expect("bearer config is valid"),
        )
        .unwrap()
        .register_security_scheme(
            OpenApiSecurityScheme::open_id_connect(
                "oidc",
                "https://identity.example/.well-known/openid-configuration",
                None,
            )
            .expect("OIDC config is valid"),
        )
        .unwrap()
        .register_security_scheme(
            OpenApiSecurityScheme::api_key(
                "api-key",
                OpenApiApiKeyLocation::Header,
                "x-api-key",
                None,
            )
            .expect("header API key config is valid"),
        )
        .unwrap()
        .register_security_scheme(
            OpenApiSecurityScheme::api_key(
                "session-cookie",
                OpenApiApiKeyLocation::Cookie,
                "session",
                None,
            )
            .expect("cookie API key config is valid"),
        )
        .unwrap();
    config
}

#[tokio::test]
async fn multipart_recursive_reusable_and_multiple_components_are_complete() {
    let container = ApplicationContainer::build()
        .await
        .expect("application container builds");
    let registry = schema_security_registry(&container).await;
    let components = registry.components();

    let multipart = serde_json::to_value(
        components
            .schemas
            .get("uploads.CreateAsset")
            .expect("multipart schema is registered"),
    )
    .unwrap();
    assert_eq!(multipart["properties"]["asset"]["type"], "string");
    assert_eq!(multipart["properties"]["asset"]["format"], "binary");
    assert_eq!(
        multipart["properties"]["attachments"]["items"]["format"],
        "binary"
    );
    assert_eq!(multipart["required"], serde_json::json!(["title", "asset"]));

    assert!(components.schemas.contains_key("RecursiveNode"));
    assert!(components.responses.contains_key("ProblemResponse"));
    let reusable = registry
        .routes()
        .find(|route| route.route_path() == "/cap07d/reusable")
        .and_then(|route| route.operation())
        .expect("reusable operation exists");
    assert_eq!(
        serde_json::to_value(&reusable.responses.responses["400"]).unwrap()["$ref"],
        "#/components/responses/ProblemResponse"
    );
    let multiple = registry
        .routes()
        .find(|route| route.route_path() == "/cap07d/multiple")
        .and_then(|route| route.operation())
        .expect("multiple response operation exists");
    assert!(multiple.responses.responses.contains_key("204"));
    assert!(multiple.responses.responses.contains_key("400"));
    assert!(multiple.responses.responses.contains_key("404"));

    container.close().await.expect("container closes");
}

#[tokio::test]
async fn security_references_are_validated_before_components_are_attached() {
    let container = ApplicationContainer::build()
        .await
        .expect("application container builds");
    let mut registry = schema_security_registry(&container).await;
    registry
        .apply_security_config(&security_config())
        .expect("all references are registered and type-compatible");
    assert_eq!(registry.components().security_schemes.len(), 4);

    let mut unknown_registry = schema_security_registry(&container).await;
    let error = unknown_registry
        .apply_security_config(&OpenApiConfig::new("CAP-07D API", "1.0.0").unwrap())
        .expect_err("unknown scheme references fail closed");
    assert!(matches!(
        AppBuildError::from(error.clone()),
        AppBuildError::OpenApiSecurity(_)
    ));
    assert!(matches!(
        error,
        OpenApiSecurityValidationError::UnknownScheme { .. }
    ));

    let mut mismatch_registry = schema_security_registry(&container).await;
    let mut mismatch_config = OpenApiConfig::new("CAP-07D API", "1.0.0").unwrap();
    mismatch_config
        .register_security_scheme(OpenApiSecurityScheme::bearer("bearer", None, None).unwrap())
        .unwrap()
        .register_security_scheme(OpenApiSecurityScheme::bearer("oidc", None, None).unwrap())
        .unwrap()
        .register_security_scheme(
            OpenApiSecurityScheme::api_key(
                "api-key",
                OpenApiApiKeyLocation::Header,
                "x-api-key",
                None,
            )
            .unwrap(),
        )
        .unwrap()
        .register_security_scheme(
            OpenApiSecurityScheme::api_key(
                "session-cookie",
                OpenApiApiKeyLocation::Cookie,
                "session",
                None,
            )
            .unwrap(),
        )
        .unwrap();
    assert!(matches!(
        mismatch_registry.apply_security_config(&mismatch_config),
        Err(OpenApiSecurityValidationError::SchemeTypeMismatch { name, .. }) if name == "oidc"
    ));

    container.close().await.expect("container closes");
}

#[tokio::test]
async fn conflicting_explicit_schema_names_fail_deterministically() {
    let container = ApplicationContainer::build()
        .await
        .expect("application container builds");
    let error = materialize(&container, TypeId::of::<CollisionController>())
        .await
        .into_openapi_route_table()
        .expect_err("same schema name with different definitions must fail");
    assert!(matches!(
        error,
        OpenApiRouteBuildError::Registry(error)
            if matches!(error.as_ref(), OpenApiRouteRegistryError::OperationComponentCollision { name, .. } if name == "collision.Shared")
    ));
    container.close().await.expect("container closes");
}

#[tokio::test]
async fn reusable_response_status_cannot_overwrite_inferred_success() {
    let container = ApplicationContainer::build()
        .await
        .expect("application container builds");
    let error = materialize(&container, TypeId::of::<StatusCollisionController>())
        .await
        .into_openapi_route_table()
        .expect_err("duplicate response status must fail");
    assert!(matches!(
        error,
        OpenApiRouteBuildError::Registry(error)
            if matches!(error.as_ref(), OpenApiRouteRegistryError::ResponseStatusCollision { status, .. } if status == "204")
    ));
    container.close().await.expect("container closes");
}

#[test]
fn duplicate_security_scheme_names_are_rejected() {
    let mut config = OpenApiConfig::new("CAP-07D API", "1.0.0").unwrap();
    let first = OpenApiSecurityScheme::bearer("auth", None, None).unwrap();
    let duplicate =
        OpenApiSecurityScheme::api_key("auth", OpenApiApiKeyLocation::Header, "x-api-key", None)
            .unwrap();
    config.register_security_scheme(first).unwrap();
    assert!(config.register_security_scheme(duplicate).is_err());
}
