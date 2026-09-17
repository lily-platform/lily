use std::sync::Arc;

use lily_http_api::__private::{get_pending_controller_routes, OpenApiRouteMetadataStatus};
use lily_http_api::async_trait::async_trait;
use lily_http_api::utoipa::{self, IntoParams, ToSchema};
use lily_http_api::{
    controller, Controller, ControllerInitError, ControllerTrait, Extensions, HttpApiError, Json,
    Path, RawBody, RequestCookies, Response,
};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, IntoParams)]
struct ContractPath {
    id: String,
}

#[derive(Deserialize, ToSchema)]
struct ContractInput {
    name: String,
}

#[derive(Serialize, ToSchema)]
struct ContractView {
    id: String,
    name: String,
}

#[derive(ToSchema)]
#[allow(dead_code)]
struct ApiErrorBody {
    code: String,
}

#[derive(ToSchema)]
#[allow(dead_code)]
struct RawPayload {
    bytes: String,
}

struct InternalOnlyInput {
    value: String,
}

impl lily_http_api::FromRequestParts for InternalOnlyInput {
    type Rejection = HttpApiError;

    async fn from_request_parts(
        _request: &mut lily_http_api::Request,
        _extensions: &Extensions,
    ) -> Result<Self, Self::Rejection> {
        Ok(Self {
            value: "internal".to_owned(),
        })
    }
}

#[derive(Controller)]
#[base_path("/cap07b")]
#[openapi(
    tag = "Contracts",
    description = "Controller fallback description",
    responses((
        status = 401,
        description = "Authentication required",
        schema = ApiErrorBody
    )),
    security(("bearer" = []))
)]
struct OpenApiContractController;

#[derive(Controller)]
#[base_path("/cap07b-undocumented")]
struct UndocumentedController;

mod separated_controller {
    use super::*;

    #[derive(Controller)]
    #[base_path("/cap07b-separated")]
    #[openapi(tag = "Separated")]
    pub(crate) struct SeparatedController;
}

#[async_trait]
impl ControllerTrait for separated_controller::SeparatedController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        Ok(Self)
    }
}

#[controller]
impl separated_controller::SeparatedController {
    #[get("/")]
    async fn action(&self) -> String {
        "separated".to_owned()
    }
}

#[async_trait]
impl ControllerTrait for OpenApiContractController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        Ok(Self)
    }
}

#[async_trait]
impl ControllerTrait for UndocumentedController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        Ok(Self)
    }
}

#[controller]
impl UndocumentedController {
    #[get("/")]
    async fn action(&self) -> String {
        "undocumented".to_owned()
    }
}

#[controller]
impl OpenApiContractController {
    /// Fetch one contract.
    ///
    /// The complete action description comes from its doc comment.
    #[get("/:id")]
    async fn inherited(
        &self,
        Path(path): Path<ContractPath>,
    ) -> Result<ContractView, HttpApiError> {
        Ok(ContractView {
            id: path.id,
            name: "inherited".to_owned(),
        })
    }

    /// This doc summary is intentionally overridden.
    #[post("/:id")]
    #[openapi(
        operation_id = "contracts.update",
        summary = "Update one contract",
        description = "Explicit action description",
        deprecated,
        responses((
            status = 404,
            description = "Contract not found",
            schema = ApiErrorBody,
            example = "contract_not_found"
        )),
        security(("contract-admin" = ["contracts:write"]))
    )]
    async fn update(
        &self,
        Path(path): Path<ContractPath>,
        Json(input): Json<ContractInput>,
    ) -> Result<ContractView, HttpApiError> {
        Ok(ContractView {
            id: path.id,
            name: input.name,
        })
    }

    #[post("/raw")]
    #[openapi(
        request_body(
            content_type = "application/octet-stream",
            schema = RawPayload,
            description = "Opaque request payload",
            example = "AAEC"
        ),
        responses((status = 202, description = "Accepted", schema = ContractView))
    )]
    async fn raw(&self, _body: RawBody) -> Result<ContractView, HttpApiError> {
        Ok(ContractView {
            id: "raw".to_owned(),
            name: "accepted".to_owned(),
        })
    }

    #[get("/cookie")]
    #[openapi(
        parameters((
            name = "session",
            in = "cookie",
            schema = String,
            description = "Opaque session identifier",
            required = true,
            example = "session-42",
            format = "opaque-session-id"
        )),
        security()
    )]
    async fn cookie(&self, _cookies: RequestCookies) -> Result<ContractView, HttpApiError> {
        Ok(ContractView {
            id: "cookie".to_owned(),
            name: "public".to_owned(),
        })
    }

    #[post("/manual")]
    #[openapi(responses((status = 202, description = "Written manually")))]
    async fn manual(&self, response: &mut Response) -> Result<(), HttpApiError> {
        response.status(202, "Accepted");
        Ok(())
    }

    #[get("/internal")]
    #[openapi(skip)]
    async fn internal(&self, input: InternalOnlyInput) -> Result<String, HttpApiError> {
        Ok(input.value)
    }
}

fn operation_for(method: &str, path: &str) -> lily_http_api::__private::OpenApiOperationMetadata {
    let routes = get_pending_controller_routes();
    let route = routes
        .iter()
        .find(|route| route.method() == method && route.path() == path)
        .unwrap_or_else(|| panic!("missing route {path}"));
    assert_eq!(
        route.openapi_metadata_status(),
        OpenApiRouteMetadataStatus::Documented
    );
    route
        .openapi_operation_factory()
        .unwrap_or_else(|| panic!("missing OpenAPI factory for {path}"))()
}

#[test]
fn controller_opt_in_documents_unspecified_actions_and_applies_defaults() {
    let metadata = operation_for("GET", "/cap07b/:id");
    let operation = metadata.operation();

    assert!(operation
        .tags
        .as_deref()
        .is_some_and(|tags| tags == ["Contracts"]));
    assert_eq!(operation.summary.as_deref(), Some("Fetch one contract."));
    assert_eq!(
        operation.description.as_deref(),
        Some("Fetch one contract.\n\nThe complete action description comes from its doc comment.")
    );
    assert!(operation.responses.responses.contains_key("200"));
    assert!(operation.responses.responses.contains_key("401"));
    assert_eq!(
        serde_json::to_value(operation.security.as_ref().expect("inherited security"))
            .expect("security serializes"),
        serde_json::json!([{ "bearer": [] }])
    );
}

#[test]
fn action_metadata_overrides_docs_and_security_but_inherits_common_responses() {
    let routes = get_pending_controller_routes();
    let route = routes
        .iter()
        .find(|route| route.path() == "/cap07b/:id" && route.method() == "POST")
        .expect("update route exists");
    let metadata = route
        .openapi_operation_factory()
        .expect("update is documented")();
    let operation = metadata.operation();

    assert_eq!(operation.operation_id.as_deref(), Some("contracts.update"));
    assert_eq!(operation.summary.as_deref(), Some("Update one contract"));
    assert_eq!(
        operation.description.as_deref(),
        Some("Explicit action description")
    );
    assert!(operation.deprecated.is_some());
    assert!(operation.responses.responses.contains_key("200"));
    assert!(operation.responses.responses.contains_key("401"));
    assert!(operation.responses.responses.contains_key("404"));
    assert_eq!(
        serde_json::to_value(operation.security.as_ref().expect("action security"))
            .expect("security serializes"),
        serde_json::json!([{ "contract-admin": ["contracts:write"] }])
    );
    assert!(metadata.components().schemas.contains_key("ContractInput"));
    assert!(metadata.components().schemas.contains_key("ContractView"));
    assert!(metadata.components().schemas.contains_key("ApiErrorBody"));
}

#[test]
fn explicit_raw_cookie_manual_and_skip_contracts_are_preserved() {
    let raw = operation_for("POST", "/cap07b/raw");
    let raw_body = raw
        .operation()
        .request_body
        .as_ref()
        .expect("raw body is explicit");
    assert!(raw_body.content.contains_key("application/octet-stream"));
    assert!(raw.operation().responses.responses.contains_key("202"));

    let cookie = operation_for("GET", "/cap07b/cookie");
    let parameters = cookie
        .operation()
        .parameters
        .as_ref()
        .expect("cookie parameter exists");
    assert!(parameters
        .iter()
        .any(|parameter| parameter.name == "session"));
    let cookie_parameter = parameters
        .iter()
        .find(|parameter| parameter.name == "session")
        .expect("cookie parameter exists");
    assert_eq!(
        serde_json::to_value(cookie_parameter).expect("parameter serializes")["schema"]["format"],
        "opaque-session-id"
    );
    assert_eq!(
        serde_json::to_value(
            cookie
                .operation()
                .security
                .as_ref()
                .expect("public override")
        )
        .expect("security serializes"),
        serde_json::json!([])
    );

    let manual = operation_for("POST", "/cap07b/manual");
    assert!(manual.operation().responses.responses.contains_key("202"));

    let routes = get_pending_controller_routes();
    let internal = routes
        .iter()
        .find(|route| route.path() == "/cap07b/internal")
        .expect("skipped route exists");
    assert_eq!(
        internal.openapi_metadata_status(),
        OpenApiRouteMetadataStatus::Skipped
    );
    assert!(internal.openapi_operation_factory().is_none());
}

#[test]
fn controller_openapi_selector_supports_a_module_qualified_impl() {
    let metadata = operation_for("GET", "/cap07b-separated");
    assert!(metadata
        .operation()
        .tags
        .as_deref()
        .is_some_and(|tags| tags == ["Separated"]));
}

#[test]
fn action_without_controller_or_action_opt_in_remains_unspecified() {
    let routes = get_pending_controller_routes();
    let route = routes
        .iter()
        .find(|route| route.path() == "/cap07b-undocumented")
        .expect("undocumented route exists");
    assert_eq!(
        route.openapi_metadata_status(),
        OpenApiRouteMetadataStatus::Unspecified
    );
    assert!(route.openapi_operation_factory().is_none());
}
