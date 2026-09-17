use std::sync::Arc;

use lily_http_api::__private::get_pending_controller_routes;
use lily_http_api::utoipa::{self, IntoParams, ToResponse, ToSchema};
use lily_http_api::{
    controller, Controller, ControllerInitError, ControllerTrait, Extensions, HttpApiError, Json,
    Path, Query,
};
use serde::{Deserialize, Serialize};
use utoipa::openapi::path::{
    HttpMethod, OperationBuilder, ParameterBuilder, ParameterIn, PathItem, PathsBuilder,
};
use utoipa::openapi::request_body::RequestBodyBuilder;
use utoipa::openapi::response::{Response, ResponsesBuilder};
use utoipa::openapi::schema::{ComponentsBuilder, KnownFormat, ObjectBuilder, SchemaFormat, Type};
use utoipa::openapi::{Content, Info, OpenApiBuilder, Ref, Required};

#[derive(Deserialize, IntoParams)]
struct OpenApiPath {
    id: u64,
}

#[derive(Deserialize, IntoParams)]
struct OpenApiQuery {
    verbose: Option<bool>,
}

#[derive(Deserialize, ToSchema)]
struct OpenApiProfile {
    display_name: String,
}

#[derive(Deserialize, ToSchema)]
struct OpenApiInput {
    profile: OpenApiProfile,
}

#[derive(Serialize, ToSchema)]
struct OpenApiView {
    id: u64,
    display_name: String,
    verbose: bool,
}

#[derive(Deserialize)]
struct UndocumentedInput {
    value: String,
}

#[derive(Serialize)]
struct UndocumentedView {
    value: String,
}

#[derive(Controller)]
#[base_path("/cap07a")]
struct OpenApiSpikeController;

#[lily_http_api::async_trait::async_trait]
impl ControllerTrait for OpenApiSpikeController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        Ok(Self)
    }
}

#[controller]
impl OpenApiSpikeController {
    #[post("/:id")]
    #[openapi]
    async fn update(
        &self,
        Path(path): Path<OpenApiPath>,
        Query(query): Query<OpenApiQuery>,
        Json(input): Json<OpenApiInput>,
    ) -> Result<OpenApiView, HttpApiError> {
        Ok(OpenApiView {
            id: path.id,
            display_name: input.profile.display_name,
            verbose: query.verbose.unwrap_or(false),
        })
    }

    #[post("/undocumented")]
    async fn undocumented(
        &self,
        Json(input): Json<UndocumentedInput>,
    ) -> Result<UndocumentedView, HttpApiError> {
        Ok(UndocumentedView { value: input.value })
    }
}

#[derive(ToSchema)]
#[allow(dead_code)]
struct RecursiveNode {
    value: String,
    #[schema(no_recursion)]
    children: Vec<RecursiveNode>,
}

/// A reusable conflict response.
#[derive(ToResponse)]
#[response(description = "Conflict")]
#[allow(dead_code)]
struct ConflictResponse {
    code: String,
}

#[test]
fn documented_action_factory_uses_the_same_pending_route_authority() {
    let routes = get_pending_controller_routes();
    let undocumented = routes
        .iter()
        .find(|route| route.path() == "/cap07a/undocumented")
        .expect("the undocumented controller route is registered");
    assert!(undocumented.openapi_operation_factory().is_none());

    let route = routes
        .into_iter()
        .find(|route| route.path() == "/cap07a/:id")
        .expect("the generated controller route is registered");

    assert_eq!(route.method(), "POST");
    let factory = route
        .openapi_operation_factory()
        .expect("empty #[openapi] attaches one monomorphized factory");
    let (operation, components) = factory().into_parts();

    let document = OpenApiBuilder::new()
        .info(Info::new("CAP-07A", "0.1.0"))
        .paths(
            PathsBuilder::new()
                .path(
                    route.path().replace(":id", "{id}"),
                    PathItem::new(HttpMethod::Post, operation),
                )
                .build(),
        )
        .components(Some(components))
        .build();
    let value = serde_json::to_value(document).expect("OpenAPI document serializes");

    assert_eq!(value["openapi"], "3.1.0");
    let operation = &value["paths"]["/cap07a/{id}"]["post"];
    let parameters = operation["parameters"]
        .as_array()
        .expect("path and query parameters are generated");
    assert!(parameters
        .iter()
        .any(|parameter| parameter["name"] == "id" && parameter["in"] == "path"));
    assert!(parameters
        .iter()
        .any(|parameter| parameter["name"] == "verbose" && parameter["in"] == "query"));
    assert_eq!(
        operation["requestBody"]["content"]["application/json"]["schema"]["$ref"],
        "#/components/schemas/OpenApiInput"
    );
    assert_eq!(
        operation["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
        "#/components/schemas/OpenApiView"
    );
    assert!(value["components"]["schemas"]["OpenApiInput"].is_object());
    assert!(value["components"]["schemas"]["OpenApiProfile"].is_object());
    assert!(value["components"]["schemas"]["OpenApiView"].is_object());
}

#[test]
fn utoipa_model_represents_the_remaining_lily_input_and_response_shapes() {
    let string_schema = ObjectBuilder::new().schema_type(Type::String).build();
    let binary_schema = ObjectBuilder::new()
        .schema_type(Type::String)
        .format(Some(SchemaFormat::KnownFormat(KnownFormat::Binary)))
        .build();
    let multipart_schema = ObjectBuilder::new()
        .property("description", string_schema.clone())
        .property("file", binary_schema.clone())
        .required("file")
        .build();

    let header = ParameterBuilder::new()
        .name("x-request-id")
        .parameter_in(ParameterIn::Header)
        .schema(Some(string_schema.clone()))
        .build();
    let cookie = ParameterBuilder::new()
        .name("session")
        .parameter_in(ParameterIn::Cookie)
        .schema(Some(string_schema))
        .build();
    let explicit_responses = ResponsesBuilder::new()
        .response("201", Response::new("Passthrough typed success"))
        .response("202", Response::new("Manual response"))
        .response("204", Response::new("No content"))
        .build();
    let operation = OperationBuilder::new()
        .parameters(Some([header, cookie]))
        .request_body(Some(
            RequestBodyBuilder::new()
                .required(Some(Required::True))
                .content("multipart/form-data", Content::new(Some(multipart_schema)))
                .build(),
        ))
        .responses(explicit_responses)
        .build();
    let operation = serde_json::to_value(operation).expect("operation serializes");

    assert_eq!(operation["parameters"][0]["in"], "header");
    assert_eq!(operation["parameters"][1]["in"], "cookie");
    assert_eq!(
        operation["requestBody"]["content"]["multipart/form-data"]["schema"]["properties"]["file"]
            ["format"],
        "binary"
    );
    for status in ["201", "202", "204"] {
        assert!(operation["responses"][status].is_object());
    }

    let form = RequestBodyBuilder::new()
        .content(
            "application/x-www-form-urlencoded",
            Content::new(Some(Ref::from_schema_name("FormPayload"))),
        )
        .build();
    let raw_or_stream = RequestBodyBuilder::new()
        .content(
            "application/octet-stream",
            Content::new(Some(binary_schema)),
        )
        .build();
    let form = serde_json::to_value(form).expect("form body serializes");
    let raw_or_stream = serde_json::to_value(raw_or_stream).expect("raw/stream body serializes");
    assert!(form["content"]["application/x-www-form-urlencoded"].is_object());
    assert_eq!(
        raw_or_stream["content"]["application/octet-stream"]["schema"]["format"],
        "binary"
    );
}

#[test]
fn utoipa_collects_recursive_schemas_and_reusable_responses() {
    let mut schemas = Vec::new();
    lily_http_api::__private::collect_openapi_schema::<RecursiveNode>(&mut schemas);
    let components = ComponentsBuilder::new()
        .schemas_from_iter(schemas)
        .response_from::<ConflictResponse>()
        .build();
    let components = serde_json::to_value(components).expect("components serialize");

    assert!(components["schemas"]["RecursiveNode"].is_object());
    assert!(components["responses"]["ConflictResponse"].is_object());
}
