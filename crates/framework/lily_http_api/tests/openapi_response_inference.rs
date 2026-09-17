use std::sync::Arc;

use lily_http_api::__private::get_pending_controller_routes;
use lily_http_api::async_trait::async_trait;
use lily_http_api::utoipa::{self, ToSchema};
use lily_http_api::{
    controller, Accepted, BinaryData, Controller, ControllerInitError, ControllerTrait, Created,
    Extensions, HttpApiError, IntoResponse, Json, NoContent, PlainText, Request, Response,
    ResponseBuilder, ResponseWriteError, ResponseWriteOutcome,
};
use serde::Serialize;

#[derive(Serialize, ToSchema)]
struct InferredView {
    value: String,
}

// Error schemas are explicit metadata; runtime conversion needs no ToSchema.
struct ApplicationError;

#[async_trait]
impl IntoResponse for ApplicationError {
    async fn write_to_response(
        self,
        response: &mut Response,
        request: &mut Request,
    ) -> Result<ResponseWriteOutcome, ResponseWriteError> {
        ResponseBuilder::new()
            .status(404, "Not Found")
            .text("missing")
            .write_to_response(response, request)
            .await
    }
}

#[derive(Controller)]
#[base_path("/openapi-response-inference")]
struct ResponseInferenceController;

#[async_trait]
impl ControllerTrait for ResponseInferenceController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        Ok(Self)
    }
}

#[controller]
impl ResponseInferenceController {
    #[post("/created")]
    #[openapi(responses((status = 404, description = "Application error")))]
    async fn created(&self) -> Result<Created<InferredView>, ApplicationError> {
        Ok(Created::new(
            "/resources/7",
            InferredView {
                value: "created".to_owned(),
            },
        ))
    }

    #[post("/accepted")]
    #[openapi]
    async fn accepted(&self) -> Accepted<InferredView> {
        Accepted::new(
            "/jobs/7",
            InferredView {
                value: "queued".to_owned(),
            },
        )
    }

    #[post("/created-empty")]
    #[openapi]
    async fn created_empty(&self) -> Created {
        Created::empty().with_location("/resources/7")
    }

    #[post("/accepted-empty")]
    #[openapi]
    async fn accepted_empty(&self) -> Result<Accepted, ApplicationError> {
        Ok(Accepted::empty())
    }

    #[post("/created-null")]
    #[openapi]
    async fn created_null(&self) -> Created<()> {
        Created::without_location(())
    }

    #[get("/custom-error-dto")]
    #[openapi]
    async fn custom_error_dto(&self) -> Result<InferredView, ApplicationError> {
        Err(ApplicationError)
    }

    #[get("/custom-error-text")]
    #[openapi]
    async fn custom_error_text(&self) -> Result<PlainText, ApplicationError> {
        Err(ApplicationError)
    }

    #[get("/custom-error-string")]
    #[openapi]
    async fn custom_error_string(&self) -> Result<String, ApplicationError> {
        Err(ApplicationError)
    }
    #[get("/direct-string")]
    #[openapi]
    async fn direct_string(&self) -> String {
        "plain".to_owned()
    }

    #[get("/direct-str")]
    #[openapi]
    async fn direct_str(&self) -> &'static str {
        "plain"
    }

    #[get("/result-string")]
    #[openapi]
    async fn result_string(&self) -> Result<String, HttpApiError> {
        Ok("json string".to_owned())
    }

    #[get("/bool")]
    #[openapi]
    async fn bool_value(&self) -> bool {
        true
    }

    #[get("/direct-bytes")]
    #[openapi]
    async fn direct_bytes(&self) -> Vec<u8> {
        vec![1, 2, 3]
    }

    #[get("/result-bytes")]
    #[openapi]
    async fn result_bytes(&self) -> Result<Vec<u8>, HttpApiError> {
        Ok(vec![1, 2, 3])
    }

    #[get("/json-wrapper")]
    #[openapi]
    async fn json_wrapper(&self) -> Json<InferredView> {
        Json(InferredView {
            value: "json".to_owned(),
        })
    }

    #[get("/plain-text")]
    #[openapi]
    async fn plain_text(&self) -> Result<PlainText, HttpApiError> {
        Ok(PlainText("plain".to_owned()))
    }

    #[get("/binary-data")]
    #[openapi]
    async fn binary_data(&self) -> Result<BinaryData, HttpApiError> {
        Ok(BinaryData(vec![1, 2, 3]))
    }

    #[delete("/no-content")]
    #[openapi]
    async fn no_content(&self) -> Result<NoContent, HttpApiError> {
        Ok(NoContent)
    }

    #[post("/builder")]
    #[openapi(responses((
        status = 202,
        description = "Explicit builder response",
        schema = String,
        content_type = "text/plain; charset=utf-8"
    )))]
    async fn builder(&self) -> ResponseBuilder {
        ResponseBuilder::new()
            .status(202, "Accepted")
            .text("accepted")
    }
}

fn operation(path: &str) -> serde_json::Value {
    let route = get_pending_controller_routes()
        .into_iter()
        .find(|route| route.path() == path)
        .unwrap_or_else(|| panic!("route {path} is registered"));
    let factory = route
        .openapi_operation_factory()
        .unwrap_or_else(|| panic!("route {path} is documented"));
    serde_json::to_value(factory().operation()).expect("operation serializes")
}

fn response_content(path: &str, status: &str) -> serde_json::Value {
    operation(path)["responses"][status]["content"].clone()
}

#[test]
fn inferred_openapi_media_types_match_runtime_into_response_semantics() {
    for suffix in [
        "direct-string",
        "direct-str",
        "plain-text",
        "custom-error-text",
    ] {
        let content = response_content(&format!("/openapi-response-inference/{suffix}"), "200");
        assert!(content["text/plain; charset=utf-8"].is_object());
        assert!(content.get("application/json").is_none());
    }

    for suffix in [
        "result-string",
        "bool",
        "result-bytes",
        "json-wrapper",
        "custom-error-dto",
        "custom-error-string",
    ] {
        let content = response_content(&format!("/openapi-response-inference/{suffix}"), "200");
        assert!(content["application/json"].is_object());
    }

    for suffix in ["direct-bytes", "binary-data"] {
        let content = response_content(&format!("/openapi-response-inference/{suffix}"), "200");
        assert_eq!(
            content["application/octet-stream"]["schema"]["type"],
            "string"
        );
        assert_eq!(
            content["application/octet-stream"]["schema"]["format"],
            "binary"
        );
    }

    let no_content = operation("/openapi-response-inference/no-content");
    assert!(no_content["responses"]["204"].is_object());
    assert!(no_content["responses"]["204"].get("content").is_none());

    let builder = response_content("/openapi-response-inference/builder", "202");
    assert!(builder["text/plain; charset=utf-8"].is_object());
}

#[test]
fn location_response_metadata_uses_inner_schema_correct_status_and_optional_header() {
    for (name, status) in [("created", "201"), ("accepted", "202")] {
        for empty in [false, true] {
            let path = format!(
                "/openapi-response-inference/{name}{}",
                if empty { "-empty" } else { "" }
            );
            let operation = operation(&path);
            let responses = &operation["responses"];
            assert!(responses.get("200").is_none(), "{responses}");
            let response = &responses[status];
            assert!(response.is_object());
            let header = &response["headers"]["Location"];
            assert_eq!(header["schema"]["type"], "string");
            assert!(header["description"]
                .as_str()
                .unwrap()
                .starts_with("Optional URI"));
            if empty {
                assert!(response.get("content").is_none());
            } else {
                assert_eq!(
                    response["content"]["application/json"]["schema"]["$ref"],
                    "#/components/schemas/InferredView"
                );
            }
        }
    }
    assert!(operation("/openapi-response-inference/created")["responses"]["404"].is_object());
    // An explicit JSON unit remains a JSON representation, not an empty response.
    assert!(
        response_content("/openapi-response-inference/created-null", "201")["application/json"]
            .is_object()
    );
}
