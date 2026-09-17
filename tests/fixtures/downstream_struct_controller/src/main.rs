use std::sync::Arc;

use framework::utoipa::{self, ToSchema};
use framework::{
    controller, AppBuilder, Controller, ControllerInitError, ControllerTrait, Extensions,
    HttpApiError, Injectable, IntoResponse, OpenApiConfig, Request, Response, ResponseWriteError,
    ResponseWriteOutcome, ServiceTrait,
};

// The renamed HTTP facade must provide DI as well as controller derives.
#[derive(Injectable)]
#[service(lifetime = "Scoped")]
struct RequestService;
impl ServiceTrait for RequestService {}

struct PublicError;

#[framework::async_trait::async_trait]
impl IntoResponse for PublicError {
    async fn write_to_response(
        self,
        response: &mut Response,
        request: &mut Request,
    ) -> Result<ResponseWriteOutcome, ResponseWriteError> {
        framework::ResponseBuilder::new()
            .status(404, "Not Found")
            .text("missing")
            .write_to_response(response, request)
            .await
    }
}

#[derive(serde::Serialize, ToSchema)]
struct RenamedRuntimeView {
    status: String,
}

#[derive(framework::MultipartForm)]
struct RenamedMultipartInput {
    title: String,
}

#[derive(Controller)]
#[base_path("/api/renamed-runtime")]
#[openapi(tag = "Renamed runtime")]
struct RenamedRuntimeController;

#[framework::async_trait::async_trait]
impl ControllerTrait for RenamedRuntimeController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        Ok(Self)
    }
}

#[controller]
impl RenamedRuntimeController {
    #[post("/created")]
    #[openapi]
    async fn created(&self) -> Result<framework::Created<RenamedRuntimeView>, PublicError> {
        Ok(framework::Created::new(
            "/resources/7",
            RenamedRuntimeView {
                status: "created".to_owned(),
            },
        ))
    }

    #[post("/accepted")]
    #[openapi]
    async fn accepted(&self) -> Result<framework::Accepted<RenamedRuntimeView>, PublicError> {
        Ok(framework::Accepted::new(
            "/jobs/7",
            RenamedRuntimeView {
                status: "queued".to_owned(),
            },
        ))
    }

    #[post("/created-empty")]
    #[openapi]
    async fn created_empty(&self) -> framework::Created {
        framework::Created::empty()
    }

    #[post("/accepted-empty")]
    #[openapi]
    async fn accepted_empty(
        &self,
    ) -> Result<framework::Accepted<framework::EmptyBody>, PublicError> {
        Ok(framework::Accepted::empty().with_location("/jobs/7"))
    }

    #[get("/")]
    #[openapi(
        operation_id = "renamed_runtime.index",
        responses((
            status = 500,
            description = "Renamed runtime failure",
            schema = RenamedRuntimeView,
            example = "failed"
        ))
    )]
    async fn index(&self) -> Result<RenamedRuntimeView, PublicError> {
        Ok(RenamedRuntimeView {
            status: "ok".to_owned(),
        })
    }

    #[post("/multipart")]
    #[openapi(operation_id = "renamed_runtime.multipart")]
    async fn multipart(
        &self,
        framework::MultipartForm(input): framework::MultipartForm<RenamedMultipartInput>,
    ) -> Result<RenamedRuntimeView, HttpApiError> {
        Ok(RenamedRuntimeView {
            status: input.title,
        })
    }
}

#[derive(Controller)]
#[base_path("/custom-errors")]
struct CustomErrorController;

#[framework::async_trait::async_trait]
impl ControllerTrait for CustomErrorController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        Ok(Self)
    }
}

#[controller]
impl CustomErrorController {
    #[get("/json")]
    #[openapi(skip)]
    async fn json(&self) -> Result<framework::Json<RenamedRuntimeView>, PublicError> {
        Err(PublicError)
    }
    #[get("/text")]
    #[openapi(skip)]
    async fn text(&self) -> Result<framework::PlainText, PublicError> {
        Err(PublicError)
    }
    #[get("/binary")]
    #[openapi(skip)]
    async fn binary(&self) -> Result<framework::BinaryData, PublicError> {
        Err(PublicError)
    }
    #[get("/builder")]
    #[openapi(skip)]
    async fn builder(&self) -> Result<framework::ResponseBuilder, PublicError> {
        Err(PublicError)
    }
    #[get("/empty")]
    #[openapi(skip)]
    async fn empty(&self) -> Result<framework::NoContent, PublicError> {
        Err(PublicError)
    }
    #[get("/stream")]
    #[openapi(skip)]
    async fn stream(&self) -> Result<framework::StreamingResponse, PublicError> {
        Err(PublicError)
    }
    #[get("/sse")]
    #[openapi(skip)]
    async fn sse(&self) -> Result<framework::SseResponse, PublicError> {
        Err(PublicError)
    }
    #[get("/file")]
    #[openapi(skip)]
    async fn file(&self) -> Result<framework::StaticFileResponse, PublicError> {
        Err(PublicError)
    }
    #[get("/manual")]
    #[openapi(skip)]
    async fn manual(&self, _response: &mut Response) -> Result<(), PublicError> {
        Err(PublicError)
    }
    #[get("/passthrough")]
    #[openapi(skip)]
    async fn passthrough(
        &self,
        _response: framework::PassthroughResponseContext<'_>,
    ) -> Result<RenamedRuntimeView, PublicError> {
        Err(PublicError)
    }
}

fn main() {
    let openapi = OpenApiConfig::new("Renamed Runtime API", "1.0.0")
        .expect("fixture OpenAPI identity is valid");
    let build = AppBuilder::new("127.0.0.1:8080")
        .tls_disabled()
        .openapi(openapi)
        .build();

    // The future is deliberately not polled. This fixture verifies that every
    // generated path resolves through the renamed public facade.
    drop(build);
}
