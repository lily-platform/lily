use std::sync::Arc;

use lily_http_api::async_trait::async_trait;
use lily_http_api::headers::UserAgent;
use lily_http_api::utoipa::{self, IntoParams, ToSchema};
use lily_http_api::AppBuilder;
use lily_http_api::{
    controller, ApplicationContainer, BodyStream, ClientIp, Controller, ControllerInitError,
    ControllerTrait, Extensions, Form, FormFile, HttpApiError, Json, Local, MultipartForm,
    NoContent, OpenApiConfig, OpenApiService, PassthroughResponseContext, Path, Principal, Query,
    RawBody, RequestCookies, Response, Service, SseResponse, StaticFileResponse, StreamingResponse,
    TypedHeader,
};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, IntoParams)]
struct QualificationPath {
    id: String,
}

#[derive(Deserialize, IntoParams)]
struct QualificationQuery {
    search: Option<String>,
}

#[derive(Deserialize, ToSchema)]
struct QualificationInput {
    value: String,
}

#[derive(Serialize, ToSchema)]
struct QualificationView {
    value: String,
}

#[derive(ToSchema)]
#[allow(dead_code)]
struct BinaryPayload {
    bytes: String,
}

#[derive(lily_http_api::MultipartForm)]
struct QualificationUpload {
    title: String,
    #[form_file]
    file: FormFile,
}

#[derive(Clone)]
struct QualificationLocal;

struct QualificationService;

#[derive(Controller)]
#[base_path("/cap07g")]
#[openapi(tag = "Qualification")]
struct QualificationController;

#[async_trait]
impl ControllerTrait for QualificationController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        Ok(Self)
    }
}

fn view(value: impl Into<String>) -> QualificationView {
    QualificationView {
        value: value.into(),
    }
}

#[controller]
impl QualificationController {
    #[get("/:id")]
    #[openapi(parameters((
        name = "session",
        in = "cookie",
        schema = String,
        description = "Opaque session identifier"
    )))]
    #[allow(clippy::too_many_arguments)]
    async fn parts(
        &self,
        Path(path): Path<QualificationPath>,
        Query(query): Query<QualificationQuery>,
        TypedHeader(_agent): TypedHeader<UserAgent>,
        _cookies: RequestCookies,
        _service: Service<QualificationService>,
        _principal: Principal,
        _local: Local<QualificationLocal>,
        _client_ip: ClientIp,
    ) -> Result<QualificationView, HttpApiError> {
        Ok(view(format!(
            "{}:{}",
            path.id,
            query.search.as_deref().unwrap_or_default()
        )))
    }

    #[post("/json")]
    async fn json(
        &self,
        Json(input): Json<QualificationInput>,
    ) -> Result<QualificationView, HttpApiError> {
        Ok(view(input.value))
    }

    #[post("/form")]
    async fn form(
        &self,
        Form(input): Form<QualificationInput>,
    ) -> Result<QualificationView, HttpApiError> {
        Ok(view(input.value))
    }

    #[post("/multipart")]
    async fn multipart(
        &self,
        MultipartForm(input): MultipartForm<QualificationUpload>,
    ) -> Result<NoContent, HttpApiError> {
        let _ = (input.title, input.file);
        Ok(NoContent)
    }

    #[post("/raw")]
    #[openapi(request_body(
        content_type = "application/octet-stream",
        schema = BinaryPayload,
        description = "Buffered opaque payload"
    ))]
    async fn raw(&self, _body: RawBody) -> Result<NoContent, HttpApiError> {
        Ok(NoContent)
    }

    #[post("/body-stream")]
    #[openapi(request_body(
        content_type = "application/octet-stream",
        schema = BinaryPayload,
        description = "Streaming opaque payload"
    ))]
    async fn body_stream(&self, _body: BodyStream) -> Result<NoContent, HttpApiError> {
        Ok(NoContent)
    }

    #[delete("/:id")]
    async fn no_content(
        &self,
        Path(_path): Path<QualificationPath>,
    ) -> Result<NoContent, HttpApiError> {
        Ok(NoContent)
    }

    #[post("/passthrough")]
    #[openapi(responses((
        status = 201,
        description = "Created with staged metadata",
        schema = QualificationView
    )))]
    async fn passthrough(
        &self,
        mut response: PassthroughResponseContext<'_>,
    ) -> Result<QualificationView, HttpApiError> {
        response.status(201)?;
        Ok(view("passthrough"))
    }

    #[post("/manual")]
    #[openapi(responses((status = 202, description = "Written manually")))]
    async fn manual(&self, response: &mut Response) -> Result<(), HttpApiError> {
        response.status(202, "Accepted");
        Ok(())
    }

    #[post("/preserved")]
    #[openapi(responses((status = 202, description = "Preserved response")))]
    async fn preserved(&self) -> Result<(), HttpApiError> {
        Ok(())
    }

    #[get("/sse")]
    #[openapi(responses((
        status = 200,
        description = "Server-sent event stream",
        schema = String,
        content_type = "text/event-stream"
    )))]
    async fn sse(&self) -> Result<SseResponse, HttpApiError> {
        unimplemented!("qualification fixture is never dispatched")
    }

    #[get("/stream")]
    #[openapi(responses((
        status = 200,
        description = "Byte stream",
        schema = BinaryPayload,
        content_type = "application/octet-stream"
    )))]
    async fn stream(&self) -> Result<StreamingResponse, HttpApiError> {
        unimplemented!("qualification fixture is never dispatched")
    }

    #[get("/static")]
    #[openapi(responses(
        (status = 200, description = "Complete file", schema = BinaryPayload, content_type = "application/octet-stream"),
        (status = 206, description = "Partial file", schema = BinaryPayload, content_type = "application/octet-stream"),
        (status = 304, description = "Not modified")
    ))]
    async fn static_file(&self) -> Result<StaticFileResponse, HttpApiError> {
        unimplemented!("qualification fixture is never dispatched")
    }
}

async fn build_document(address: &str) -> Vec<u8> {
    let container = Arc::new(
        ApplicationContainer::build()
            .await
            .expect("qualification container builds"),
    );
    let mut config = OpenApiConfig::new("CAP-07G Qualification", "1.0.0").unwrap();
    config
        .description("Complete typed action qualification document")
        .unwrap()
        .register_tag("Qualification", Some("Typed action matrix"))
        .unwrap();
    let app = AppBuilder::new(address)
        .container(Arc::clone(&container))
        .openapi(config)
        .build()
        .await
        .expect("OpenAPI qualification App builds");
    let document = container
        .resolve::<OpenApiService>(None)
        .await
        .expect("OpenAPI service resolves")
        .snapshot()
        .expect("OpenAPI document is attached")
        .canonical_json()
        .to_vec();
    drop(app);
    container
        .close()
        .await
        .expect("qualification container closes");
    document
}

#[tokio::test]
async fn typed_action_matrix_produces_one_complete_byte_stable_document() {
    let first = build_document("127.0.0.1:45171").await;
    let second = build_document("127.0.0.1:45172").await;
    assert_eq!(first, second);

    let document: serde_json::Value = serde_json::from_slice(&first).unwrap();
    assert_eq!(document["openapi"], "3.1.0");
    let paths = document["paths"].as_object().expect("paths object");
    assert_eq!(paths.len(), 12);
    assert_eq!(
        paths
            .values()
            .map(|path| path.as_object().expect("path item").len())
            .sum::<usize>(),
        13
    );

    let parts = &document["paths"]["/cap07g/{id}"]["get"];
    let parameters = parts["parameters"].as_array().expect("parts parameters");
    assert_eq!(parameters.len(), 4);
    for (name, location) in [
        ("id", "path"),
        ("search", "query"),
        ("user-agent", "header"),
        ("session", "cookie"),
    ] {
        assert!(parameters
            .iter()
            .any(|parameter| parameter["name"] == name && parameter["in"] == location));
    }

    for (path, content_type) in [
        ("/cap07g/json", "application/json"),
        ("/cap07g/form", "application/x-www-form-urlencoded"),
        ("/cap07g/multipart", "multipart/form-data"),
        ("/cap07g/raw", "application/octet-stream"),
        ("/cap07g/body-stream", "application/octet-stream"),
    ] {
        assert!(document["paths"][path]["post"]["requestBody"]["content"]
            .get(content_type)
            .is_some());
    }

    assert!(document["paths"]["/cap07g/{id}"]["delete"]["responses"]
        .get("204")
        .is_some());
    assert!(
        document["paths"]["/cap07g/passthrough"]["post"]["responses"]
            .get("201")
            .is_some()
    );
    assert!(document["paths"]["/cap07g/manual"]["post"]["responses"]
        .get("202")
        .is_some());
    assert!(
        document["paths"]["/cap07g/sse"]["get"]["responses"]["200"]["content"]
            .get("text/event-stream")
            .is_some()
    );
    assert!(
        document["paths"]["/cap07g/stream"]["get"]["responses"]["200"]["content"]
            .get("application/octet-stream")
            .is_some()
    );
    for status in ["200", "206", "304"] {
        assert!(document["paths"]["/cap07g/static"]["get"]["responses"]
            .get(status)
            .is_some());
    }

    if let Some(path) = std::env::var_os("LILY_CAP07G_OPENAPI_EVIDENCE") {
        std::fs::write(path, &first).expect("qualification evidence is written");
    }
}
