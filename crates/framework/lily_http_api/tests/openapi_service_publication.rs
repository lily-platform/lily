use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use lily_http_api::async_trait::async_trait;
use lily_http_api::AppBuilder;
use lily_http_api::{
    controller, ApplicationContainer, CancellationToken, Controller, ControllerInitError,
    ControllerTrait, Extensions, HttpApiError, HttpHealthService, NoContent, OpenApiConfig,
    OpenApiJson, OpenApiSecurityScheme, OpenApiService, OpenApiServiceError, Service,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

static CONSTRUCTOR_OBSERVED_UNINITIALIZED: AtomicBool = AtomicBool::new(false);

#[derive(Controller)]
#[base_path("/cap07e")]
#[openapi(tag = "Public")]
struct OpenApiPublicationController {
    openapi: Arc<OpenApiService>,
}

#[async_trait]
impl ControllerTrait for OpenApiPublicationController {
    async fn new(extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        let openapi = extensions
            .get_service::<OpenApiService>(None)
            .await
            .map_err(ControllerInitError::dependency)?;
        CONSTRUCTOR_OBSERVED_UNINITIALIZED.store(
            matches!(openapi.snapshot(), Err(OpenApiServiceError::NotInitialized)),
            Ordering::SeqCst,
        );
        Ok(Self { openapi })
    }
}

#[controller]
impl OpenApiPublicationController {
    #[get("/status")]
    #[openapi(
        operation_id = "cap07e.status",
        security(("bearer" = []))
    )]
    async fn status(&self) -> Result<NoContent, HttpApiError> {
        Ok(NoContent)
    }

    #[get("/openapi.json")]
    #[openapi(skip)]
    async fn document(
        &self,
        Service(openapi): Service<OpenApiService>,
    ) -> Result<OpenApiJson, HttpApiError> {
        if !Arc::ptr_eq(&self.openapi, &openapi) {
            return Err(HttpApiError::InitializationError(
                "OpenAPI DI identity changed".to_owned(),
            ));
        }
        openapi.json().map_err(Into::into)
    }
}

#[tokio::test]
async fn enabled_app_attaches_one_document_and_user_action_publishes_it() {
    CONSTRUCTOR_OBSERVED_UNINITIALIZED.store(false, Ordering::SeqCst);
    let reservation = TcpListener::bind("127.0.0.1:0").expect("reserve loopback address");
    let address = reservation
        .local_addr()
        .expect("reserved address is readable");
    drop(reservation);

    let mut config =
        OpenApiConfig::new("Lily Publication API", "2026.08").expect("document info is valid");
    config
        .description("CAP-07E immutable publication fixture")
        .unwrap()
        .register_server("https://api.example.test/v1", Some("Production"))
        .unwrap()
        .register_tag("Public", Some("Public operations"))
        .unwrap()
        .register_security_scheme(
            OpenApiSecurityScheme::bearer("bearer", Some("JWT"), None)
                .expect("bearer scheme is valid"),
        )
        .unwrap();

    let container = Arc::new(
        ApplicationContainer::build()
            .await
            .expect("caller-owned container builds"),
    );
    assert!(container.resolve::<OpenApiService>(None).await.is_err());

    let app = AppBuilder::new(&address.to_string())
        .container(Arc::clone(&container))
        .openapi(config)
        .build()
        .await
        .expect("OpenAPI-enabled application builds");
    assert!(CONSTRUCTOR_OBSERVED_UNINITIALIZED.load(Ordering::SeqCst));

    let service = container
        .resolve::<OpenApiService>(None)
        .await
        .expect("OpenAPI service resolves after attachment");
    let snapshot = service
        .snapshot()
        .expect("document is attached before build returns");
    let value: serde_json::Value =
        serde_json::from_slice(snapshot.canonical_json()).expect("canonical document is JSON");
    assert_eq!(value["openapi"], "3.1.0");
    assert_eq!(value["info"]["title"], "Lily Publication API");
    assert_eq!(value["info"]["version"], "2026.08");
    assert_eq!(value["servers"][0]["url"], "https://api.example.test/v1");
    assert_eq!(value["tags"][0]["name"], "Public");
    assert!(value["paths"].get("/cap07e/status").is_some());
    assert!(value["paths"].get("/cap07e/openapi.json").is_none());
    assert!(value["components"]["securitySchemes"]
        .get("bearer")
        .is_some());
    let expected_json = snapshot.canonical_json().to_vec();

    let health = container
        .resolve::<HttpHealthService>(None)
        .await
        .expect("health service resolves");
    let cancellation = CancellationToken::new();
    let cancel = cancellation.clone();
    let server = tokio::spawn(async move { app.start_with_cancellation(cancellation).await });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if health.snapshot().expect("health remains readable").ready {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("listener becomes ready");

    let mut connection = tokio::net::TcpStream::connect(address)
        .await
        .expect("listener accepts the documentation request");
    connection
        .write_all(
            b"GET /cap07e/openapi.json HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await
        .expect("documentation request is written");
    let mut response = Vec::new();
    tokio::time::timeout(
        Duration::from_secs(5),
        connection.read_to_end(&mut response),
    )
    .await
    .expect("documentation response is bounded")
    .expect("documentation response is readable");
    let separator = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("HTTP response contains a header boundary");
    let headers =
        std::str::from_utf8(&response[..separator]).expect("HTTP response headers are UTF-8");
    assert!(headers.starts_with("HTTP/1.1 200"), "{headers}");
    assert!(
        headers
            .to_ascii_lowercase()
            .contains("content-type: application/json"),
        "{headers}"
    );
    assert_eq!(&response[separator + 4..], expected_json.as_slice());

    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("server shutdown is bounded")
        .expect("server task does not panic")
        .expect("server shuts down cleanly");
    container
        .close()
        .await
        .expect("caller-owned container closes cleanly");
}
