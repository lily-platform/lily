use std::future::Future;
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use lily_http_api::AppBuilder;
use lily_http_api::{
    controller, CancellationToken, Controller, ControllerInitError, ControllerTrait, Extensions,
    FromRequest, FromRequestParts, HttpApiError, HttpHealthService, PlainText, Request,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

static OBSERVED_ACTION_INPUT: Mutex<Option<(String, Vec<u8>)>> = Mutex::new(None);

struct QueryMarker(String);

impl FromRequestParts for QueryMarker {
    type Rejection = HttpApiError;

    fn from_request_parts(
        request: &mut Request,
        _extensions: &Extensions,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        let value = request.query_first("value").map(ToString::to_string);
        async move {
            value
                .map(Self)
                .ok_or_else(|| HttpApiError::InvalidQueryString("missing value".to_string()))
        }
    }
}

struct ApplicationPayload(Vec<u8>);

impl FromRequest for ApplicationPayload {
    type Rejection = HttpApiError;

    async fn from_request(
        request: &mut Request,
        _extensions: &Extensions,
    ) -> Result<Self, Self::Rejection> {
        request.buffer_body().await.map_err(HttpApiError::from)?;
        Ok(Self(request.body_bytes().unwrap_or_default().to_vec()))
    }
}

#[derive(Controller)]
#[base_path("/typed-runtime")]
struct TypedRuntimeController;

#[lily_http_api::async_trait::async_trait]
impl ControllerTrait for TypedRuntimeController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        Ok(Self)
    }
}

#[controller]
impl TypedRuntimeController {
    #[post("/execute")]
    async fn execute(
        &self,
        QueryMarker(value): QueryMarker,
        ApplicationPayload(body): ApplicationPayload,
    ) -> Result<PlainText, HttpApiError> {
        *OBSERVED_ACTION_INPUT
            .lock()
            .expect("typed action observation remains available") =
            Some((value.clone(), body.clone()));
        Ok(PlainText(format!("{value}:{}", body.len())))
    }
}

#[tokio::test]
async fn generated_adapter_extracts_in_order_and_invokes_the_app_owned_controller() {
    *OBSERVED_ACTION_INPUT
        .lock()
        .expect("typed action observation remains available") = None;

    let reservation = TcpListener::bind("127.0.0.1:0").expect("reserve a loopback address");
    let address = reservation
        .local_addr()
        .expect("reserved listener has an address");
    drop(reservation);

    let app = AppBuilder::new(&address.to_string())
        .build()
        .await
        .expect("typed controller application builds");
    let health = app
        .extensions()
        .get_service::<HttpHealthService>(None)
        .await
        .expect("HTTP health service resolves");
    let cancellation = CancellationToken::new();
    let cancel = cancellation.clone();
    let server = tokio::spawn(async move { app.start_with_cancellation(cancellation).await });

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if health
                .snapshot()
                .expect("health snapshot remains readable")
                .ready
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("typed controller listener becomes ready");

    let mut connection = tokio::net::TcpStream::connect(address)
        .await
        .expect("typed controller listener accepts a connection");
    connection
        .write_all(
            b"POST /typed-runtime/execute?value=alpha HTTP/1.1\r\n\
Host: localhost\r\n\
Content-Length: 7\r\n\
Connection: close\r\n\
\r\n\
payload",
        )
        .await
        .expect("HTTP request is written");

    let mut response = Vec::new();
    tokio::time::timeout(
        Duration::from_secs(5),
        connection.read_to_end(&mut response),
    )
    .await
    .expect("HTTP response is bounded")
    .expect("HTTP response is readable");
    let response = String::from_utf8(response).expect("HTTP response is UTF-8");
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.ends_with("alpha:7"), "{response}");
    assert_eq!(
        OBSERVED_ACTION_INPUT
            .lock()
            .expect("typed action observation remains available")
            .as_ref(),
        Some(&("alpha".to_string(), b"payload".to_vec()))
    );

    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("typed controller server shutdown is bounded")
        .expect("typed controller server task does not panic")
        .expect("typed controller server shuts down cleanly");
}
