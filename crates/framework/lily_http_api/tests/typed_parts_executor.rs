use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use lily_http_api::headers::UserAgent;
use lily_http_api::AppBuilder;
use lily_http_api::{
    controller, CancellationToken, ClientIp, Controller, ControllerInitError, ControllerTrait,
    Extensions, GuardInitError, GuardRejection, GuardTrait, HttpApiError, HttpHealthService, Local,
    Path, PlainText, Principal, Query, Request, RequestCookies, TypedHeader,
};
use serde::Deserialize;
use serde_json::Map;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

const UUID_VALUE: &str = "550e8400-e29b-41d4-a716-446655440000";

#[derive(Debug)]
struct RuntimeTenant(String);

struct RuntimeIdentityGuard;

#[lily_http_api::async_trait::async_trait]
impl GuardTrait for RuntimeIdentityGuard {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, GuardInitError> {
        Ok(Self)
    }

    async fn can_activate(
        &self,
        request: &mut Request,
        _cancellation: lily_http_api::ExecutionCancellation,
    ) -> Result<(), GuardRejection> {
        request.set_principal(Principal::new("user-42", [], [], Map::new()));
        request
            .local_mut()
            .insert(Arc::new(RuntimeTenant("tenant-a".to_string())));
        Ok(())
    }
}

#[derive(Deserialize)]
struct RuntimePath {
    id: Uuid,
    slug: String,
}

#[derive(Deserialize)]
struct RuntimeQuery {
    enabled: bool,
    count: u32,
    optional: Option<u16>,
    #[serde(default)]
    tag: Vec<String>,
}

#[derive(Controller)]
#[base_path("/typed-parts")]
struct TypedPartsController;

#[lily_http_api::async_trait::async_trait]
impl ControllerTrait for TypedPartsController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        Ok(Self)
    }
}

#[controller]
impl TypedPartsController {
    #[allow(clippy::too_many_arguments)]
    #[get("/:id/:slug")]
    #[guard(RuntimeIdentityGuard)]
    async fn inspect(
        &self,
        Path(path): Path<RuntimePath>,
        Query(query): Query<RuntimeQuery>,
        TypedHeader(user_agent): TypedHeader<UserAgent>,
        cookies: RequestCookies,
        principal: Principal,
        Local(tenant): Local<Arc<RuntimeTenant>>,
        ClientIp(client_ip): ClientIp,
        request: &mut Request,
    ) -> Result<PlainText, HttpApiError> {
        let session = cookies
            .get("session")
            .map_err(|_| HttpApiError::BadRequest("session cookie is invalid".to_string()))?
            .ok_or_else(|| HttpApiError::BadRequest("session cookie is missing".to_string()))?;
        assert_eq!(request.query_values("tag").count(), 2);
        assert!(client_ip.is_loopback());

        Ok(PlainText(format!(
            "{}|{}|{}|{}|{:?}|{}|{}|{}|{}|{}",
            path.id,
            path.slug,
            query.enabled,
            query.count,
            query.optional,
            query.tag.join(","),
            user_agent.as_str(),
            session,
            principal.subject(),
            tenant.0,
        )))
    }
}

#[tokio::test]
async fn generated_adapter_binds_all_tae03_parts_from_a_real_request() {
    let reservation = TcpListener::bind("127.0.0.1:0").expect("reserve a loopback address");
    let address = reservation
        .local_addr()
        .expect("reserved listener has an address");
    drop(reservation);

    let app = AppBuilder::new(&address.to_string())
        .build()
        .await
        .expect("typed parts application builds");
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
    .expect("typed parts listener becomes ready");

    let target = format!(
        "/typed-parts/{UUID_VALUE}/team%252Fblue?enabled=true&count=42&tag=first&optional=7&tag=second"
    );
    let request = format!(
        "GET {target} HTTP/1.1\r\n\
Host: localhost\r\n\
User-Agent: lily-test/1.0\r\n\
Cookie: session=opaque%2Ftoken\r\n\
Connection: close\r\n\
\r\n"
    );
    let mut connection = tokio::net::TcpStream::connect(address)
        .await
        .expect("typed parts listener accepts a connection");
    connection
        .write_all(request.as_bytes())
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
    assert!(
        response.ends_with(&format!(
            "{UUID_VALUE}|team%2Fblue|true|42|Some(7)|first,second|lily-test/1.0|opaque%2Ftoken|user-42|tenant-a"
        )),
        "{response}"
    );

    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("typed parts server shutdown is bounded")
        .expect("typed parts server task does not panic")
        .expect("typed parts server shuts down cleanly");
}
