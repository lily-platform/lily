//! Propagation through the real Upgrade, identity, message and DI cleanup paths.
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use lily_injection::Injectable;
use lily_injection::{InjectionError, ServiceTrait};
use lily_websocket::{
    Emit, Extensions, WebSocketActionError, WebSocketController, WebSocketControllerInitError,
    WebSocketControllerTrait, WsMessageBody,
    middleware::{
        MiddlewareDescriptor, MiddlewareKind, WebSocketIdentity, WebSocketIdentityMiddleware,
        WsHandshakeExchange, WsHandshakeRejection, WsMiddlewareInitError,
    },
};
use opentelemetry::trace::{SpanContext, TraceContextExt};
use serde_json::json;
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};

pub(crate) static IDENTITIES: Mutex<Vec<(u8, SpanContext)>> = Mutex::new(Vec::new());
pub(crate) static DISPOSALS: Mutex<Vec<SpanContext>> = Mutex::new(Vec::new());

#[derive(Default, Injectable)]
#[service(lifetime = "Scoped")]
struct Resource;
#[async_trait::async_trait]
impl ServiceTrait for Resource {
    #[lily_trace::lily_trace(name = "test.ws.resource.dispose")]
    async fn dispose(&self) -> Result<(), InjectionError> {
        DISPOSALS
            .lock()
            .unwrap()
            .push(lily_trace::current_context().span().span_context().clone());
        tracing::info!(probe = "ws_resource_disposed");
        Ok(())
    }
}

pub(crate) struct Identity;
#[async_trait::async_trait]
impl WebSocketIdentityMiddleware for Identity {
    async fn new(_: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
        Ok(Self)
    }
    fn descriptor(&self) -> MiddlewareDescriptor {
        MiddlewareDescriptor::new("test_identity", MiddlewareKind::WebSocketHandshake)
    }
    #[lily_trace::lily_trace(name = "test.ws.identity")]
    async fn identify(
        &self,
        exchange: &mut WsHandshakeExchange,
        _: lily_websocket::ExecutionCancellation,
    ) -> Result<WebSocketIdentity, WsHandshakeRejection> {
        let case: u8 = exchange
            .request()
            .headers()
            .get_custom_header("x-case")
            .unwrap()
            .parse()
            .unwrap();
        IDENTITIES.lock().unwrap().push((
            case,
            lily_trace::current_context().span().span_context().clone(),
        ));
        drop(exchange.service::<Resource>().await.unwrap());
        tokio::task::yield_now().await;
        if case == 6 {
            return Err(WsHandshakeRejection::forbidden(
                lily_websocket::middleware::MiddlewareErrorCode::new("TEST_IDENTITY_REJECTED")
                    .unwrap(),
            ));
        }
        Ok(WebSocketIdentity::anonymous())
    }
}

#[derive(WebSocketController)]
#[namespace("parent")]
struct Endpoints {
    extensions: Arc<Extensions>,
}
#[async_trait::async_trait]
impl WebSocketControllerTrait for Endpoints {
    async fn new(extensions: Arc<Extensions>) -> Result<Self, WebSocketControllerInitError> {
        Ok(Self { extensions })
    }
}
#[lily_websocket::websocket_controller]
impl Endpoints {
    #[connected]
    async fn connected(&self) -> Result<(), lily_websocket::WebSocketLifecycleError> {
        drop(self.extensions.get_service::<Resource>(None).await.unwrap());
        tracing::info!(probe = "controller_connected");
        Ok(())
    }
    #[disconnected]
    async fn disconnected(&self) -> Result<(), lily_websocket::WebSocketLifecycleError> {
        drop(self.extensions.get_service::<Resource>(None).await.unwrap());
        tracing::info!(probe = "controller_disconnected");
        Ok(())
    }
    #[message("check")]
    async fn check(&self) -> Result<Emit<serde_json::Value>, WebSocketActionError> {
        drop(self.extensions.get_service::<Resource>(None).await.unwrap());
        Ok(Emit::new("parent:done", json!({"ok": true}))?)
    }
}

pub(crate) async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(15), future)
        .await
        .expect("WebSocket propagation test deadline")
}

pub(crate) fn trace_id(case: u8) -> String {
    format!("4bf92f3577b34da6a3ce929d0e0e47{case:02x}")
}
pub(crate) fn parent_id(case: u8) -> String {
    format!("00f067aa0ba902{case:02x}")
}

pub(crate) async fn exercise(address: std::net::SocketAddr, case: u8) {
    let mut request = format!("ws://{address}/ws?namespace=parent")
        .into_client_request()
        .unwrap();
    request.headers_mut().insert(
        "origin",
        if case == 7 {
            "https://rejected.test"
        } else {
            "https://parent.test"
        }
        .parse()
        .unwrap(),
    );
    request
        .headers_mut()
        .insert("x-case", case.to_string().parse().unwrap());
    let flags = if case == 5 { "00" } else { "01" };
    let parent = format!("00-{}-{}-{flags}", trace_id(case), parent_id(case));
    if case != 3 {
        request.headers_mut().insert(
            "traceparent",
            if case == 4 { "invalid" } else { &parent }.parse().unwrap(),
        );
    }
    request
        .headers_mut()
        .insert("tracestate", "vendor=test".parse().unwrap());
    if case == 8 {
        request
            .headers_mut()
            .insert("sec-websocket-key", "invalid".parse().unwrap());
    }
    if case == 9 {
        request
            .headers_mut()
            .append("traceparent", parent.parse().unwrap());
    }
    let response = bounded(tokio_tungstenite::connect_async(request)).await;
    if matches!(case, 6..=9) {
        let error = response.unwrap_err();
        let tokio_tungstenite::tungstenite::Error::Http(response) = error else {
            panic!("expected HTTP rejection: {error:?}");
        };
        assert_eq!(
            response.status().as_u16(),
            if matches!(case, 6 | 7) { 403 } else { 400 }
        );
        return;
    }
    let (mut client, response) = response.unwrap();
    assert_eq!(response.status().as_u16(), 101);
    client
        .send(
            WsMessageBody::try_new(
                "parent:check",
                json!({"secret": "telemetry-parent-private-canary"}),
            )
            .unwrap()
            .with_namespace("parent".into())
            .to_message()
            .unwrap(),
        )
        .await
        .unwrap();
    let reply = bounded(async {
        loop {
            match client.next().await.unwrap().unwrap() {
                Message::Text(text) => break text,
                Message::Ping(bytes) => client.send(Message::Pong(bytes)).await.unwrap(),
                other => panic!("unexpected {other:?}"),
            }
        }
    })
    .await;
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&reply).unwrap()["event"],
        "parent:done"
    );
    if case != 10 {
        client.close(None).await.unwrap();
        bounded(async {
            while let Some(frame) = client.next().await {
                if matches!(frame, Ok(Message::Close(_))) {
                    break;
                }
            }
        })
        .await;
    }
    // Case 10 drops TCP without a Close frame; cleanup is still owned.
    drop(client);
}
