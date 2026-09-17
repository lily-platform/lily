//! Real message deadlines and peer disconnection cannot cancel a host worker.
use futures_util::{SinkExt, StreamExt};
use lily_websocket::{
    ApplicationScopeFactory, BackgroundCancellation, BackgroundServiceTrait, Emit,
    ExecutionCancellation, Extensions, Injectable, InjectionError, ProcessContext, ServerConfig,
    ServiceTrait, WebSocketActionError, WebSocketController, WebSocketControllerInitError,
    WebSocketControllerTrait, WsAppBuilder, WsMessageBody, async_trait, websocket_controller,
};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};
use tokio_util::sync::CancellationToken;

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
struct Probe {
    worker_entered: CancellationToken,
    worker_stopped: CancellationToken,
    action_cancelled: CancellationToken,
    worker_signal: Mutex<Option<BackgroundCancellation>>,
    scope_ids: Mutex<Vec<u64>>,
    scopes_disposed: AtomicUsize,
}
impl ServiceTrait for Probe {}

#[derive(Default, Injectable)]
#[service(lifetime = "Scoped")]
struct Scoped {
    #[inject]
    probe: Arc<Probe>,
    id: u64,
}
#[async_trait]
impl ServiceTrait for Scoped {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        self.id = ProcessContext::current().unwrap().process_id;
        self.probe.scope_ids.lock().unwrap().push(self.id);
        Ok(())
    }
    async fn dispose(&self) -> Result<(), InjectionError> {
        assert_eq!(self.id, ProcessContext::current().unwrap().process_id);
        self.probe.scopes_disposed.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
}
struct Worker {
    scopes: Arc<ApplicationScopeFactory>,
}
#[async_trait]
impl BackgroundServiceTrait for Worker {
    type Error = InjectionError;
    async fn new(scopes: Arc<ApplicationScopeFactory>) -> Result<Self, Self::Error> {
        Ok(Self { scopes })
    }
    async fn execute_async(&mut self, stopping: BackgroundCancellation) -> Result<(), Self::Error> {
        self.scopes
            .create_scope(ProcessContext::new())?
            .run(|extensions| {
                Box::pin(async move {
                    let scoped = extensions.get_service::<Scoped>(None).await?;
                    *scoped.probe.worker_signal.lock().unwrap() = Some(stopping.clone());
                    scoped.probe.worker_entered.cancel();
                    stopping.cancelled().await;
                    scoped.probe.worker_stopped.cancel();
                    Ok::<_, InjectionError>(())
                })
            })
            .await
    }
}

#[derive(WebSocketController)]
#[namespace("background")]
struct Controller {
    extensions: Arc<Extensions>,
}
#[async_trait]
impl WebSocketControllerTrait for Controller {
    async fn new(extensions: Arc<Extensions>) -> Result<Self, WebSocketControllerInitError> {
        Ok(Self { extensions })
    }
}
#[websocket_controller]
impl Controller {
    #[message("deadline")]
    async fn deadline(
        &self,
        cancellation: ExecutionCancellation,
    ) -> Result<Emit<bool>, WebSocketActionError> {
        let scoped = self.extensions.get_service::<Scoped>(None).await.unwrap();
        cancellation.cancelled().await;
        scoped.probe.action_cancelled.cancel();
        Ok(Emit::new("background:cooperated", true)?)
    }
    #[message("ping")]
    async fn ping(&self) -> Result<Emit<bool>, WebSocketActionError> {
        Ok(Emit::new("background:pong", true)?)
    }
}

async fn bounded<T>(work: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), work)
        .await
        .expect("background isolation deadline")
}

#[tokio::test]
async fn message_timeout_and_peer_close_leave_worker_running_until_host_shutdown() {
    let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = reservation.local_addr().unwrap();
    let app = Arc::new(
        WsAppBuilder::new(&address.to_string())
            .config(ServerConfig {
                message_timeout_secs: 1,
                allowed_origins: vec!["https://background.test".into()],
                ..Default::default()
            })
            .add_background_service::<Worker>()
            .build()
            .await
            .unwrap(),
    );
    drop(reservation);
    let probe = app.container().resolve::<Probe>(None).await.unwrap();
    let server_app = app.clone();
    let server = tokio::spawn(async move { server_app.start().await });
    bounded(probe.worker_entered.cancelled()).await;
    bounded(async {
        while !app.health_snapshot().unwrap().ready {
            assert!(!server.is_finished());
            tokio::task::yield_now().await;
        }
    })
    .await;
    let mut request = format!("ws://{address}/ws?namespace=background")
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("origin", "https://background.test".parse().unwrap());
    let (mut socket, response) = bounded(tokio_tungstenite::connect_async(request))
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 101);
    for (event, expected) in [
        ("deadline", "background:cooperated"),
        ("ping", "background:pong"),
    ] {
        let message = WsMessageBody::try_new(format!("background:{event}"), ())
            .unwrap()
            .with_namespace("background".into())
            .to_message()
            .unwrap();
        socket.send(message).await.unwrap();
        let message = bounded(async {
            loop {
                match socket.next().await.unwrap().unwrap() {
                    Message::Text(text) => break text,
                    Message::Ping(bytes) => socket.send(Message::Pong(bytes)).await.unwrap(),
                    other => panic!("unexpected frame: {other:?}"),
                }
            }
        })
        .await;
        let message: serde_json::Value = serde_json::from_str(&message).unwrap();
        assert_eq!(message["event"], expected);
    }
    assert!(
        probe.action_cancelled.is_cancelled(),
        "real message deadline must fire"
    );
    assert!(
        !probe
            .worker_signal
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .is_cancelled()
    );
    assert!(!probe.worker_stopped.is_cancelled());
    assert_eq!(probe.scopes_disposed.load(Ordering::Acquire), 1);
    assert_eq!(app.container().active_scope_count(), 1);
    socket.close(None).await.unwrap();
    let close = bounded(socket.next()).await.unwrap().unwrap();
    assert!(matches!(close, Message::Close(_)));
    drop(socket);
    bounded(async {
        while app.active_connection_count().await != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(app.health_snapshot().unwrap().ready);
    assert!(
        !probe
            .worker_signal
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .is_cancelled()
    );
    assert_eq!(app.container().active_scope_count(), 1);
    let ids = probe.scope_ids.lock().unwrap().clone();
    assert_eq!(ids.len(), 2);
    assert_ne!(
        ids[0], ids[1],
        "worker and message must resolve independent scopes"
    );
    bounded(app.close()).await.unwrap();
    bounded(server).await.unwrap().unwrap();
    assert!(probe.worker_stopped.is_cancelled());
    assert_eq!(probe.scopes_disposed.load(Ordering::Acquire), 2);
    assert_eq!(app.container().active_scope_count(), 0);
}
