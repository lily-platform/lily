use super::*;
use crate::AppBuilder;
use http_body_util::Full;
use hyper::client::conn::http2;
use tokio::io::duplex;
use tokio::sync::mpsc;

#[tokio::test]
async fn actual_http2_workers_are_registered_in_connection_and_root_before_user_poll() {
    let app = Arc::new(AppBuilder::new("127.0.0.1:0").build().await.unwrap());
    let parent = app.task_inventory().protocol.clone();
    let local = TaskRegistry::default();
    let (entered, mut arrivals) = mpsc::unbounded_channel();
    let release = CancellationToken::new();
    let body_release = release.clone();
    let local_probe = local.clone();
    let parent_probe = parent.clone();
    let service = service_fn(move |_request: HyperRequest<Incoming>| {
        let entered = entered.clone();
        let token = body_release.clone();
        let local = local_probe.clone();
        let parent = parent_probe.clone();
        async move {
            // Runs inside Hyper's real H2 worker, after both registrations.
            assert!(local.snapshot().registered > 0);
            assert!(parent.snapshot().registered > 0);
            entered.send(()).unwrap();
            token.cancelled().await;
            Ok::<_, Infallible>(HyperResponse::new(Full::new(Bytes::from_static(b"done"))))
        }
    });
    let (client_io, server_io) = duplex(64 * 1024);
    let config = HttpTransportConfig::default();
    let builder = HttpServer::connection_builder_with_executor(
        &config,
        HttpProtocol::Http2,
        TrackedHttpExecutor {
            local: local.clone(),
            parent: parent.clone(),
            keep_alive: app.clone(),
        },
    );
    let stop = CancellationToken::new();
    let server_stop = stop.clone();
    let owned_local = local.clone();
    let server = tokio::spawn(async move {
        let _owner = ConnectionProtocolOwner { tasks: owned_local };
        HttpServer::drive_connection(
            server_io,
            builder,
            service,
            server_stop,
            ConnectionActivity::new(),
            config.connection_idle_timeout,
        )
        .await
    });
    let (mut sender, connection) = http2::handshake(TokioExecutor::new(), TokioIo::new(client_io))
        .await
        .unwrap();
    let client = tokio::spawn(connection);
    let mut requests = futures::stream::FuturesUnordered::new();
    for _ in 0..3 {
        requests.push(
            sender.send_request(
                HyperRequest::builder()
                    .uri("http://lily.test/")
                    .body(Full::new(Bytes::new()))
                    .unwrap(),
            ),
        );
    }
    let collected = tokio::spawn(async move {
        use futures::StreamExt;
        while let Some(response) = requests.next().await {
            assert_eq!(
                response
                    .unwrap()
                    .into_body()
                    .collect()
                    .await
                    .unwrap()
                    .to_bytes(),
                "done"
            );
        }
    });
    for _ in 0..3 {
        tokio::time::timeout(Duration::from_secs(5), arrivals.recv())
            .await
            .unwrap()
            .unwrap();
    }
    assert_eq!(local.snapshot().registered, 3);
    assert_eq!(parent.snapshot().outstanding, 3);
    stop.cancel();
    tokio::task::yield_now().await;
    assert!(
        !server.is_finished(),
        "graceful transport must still drain accepted H2 work"
    );
    release.cancel();
    tokio::time::timeout(Duration::from_secs(5), collected)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    local.seal();
    parent.seal();
    assert!(local.wait().await.is_terminal());
    assert_eq!(parent.wait().await.completed, 3);
    drop(sender);
    client.abort();
    let _ = client.await;
    app.close().await.unwrap();
}

#[tokio::test]
async fn protocol_panic_is_terminal_evidence_but_not_clean_shutdown() {
    use hyper::rt::Executor;
    let app = Arc::new(AppBuilder::new("127.0.0.1:0").build().await.unwrap());
    let local = TaskRegistry::default();
    let parent = app.task_inventory().protocol.clone();
    let executor = TrackedHttpExecutor {
        local: local.clone(),
        parent: parent.clone(),
        keep_alive: app.clone(),
    };
    executor.execute(async { panic!("expected protocol owner qualification panic") });
    local.seal();
    parent.seal();
    assert_eq!(local.wait().await.panicked, 1);
    assert_eq!(parent.wait().await.panicked, 1);
    assert!(app.task_inventory().transport_is_terminal());
    assert!(app.task_inventory().transport_panicked());
    assert!(app.close().await.is_err());
    // Terminal failed children do not prohibit their dependency cleanup.
    assert!(lily_injection::__private::container_shutdown_quiescent(
        app.container()
    ));
}
