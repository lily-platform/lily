#![no_main]

use std::{convert::Infallible, sync::OnceLock, time::Duration};

use bytes::Bytes;
use http_body_util::Full;
use hyper::{service::service_fn, Response};
use hyper_util::rt::TokioIo;
use libfuzzer_sys::fuzz_target;
use lily_http_api::{__private::HttpServer, HttpProtocol, HttpTransportConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const MAX_FUZZ_INPUT_BYTES: usize = 64 * 1024;

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("fuzz runtime must build")
    })
}

fuzz_target!(|input: &[u8]| {
    if input.len() > MAX_FUZZ_INPUT_BYTES {
        return;
    }

    runtime().block_on(async {
        let (mut client, server) = tokio::io::duplex(MAX_FUZZ_INPUT_BYTES * 2);
        let mut server_task = tokio::spawn(async move {
            let builder =
                HttpServer::connection_builder(&HttpTransportConfig::default(), HttpProtocol::Auto);
            let service = service_fn(|_request| async {
                Ok::<_, Infallible>(
                    Response::builder()
                        .status(204)
                        .body(Full::new(Bytes::new()))
                        .expect("static fuzz response must be valid"),
                )
            });
            builder
                .serve_connection(TokioIo::new(server), service)
                .await
        });

        let _ = tokio::time::timeout(Duration::from_millis(50), client.write_all(input)).await;
        let _ = tokio::time::timeout(Duration::from_millis(20), client.shutdown()).await;
        let mut response = vec![0_u8; MAX_FUZZ_INPUT_BYTES];
        let _ = tokio::time::timeout(Duration::from_millis(20), client.read(&mut response)).await;
        drop(client);

        match tokio::time::timeout(Duration::from_millis(100), &mut server_task).await {
            Ok(Err(join_error)) if join_error.is_panic() => {
                std::panic::resume_unwind(join_error.into_panic());
            }
            Ok(_) => {}
            Err(_) => {
                server_task.abort();
                match server_task.await {
                    Err(join_error) if join_error.is_panic() => {
                        std::panic::resume_unwind(join_error.into_panic());
                    }
                    Ok(_) | Err(_) => {}
                }
            }
        }
    });
});
