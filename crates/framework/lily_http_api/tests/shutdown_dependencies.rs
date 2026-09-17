//! Isolated process: the tracing runtime is process-global and cannot be
//! initialized in the shared HTTP unit-test binary.

use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper_util::rt::TokioIo;
use lily_http_api::{
    AppBuilder, CancellationToken, ExecutionCancellation, Extensions, HttpExchange, HttpMiddleware,
    HttpMiddlewareError, HttpMiddlewareInitError, HttpNext, MiddlewareDescriptor, MiddlewareKind,
};
use lily_injection::Injectable;
use lily_injection::ServiceTrait;
use lily_trace::runtime::{ExportConfig, FileExportConfig, FileRotation};
use lily_trace::TraceConfig;
use opentelemetry::trace::TraceContextExt;

const TRACE_ID: &str = "4bf92f3577b34da6a3ce929d0e0e4736";
const PARENT_ID: &str = "00f067aa0ba902b7";
static DISPOSE_ID: std::sync::Mutex<Option<(String, String)>> = std::sync::Mutex::new(None);

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
struct TelemetryUser {
    entered: CancellationToken,
    release: CancellationToken,
    scope_disposed: CancellationToken,
}

#[async_trait::async_trait]
impl ServiceTrait for TelemetryUser {
    async fn dispose(&self) -> Result<(), lily_injection::InjectionError> {
        assert!(self.scope_disposed.is_cancelled());
        tracing::info!(
            phase7 = "dependency-disposed",
            "dependency cleanup before telemetry"
        );
        Ok(())
    }
}

#[derive(Default, Injectable)]
#[service(lifetime = "Scoped")]
struct ScopedTelemetryUser {
    #[inject]
    root: Arc<TelemetryUser>,
}

struct ScopeDisposalFinished<'a>(&'a CancellationToken);
impl Drop for ScopeDisposalFinished<'_> {
    fn drop(&mut self) {
        self.0.cancel();
        tracing::info!(phase9 = "scope-disposal-ended");
    }
}

#[async_trait::async_trait]
impl ServiceTrait for ScopedTelemetryUser {
    #[lily_trace::lily_trace(name = "test.http.scope.dispose")]
    async fn dispose(&self) -> Result<(), lily_injection::InjectionError> {
        let cx = lily_trace::current_context();
        let span = cx.span();
        let id = span.span_context();
        assert!(DISPOSE_ID
            .lock()
            .unwrap()
            .replace((id.trace_id().to_string(), id.span_id().to_string()))
            .is_none());
        let _finished = ScopeDisposalFinished(&self.root.scope_disposed);
        tracing::info!(
            phase9 = "scope-disposed",
            "request scope before application dependency"
        );
        if std::env::var("LILY_HTTP_SHUTDOWN_CASE").as_deref() == Ok("dispose-timeout") {
            std::future::pending().await
        } else if std::env::var("LILY_HTTP_SHUTDOWN_CASE").as_deref() == Ok("dispose-error") {
            Err(lily_injection::InjectionError::DisposeError(
                "fixture scoped disposal failed".into(),
            ))
        } else {
            Ok(())
        }
    }
}

struct ActiveRequest(Arc<Extensions>);
#[async_trait::async_trait]
impl HttpMiddleware for ActiveRequest {
    async fn new(extensions: Arc<Extensions>) -> Result<Self, HttpMiddlewareInitError> {
        Ok(Self(extensions))
    }
    fn descriptor(&self) -> MiddlewareDescriptor {
        MiddlewareDescriptor::new("telemetry-qualification", MiddlewareKind::Custom)
    }
    async fn handle(
        &self,
        exchange: &mut HttpExchange<'_>,
        next: HttpNext<'_>,
        cancellation: ExecutionCancellation,
    ) -> Result<(), HttpMiddlewareError> {
        let scoped = self
            .0
            .get_service::<ScopedTelemetryUser>(None)
            .await
            .unwrap();
        scoped.root.entered.cancel();
        scoped.root.release.cancelled().await;
        assert!(!cancellation.is_cancelled());
        next.run(exchange).await
    }
}

async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .unwrap()
}

#[tokio::test]
async fn owned_di_final_event_is_flushed_before_http_reports_terminal() {
    let Ok(case) = std::env::var("LILY_HTTP_SHUTDOWN_CASE") else {
        for case in ["normal", "dispose-error", "dispose-timeout"] {
            let output = bounded(
                tokio::process::Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "owned_di_final_event_is_flushed_before_http_reports_terminal",
                        "--nocapture",
                    ])
                    .env("LILY_HTTP_SHUTDOWN_CASE", case)
                    .env("LILY__LIFECYCLE__SHUTDOWN_TIMEOUT_SECS", "2")
                    .kill_on_drop(true)
                    .output(),
            )
            .await
            .unwrap();
            assert!(
                output.status.success(),
                "{case}: {}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        return;
    };
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("http-shutdown.jsonl");
    let config = TraceConfig {
        enabled: true,
        export: ExportConfig {
            console: false,
            file: Some(FileExportConfig {
                path: path.display().to_string(),
                rotation: FileRotation::Daily,
                max_file_bytes: 1024 * 1024,
                max_files: 2,
                buffered_lines: 64,
                max_record_bytes: 16 * 1024,
            }),
            ..Default::default()
        },
        ..Default::default()
    };
    let app = AppBuilder::new("127.0.0.1:0")
        .tracing_config(config)
        .middleware::<ActiveRequest>()
        .build()
        .await
        .unwrap();
    let health = app
        .extensions()
        .get_service::<lily_http_api::HttpHealthService>(None)
        .await
        .unwrap();
    let probe = app
        .container()
        .resolve::<TelemetryUser>(None)
        .await
        .unwrap();
    let runtime = app.clone();
    let root = tokio::spawn(async move {
        runtime
            .start_with_cancellation(CancellationToken::new())
            .await
    });
    bounded(async {
        while app.bound_address().is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let socket = tokio::net::TcpStream::connect(app.bound_address().unwrap())
        .await
        .unwrap();
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(socket))
        .await
        .unwrap();
    let client = tokio::spawn(connection);
    let request = tokio::spawn(async move {
        let response = sender
            .send_request(
                hyper::Request::builder()
                    .uri("/phase9")
                    .header("host", "lily.test")
                    .header("traceparent", format!("00-{TRACE_ID}-{PARENT_ID}-01"))
                    .body(Full::new(Bytes::new()))
                    .unwrap(),
            )
            .await
            .unwrap();
        response.into_body().collect().await.unwrap();
    });
    bounded(probe.entered.cancelled()).await;
    let closing = app.close();
    tokio::pin!(closing);
    assert!(futures::poll!(&mut closing).is_pending());
    assert!(!probe.scope_disposed.is_cancelled());
    probe.release.cancel();
    let close_result = bounded(closing).await;
    let root_result = bounded(root).await.unwrap();
    assert_eq!(close_result.is_err(), case != "normal", "{close_result:?}");
    assert_eq!(root_result.is_err(), case != "normal", "{root_result:?}");
    bounded(request).await.unwrap();
    bounded(client).await.unwrap().unwrap();
    let snapshot = health.snapshot().unwrap();
    for (name, expected) in [
        (
            "http.shutdown",
            if case != "normal" {
                "terminal_failed"
            } else {
                "graceful_completed"
            },
        ),
        (
            "di.container",
            if case != "normal" {
                "disposal_failed"
            } else {
                "disposed"
            },
        ),
        ("telemetry.export", "shutdown_completed"),
    ] {
        assert_eq!(
            snapshot
                .checks
                .iter()
                .find(|check| check.name == name)
                .unwrap()
                .reason_code,
            expected
        );
    }
    let bytes = std::fs::read_to_string(&path).unwrap();
    let records: Vec<serde_json::Value> = bytes
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let exactly = |field: &str, value: &str| {
        let matching: Vec<_> = records
            .iter()
            .enumerate()
            .filter(|(_, r)| r["fields"][field] == value)
            .collect();
        assert_eq!(matching.len(), 1, "exactly one {field}={value}");
        matching[0].0
    };
    let scope = exactly("phase9", "scope-disposed");
    let scope_finished = exactly("phase9", "scope-disposal-ended");
    let dependency = exactly("phase7", "dependency-disposed");
    let checkpoint = exactly("checkpoint", "before_telemetry");
    assert!(scope < scope_finished && scope_finished < dependency && dependency < checkpoint);
    assert_eq!(
        records[checkpoint]["fields"]["completion"], "incomplete",
        "the exported checkpoint must not claim the root has already joined"
    );
    let terminal = exactly("lily.event", "http.server.terminal");
    let (trace_id, span_id) = DISPOSE_ID.lock().unwrap().clone().unwrap();
    assert_eq!(trace_id, TRACE_ID);
    assert_eq!(records[scope]["trace_id"], trace_id);
    assert_eq!(records[scope]["span_id"], span_id);
    assert_eq!(records[scope_finished]["trace_id"], trace_id);
    assert_eq!(records[scope_finished]["span_id"], span_id);
    assert_eq!(records[terminal]["trace_id"], trace_id);
    assert_ne!(records[terminal]["span_id"], span_id);
    assert_ne!(records[terminal]["span_id"], PARENT_ID);
    let cleanup: Vec<_> = records
        .iter()
        .filter(|r| {
            r["span_id"] == span_id && r["fields"]["lily.operation"] == "test.http.scope.dispose"
        })
        .collect();
    assert_eq!(cleanup.len(), 2);
    assert_eq!(cleanup[0]["fields"]["lily.lifecycle"], "started");
    assert_eq!(
        cleanup[1]["fields"]["lily.lifecycle"],
        if case == "dispose-timeout" {
            "dropped"
        } else {
            "completed"
        }
    );
    assert!(cleanup[1]["fields"]["lily.duration_ms"]
        .as_f64()
        .unwrap()
        .is_finite());
    assert_eq!(cleanup[1]["trace_id"], trace_id);
    assert_eq!(
        lily_trace::tracing_runtime_status(),
        lily_trace::TracingRuntimeStatus::Shutdown
    );
    assert_eq!(app.close().await.is_err(), case != "normal");
    assert_eq!(health.snapshot().unwrap(), snapshot);
}
