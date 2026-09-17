use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures_util::FutureExt;
use lily_injection::Injectable;
use lily_injection::{InjectionError, ServiceTrait, async_trait::async_trait};
use lily_trace::{
    ExportConfig, FileExportConfig, TraceConfig, TracingRuntimeStatus, tracing_runtime_status,
};
use lily_websocket::{ServerConfig, WsAppBuilder};
use opentelemetry::trace::TraceContextExt;
use tokio::sync::Semaphore;

#[path = "../../../integrations/lily_trace/tests/support/collector.rs"]
mod collector;

static EAGER_INITIALIZER_ENTERED: Semaphore = Semaphore::const_new(0);
static EAGER_DISPOSER_ENTERED: Semaphore = Semaphore::const_new(0);
static EAGER_DISPOSER_DROPPED: Semaphore = Semaphore::const_new(0);
static ALL_SERVICES_DROPPED: Semaphore = Semaphore::const_new(0);

static ROOT_DISPOSE_CALLS: AtomicUsize = AtomicUsize::new(0);
static EAGER_DISPOSE_CALLS: AtomicUsize = AtomicUsize::new(0);
static SERVICE_DROP_CALLS: AtomicUsize = AtomicUsize::new(0);
static IDENTITIES: std::sync::Mutex<Vec<(&str, String, String)>> =
    std::sync::Mutex::new(Vec::new());

fn capture_identity(operation: &'static str) {
    let cx = lily_trace::current_context();
    let span = cx.span();
    let id = span.span_context();
    assert!(id.is_valid());
    IDENTITIES.lock().unwrap().push((
        operation,
        id.trace_id().to_string(),
        id.span_id().to_string(),
    ));
}

async fn wait_for(signal: &'static Semaphore) {
    signal
        .acquire()
        .await
        .expect("test lifecycle semaphore must remain open")
        .forget();
}

fn record_service_drop() {
    if SERVICE_DROP_CALLS.fetch_add(1, Ordering::SeqCst) == 1 {
        ALL_SERVICES_DROPPED.add_permits(1);
    }
}

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
struct DeadlineRootService;

#[async_trait]
impl ServiceTrait for DeadlineRootService {
    async fn dispose(&self) -> Result<(), InjectionError> {
        ROOT_DISPOSE_CALLS.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

impl Drop for DeadlineRootService {
    fn drop(&mut self) {
        record_service_drop();
    }
}

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
struct DeadlineEagerService {
    #[inject]
    _root: Arc<DeadlineRootService>,
}

struct EagerDisposerDropGuard;

impl Drop for EagerDisposerDropGuard {
    fn drop(&mut self) {
        tracing::info!(probe = "eager_disposer_dropped");
        EAGER_DISPOSER_DROPPED.add_permits(1);
    }
}

#[async_trait]
impl ServiceTrait for DeadlineEagerService {
    #[lily_trace::lily_trace(name = "test.ws.build.initialize")]
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        capture_identity("test.ws.build.initialize");
        EAGER_INITIALIZER_ENTERED.add_permits(1);
        std::future::pending().await
    }

    #[lily_trace::lily_trace(name = "test.ws.build.dispose")]
    async fn dispose(&self) -> Result<(), InjectionError> {
        capture_identity("test.ws.build.dispose");
        EAGER_DISPOSE_CALLS.fetch_add(1, Ordering::SeqCst);
        EAGER_DISPOSER_ENTERED.add_permits(1);
        let _drop_guard = EagerDisposerDropGuard;
        std::future::pending().await
    }
}

impl Drop for DeadlineEagerService {
    fn drop(&mut self) {
        record_service_drop();
    }
}

#[tokio::test(start_paused = true)]
async fn in_progress_di_build_rollback_deadline_allows_owned_tracing_to_reach_shutdown() {
    let live_export = std::env::var("LILY_WS_BUILD_OTLP").is_ok();
    // Tonic connects eagerly. Never advance its real network timers with the
    // paused clock used by the deterministic, file-only deadline qualification.
    if live_export {
        tokio::time::resume();
    }
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("build-deadline.jsonl");
    let rollback_timeout = lily_injection::__private::configured_build_rollback_timeout()
        .expect("valid build rollback timeout environment");
    let trace = TraceConfig {
        enabled: true,
        service_name: "websocket-build-deadline-test".to_owned(),
        export: ExportConfig {
            console: false,
            file: std::env::var("LILY_WS_BUILD_OTLP")
                .is_err()
                .then(|| FileExportConfig {
                    path: path.display().to_string(),
                    rotation: lily_trace::FileRotation::Daily,
                    max_file_bytes: 4 * 1024 * 1024,
                    max_files: 2,
                    buffered_lines: 1024,
                    max_record_bytes: 16384,
                }),
            otlp: std::env::var("LILY_WS_BUILD_OTLP").ok().map(|endpoint| {
                lily_trace::OtlpExportConfig {
                    endpoint,
                    flush_interval_millis: 60_000,
                    metrics_export_interval_millis: 300_000,
                    max_queue_items: 4096,
                    max_queue_bytes: 8 * 1024 * 1024,
                    protocol: "grpc".into(),
                    headers: Default::default(),
                    timeout: 5,
                    max_batch_size: 512,
                    max_export_retries: 1,
                    retry_backoff_millis: 25,
                }
            }),
        },
        ..TraceConfig::default()
    };

    let builder = WsAppBuilder::new("127.0.0.1:0")
        .config(ServerConfig::default())
        .tracing_config(trace);
    let build = tokio::spawn(async move { builder.build().await });

    wait_for(&EAGER_INITIALIZER_ENTERED).await;
    assert_eq!(tracing_runtime_status(), TracingRuntimeStatus::Initialized);
    let aborted_at = tokio::time::Instant::now();
    build.abort();
    assert!(matches!(build.await, Err(error) if error.is_cancelled()));
    wait_for(&EAGER_DISPOSER_ENTERED).await;

    if !live_export {
        tokio::time::advance(rollback_timeout - std::time::Duration::from_millis(1)).await;
        assert_eq!(tracing_runtime_status(), TracingRuntimeStatus::Initialized);
        assert_eq!(ROOT_DISPOSE_CALLS.load(Ordering::SeqCst), 0);
        tokio::time::advance(std::time::Duration::from_millis(1)).await;
        tokio::time::resume();
    }
    tokio::time::timeout(
        rollback_timeout + std::time::Duration::from_secs(5),
        async {
            wait_for(&EAGER_DISPOSER_DROPPED).await;
            wait_for(&ALL_SERVICES_DROPPED).await;
        },
    )
    .await
    .unwrap();
    assert!(
        aborted_at.elapsed() >= rollback_timeout,
        "pending disposer must survive until the actual rollback deadline"
    );
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let workers = lily_trace::TracingShutdownEvidence::default().workers();
            if tracing_runtime_status() == TracingRuntimeStatus::Shutdown
                && workers.registered > 0
                && workers.is_terminal()
            {
                assert_eq!(
                    workers.failed, 0,
                    "every exporter/provider worker must actually join"
                );
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    assert_eq!(tracing_runtime_status(), TracingRuntimeStatus::Shutdown);
    assert_eq!(EAGER_DISPOSE_CALLS.load(Ordering::SeqCst), 1);
    assert_eq!(ROOT_DISPOSE_CALLS.load(Ordering::SeqCst), 0);
    assert_eq!(SERVICE_DROP_CALLS.load(Ordering::SeqCst), 2);
    if let Ok(report) = std::env::var("LILY_WS_BUILD_REPORT") {
        let identities = IDENTITIES.lock().unwrap();
        assert_eq!(identities.len(), 2);
        std::fs::write(
            std::path::Path::new(&report).join("build-identities.json"),
            serde_json::to_vec(&*identities).unwrap(),
        )
        .unwrap();
        return;
    }
    let records: Vec<serde_json::Value> = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let identities = IDENTITIES.lock().unwrap();
    assert_eq!(identities.len(), 2);
    for (operation, trace, span) in identities.iter() {
        let events: Vec<_> = records
            .iter()
            .filter(|record| record["fields"]["lily.operation"] == *operation)
            .collect();
        assert_eq!(
            events.len(),
            2,
            "exact start/drop pair must be flushed after rollback abort"
        );
        for event in &events {
            assert_eq!(event["trace_id"], *trace);
            assert_eq!(event["span_id"], *span);
            assert_eq!(event["trace_flags"], "01");
        }
        assert_eq!(events[0]["fields"]["lily.lifecycle"], "started");
        assert_eq!(events[1]["fields"]["lily.lifecycle"], "dropped");
        assert!(
            events[1]["fields"]["lily.duration_ms"]
                .as_f64()
                .unwrap()
                .is_finite()
        );
        assert_eq!(events[1]["span"]["lily.lifecycle"], "dropped");
    }
    let dropped: Vec<_> = records
        .iter()
        .filter(|r| r["fields"]["probe"] == "eager_disposer_dropped")
        .collect();
    assert_eq!(dropped.len(), 1);
    let disposal = identities
        .iter()
        .find(|(operation, _, _)| *operation == "test.ws.build.dispose")
        .unwrap();
    assert_eq!(dropped[0]["trace_id"], disposal.1);
    assert_eq!(dropped[0]["span_id"], disposal.2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker and the pinned Collector; verifies rollback deadline export"]
async fn build_deadline_flushes_terminal_spans_logs_and_metrics_to_collector() {
    let mut collector = collector::Collector::new();
    let result = std::panic::AssertUnwindSafe(async {
        let endpoint = collector.start().await.unwrap();
        let output = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            tokio::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "in_progress_di_build_rollback_deadline_allows_owned_tracing_to_reach_shutdown",
                    "--nocapture",
                ])
                .env("LILY_WS_BUILD_OTLP", endpoint)
                .env("LILY_WS_BUILD_REPORT", &collector.report)
                .env("LILY_INJECTION_BUILD_ROLLBACK_TIMEOUT_SECS", "1")
                .kill_on_drop(true)
                .output(),
        )
        .await
        .unwrap()
        .unwrap();
        std::fs::write(
            collector.report.join("build-child.log"),
            [&output.stdout[..], &output.stderr[..]].concat(),
        )
        .unwrap();
        assert!(
            output.status.success(),
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        collector.stop_and_capture().await.unwrap();
        let read = |file: &str, resource: &str, scope: &str, field: &str| {
            let text =
                std::fs::read_to_string(collector.report.join("capture").join(file)).unwrap();
            let mut records = Vec::new();
            for batch in serde_json::Deserializer::from_str(&text).into_iter::<serde_json::Value>()
            {
                for resource in batch.unwrap()[resource].as_array().unwrap() {
                    for scope in resource[scope].as_array().unwrap() {
                        records.extend(scope[field].as_array().unwrap().iter().cloned());
                    }
                }
            }
            records
        };
        let spans = read("traces.jsonl", "resourceSpans", "scopeSpans", "spans");
        let logs = read("logs.jsonl", "resourceLogs", "scopeLogs", "logRecords");
        let metrics = read(
            "metrics.jsonl",
            "resourceMetrics",
            "scopeMetrics",
            "metrics",
        );
        let identities: Vec<(String, String, String)> = serde_json::from_slice(
            &std::fs::read(collector.report.join("build-identities.json")).unwrap(),
        )
        .unwrap();
        let attribute = |record: &serde_json::Value, name: &str| {
            let attrs = record["attributes"].as_array().unwrap();
            let mut keys = std::collections::HashSet::new();
            for kv in attrs {
                assert!(
                    keys.insert(kv["key"].as_str().unwrap()),
                    "duplicate raw attribute"
                );
            }
            attrs
                .iter()
                .find(|kv| kv["key"] == name)
                .map(|kv| kv["value"].clone())
        };
        assert_eq!(identities.len(), 2);
        assert_eq!(
            spans
                .iter()
                .filter(|s| s["name"].as_str().unwrap().starts_with("test.ws.build."))
                .count(),
            2
        );
        for (operation, trace, id) in identities {
            let matching: Vec<_> = spans.iter().filter(|s| s["name"] == operation).collect();
            assert_eq!(matching.len(), 1);
            let span = matching[0];
            assert_eq!(span["traceId"], trace);
            assert_eq!(span["spanId"], id);
            assert_eq!(
                attribute(span, "lily.lifecycle"),
                Some(serde_json::json!({"stringValue":"dropped"}))
            );
            assert!(attribute(span, "lily.duration_ms").unwrap()["doubleValue"].is_number());
            for key in [
                "droppedAttributesCount",
                "droppedEventsCount",
                "droppedLinksCount",
            ] {
                assert_eq!(span[key].as_u64().unwrap_or(0), 0);
            }
            let events: Vec<_> = logs
                .iter()
                .filter(|l| {
                    attribute(l, "lily.operation")
                        == Some(serde_json::json!({"stringValue":operation}))
                })
                .collect();
            assert_eq!(events.len(), 2);
            let mut phases = std::collections::HashSet::new();
            for event in events {
                assert_eq!(event["traceId"], trace);
                assert_eq!(event["spanId"], id);
                assert_eq!(event["droppedAttributesCount"].as_u64().unwrap_or(0), 0);
                assert!(
                    phases.insert(
                        attribute(event, "lily.lifecycle").unwrap()["stringValue"]
                            .as_str()
                            .unwrap()
                            .to_owned()
                    )
                );
            }
            assert_eq!(
                phases,
                std::collections::HashSet::from(["started".into(), "dropped".into()])
            );
        }
        let dropped: Vec<_> = logs
            .iter()
            .filter(|l| {
                attribute(l, "probe")
                    == Some(serde_json::json!({"stringValue":"eager_disposer_dropped"}))
            })
            .collect();
        assert_eq!(dropped.len(), 1);
        let counters: Vec<_> = metrics
            .iter()
            .filter(|m| m["name"] == "di.service.resolutions.total")
            .collect();
        assert!(
            !counters.is_empty(),
            "DI build metrics must flush after the rollback deadline"
        );
        for counter in counters {
            let points = counter["sum"]["dataPoints"].as_array().unwrap();
            assert_eq!(points.len(), 1);
            assert_eq!(points[0]["asInt"], "1");
        }
    })
    .catch_unwind()
    .await;
    let cleanup = collector.cleanup().await;
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
    cleanup.unwrap();
}
