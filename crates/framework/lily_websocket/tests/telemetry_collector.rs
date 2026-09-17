//! Explicit qualification against the pinned live Collector, plus JSONL IDs.
#[path = "../../../integrations/lily_trace/tests/support/collector.rs"]
mod collector;
#[path = "support/telemetry_fixture.rs"]
mod fixture;

use fixture::{Identity, bounded, exercise, parent_id, trace_id};
use futures_util::FutureExt;
use lily_trace::{
    TraceConfig,
    runtime::{ExportConfig, FileExportConfig, OtlpExportConfig},
};
use lily_websocket::{ServerConfig, WsAppBuilder};
use serde_json::Value;
use std::{collections::HashSet, path::Path, sync::Arc};

fn records(path: &Path, resource: &str, scope: &str, field: &str) -> Vec<Value> {
    let text = std::fs::read_to_string(path).unwrap();
    assert!(!text.contains("telemetry-parent-private-canary"));
    let mut records = Vec::new();
    for batch in serde_json::Deserializer::from_str(&text).into_iter::<Value>() {
        for resource in batch.unwrap()[resource].as_array().unwrap() {
            for scope in resource[scope].as_array().unwrap() {
                records.extend(scope[field].as_array().unwrap().iter().cloned());
            }
        }
    }
    records
}

fn attribute<'a>(value: &'a Value, name: &str) -> Option<&'a Value> {
    let attributes = value["attributes"].as_array()?;
    let mut seen = HashSet::new();
    for attribute in attributes {
        assert!(
            seen.insert(attribute["key"].as_str().unwrap()),
            "duplicate raw OTLP attribute"
        );
    }
    attributes
        .iter()
        .find(|kv| kv["key"] == name)
        .map(|kv| &kv["value"])
}

async fn run_profile(profile: &str, directory: &Path, endpoint: String) {
    let file = directory.join("application.jsonl");
    let config = TraceConfig {
        enabled: true,
        level: "info".into(),
        service_name: "ws-propagation-qualification".into(),
        export: ExportConfig {
            console: false,
            file: (profile == "file").then(|| FileExportConfig {
                path: file.to_string_lossy().into_owned(),
                rotation: lily_trace::FileRotation::Daily,
                max_file_bytes: 16 * 1024 * 1024,
                max_files: 2,
                buffered_lines: 1024,
                max_record_bytes: 16_384,
            }),
            otlp: (profile == "otlp").then(|| OtlpExportConfig {
                endpoint,
                flush_interval_millis: 60_000,
                metrics_export_interval_millis: 300_000,
                max_queue_items: 8192,
                max_queue_bytes: 16 * 1024 * 1024,
                protocol: "grpc".into(),
                headers: Default::default(),
                timeout: 5,
                max_batch_size: 512,
                max_export_retries: 1,
                retry_backoff_millis: 25,
            }),
        },
        ..Default::default()
    };
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = socket.local_addr().unwrap();
    drop(socket);
    let app = Arc::new(
        WsAppBuilder::new(&address.to_string())
            .config(ServerConfig {
                allowed_origins: vec!["https://parent.test".into()],
                ..Default::default()
            })
            .identity_middleware::<Identity>()
            .tracing_config(config)
            .build()
            .await
            .unwrap(),
    );
    let running = app.clone();
    let server = tokio::spawn(async move { running.start().await });
    bounded(async {
        while !app.health_snapshot().unwrap().accepting_new_work {
            assert!(!server.is_finished());
            tokio::task::yield_now().await;
        }
    })
    .await;
    futures_util::future::join_all([1, 6, 7, 8, 10].map(|case| exercise(address, case))).await;
    bounded(app.close()).await.unwrap();
    bounded(server).await.unwrap().unwrap();
    assert_eq!(
        lily_trace::tracing_runtime_status(),
        lily_trace::TracingRuntimeStatus::Shutdown
    );

    let expected: Vec<_> = fixture::DISPOSALS
        .lock()
        .unwrap()
        .iter()
        .map(|cx| {
            serde_json::json!({
                "trace_id": cx.trace_id().to_string(), "span_id": cx.span_id().to_string(),
            })
        })
        .collect();
    assert_eq!(expected.len(), 9);
    std::fs::write(
        directory.join(format!("{profile}-expected.json")),
        serde_json::to_vec(&expected).unwrap(),
    )
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker and the pinned Collector image; runs real sockets and OTLP export"]
async fn collector_and_jsonl_preserve_upgrade_and_cleanup_parentage() {
    if let Ok(profile) = std::env::var("LILY_WS_TELEMETRY_CHILD") {
        assert!(matches!(profile.as_str(), "otlp" | "file"));
        run_profile(
            &profile,
            Path::new(&std::env::var("LILY_WS_TELEMETRY_REPORT").unwrap()),
            std::env::var("LILY_WS_TELEMETRY_ENDPOINT").unwrap(),
        )
        .await;
        return;
    }
    let mut collector = collector::Collector::new();
    let result = std::panic::AssertUnwindSafe(async {
        let endpoint = collector.start().await.unwrap();
        let file = collector.report.join("application.jsonl");
        for profile in ["otlp", "file"] {
            let output = bounded(
                tokio::process::Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--ignored",
                        "--exact",
                        "collector_and_jsonl_preserve_upgrade_and_cleanup_parentage",
                        "--nocapture",
                    ])
                    .env("LILY_WS_TELEMETRY_CHILD", profile)
                    .env("LILY_WS_TELEMETRY_REPORT", &collector.report)
                    .env("LILY_WS_TELEMETRY_ENDPOINT", &endpoint)
                    .kill_on_drop(true)
                    .output(),
            )
            .await
            .unwrap();
            std::fs::write(
                collector.report.join(format!("{profile}-stdout.log")),
                &output.stdout,
            )
            .unwrap();
            std::fs::write(
                collector.report.join(format!("{profile}-stderr.log")),
                &output.stderr,
            )
            .unwrap();
            assert!(
                output.status.success(),
                "{profile} subprocess failed: {}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        collector.stop_and_capture().await.unwrap();
        let capture = collector.report.join("capture");
        let spans = records(
            &capture.join("traces.jsonl"),
            "resourceSpans",
            "scopeSpans",
            "spans",
        );
        let logs = records(
            &capture.join("logs.jsonl"),
            "resourceLogs",
            "scopeLogs",
            "logRecords",
        );
        let file_source = std::fs::read_to_string(&file).unwrap();
        assert!(!file_source.contains("telemetry-parent-private-canary"));
        let file_logs: Vec<Value> = file_source
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(
            spans
                .iter()
                .filter(|s| s["name"] == "websocket.connection")
                .count(),
            5
        );
        assert_eq!(
            spans
                .iter()
                .filter(|s| s["name"] == "test.ws.resource.dispose")
                .count(),
            9
        );
        for profile in ["otlp", "file"] {
            let expected: Vec<Value> = serde_json::from_slice(
                &std::fs::read(collector.report.join(format!("{profile}-expected.json"))).unwrap(),
            )
            .unwrap();
            assert_eq!(expected.len(), 9);
            let mut identities = HashSet::new();
            for identity in expected {
                assert!(identities.insert((
                    identity["trace_id"].as_str().unwrap().to_owned(),
                    identity["span_id"].as_str().unwrap().to_owned()
                )));
                if profile == "otlp" {
                    assert_eq!(
                        spans
                            .iter()
                            .filter(|s| s["name"] == "test.ws.resource.dispose"
                                && s["traceId"] == identity["trace_id"]
                                && s["spanId"] == identity["span_id"])
                            .count(),
                        1
                    );
                } else {
                    let matching: Vec<_> = file_logs
                        .iter()
                        .filter(|log| {
                            log["fields"]["probe"] == "ws_resource_disposed"
                                && log["span_id"] == identity["span_id"]
                        })
                        .collect();
                    assert_eq!(
                        matching.len(),
                        1,
                        "JSONL must preserve the context captured during dispose"
                    );
                    assert_eq!(matching[0]["trace_id"], identity["trace_id"]);
                    assert_eq!(
                        file_logs
                            .iter()
                            .filter(|log| log["span_id"] == identity["span_id"]
                                && log["fields"]["lily.lifecycle"] == "completed")
                            .count(),
                        1
                    );
                }
            }
        }
        for case in [1, 6, 7, 8, 10] {
            let owned: Vec<_> = spans
                .iter()
                .filter(|s| s["traceId"] == trace_id(case))
                .collect();
            let one = |name: &str| {
                let matching: Vec<_> = owned.iter().filter(|s| s["name"] == name).collect();
                assert_eq!(matching.len(), 1, "case {case}: {name}");
                *matching[0]
            };
            let connection = one("websocket.connection");
            assert_eq!(connection["parentSpanId"], parent_id(case));
            let handshake = one("websocket.handshake");
            assert_eq!(handshake["parentSpanId"], connection["spanId"]);
            if matches!(case, 1 | 6 | 10) {
                assert_eq!(one("test.ws.identity")["parentSpanId"], handshake["spanId"]);
            }
            if matches!(case, 1 | 10) {
                assert_eq!(
                    one("websocket.message")["parentSpanId"],
                    connection["spanId"]
                );
                assert_eq!(
                    one("websocket.connection.cleanup")["parentSpanId"],
                    connection["spanId"]
                );
            }
            for dispose in owned
                .iter()
                .filter(|s| s["name"] == "test.ws.resource.dispose")
            {
                let cleanup: Vec<_> = owned
                    .iter()
                    .filter(|s| s["spanId"] == dispose["parentSpanId"])
                    .collect();
                assert_eq!(cleanup.len(), 1);
                assert_eq!(cleanup[0]["name"], "di.scope.dispose");
                assert_eq!(
                    owned
                        .iter()
                        .filter(|s| s["spanId"] == cleanup[0]["parentSpanId"])
                        .count(),
                    1
                );
                assert_eq!(
                    attribute(dispose, "lily.lifecycle").unwrap()["stringValue"],
                    "completed"
                );
                let events: Vec<_> = logs
                    .iter()
                    .filter(|log| {
                        log["spanId"] == dispose["spanId"]
                            && attribute(log, "probe")
                                .is_some_and(|v| v["stringValue"] == "ws_resource_disposed")
                    })
                    .collect();
                assert_eq!(events.len(), 1);
                assert_eq!(events[0]["traceId"], dispose["traceId"]);
            }
        }
    })
    .catch_unwind()
    .await;
    collector.cleanup().await.unwrap();
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}
