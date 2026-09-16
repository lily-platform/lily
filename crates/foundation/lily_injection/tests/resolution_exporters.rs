//! Installed profiles use separate processes, matching application ownership.
#[path = "../../../integrations/lily_trace/tests/support/collector.rs"]
mod collector;

use futures::FutureExt;
use lily_injectable_derive::Injectable;
use lily_injection::{ApplicationContainer, ServiceTrait};
use lily_trace::{
    TraceConfig, TraceInstallOutcome, TracingRuntimeOwner,
    runtime::{ExportConfig, FileExportConfig, OtlpExportConfig},
};
use opentelemetry::trace::TraceContextExt;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashSet},
    path::Path,
    time::Duration,
};
use tracing::Instrument;

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
struct Singleton;
impl ServiceTrait for Singleton {}
#[derive(Default, Injectable)]
#[service(lifetime = "Scoped")]
struct Scoped;
impl ServiceTrait for Scoped {}
struct Missing;

async fn child(profile: &str, directory: &Path, endpoint: &str) {
    let debug = profile.ends_with("debug");
    let file = profile.starts_with("file");
    let config = TraceConfig {
        enabled: true,
        service_name: profile.into(),
        level: if debug { "debug" } else { "info" }.into(),
        export: ExportConfig {
            console: profile == "otlp-console-debug",
            file: file.then(|| FileExportConfig {
                path: directory
                    .join(format!("{profile}.jsonl"))
                    .display()
                    .to_string(),
                rotation: lily_trace::FileRotation::Daily,
                max_file_bytes: 4 * 1024 * 1024,
                max_files: 2,
                buffered_lines: 1024,
                max_record_bytes: 16384,
            }),
            otlp: (!file).then(|| OtlpExportConfig {
                endpoint: endpoint.into(),
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
            }),
        },
        ..Default::default()
    };
    let TraceInstallOutcome::Owned(owner) = TracingRuntimeOwner::install(&config).unwrap() else {
        panic!("owned runtime")
    };
    let container = ApplicationContainer::builder().build().await.unwrap();
    let parent = tracing::info_span!(parent: None, "test.di.owner");
    let cx = lily_trace::context_for_span(&parent);
    let cx = cx.span().span_context().clone();
    async {
        container.resolve::<Singleton>(None).await.ok().unwrap();
        assert!(matches!(
            container.resolve::<Missing>(None).await,
            Err(lily_injection::InjectionError::ServiceNotFound(_))
        ));
        assert!(matches!(
            container.resolve::<Scoped>(None).await,
            Err(lily_injection::InjectionError::ScopeRequired { .. })
        ));
    }
    .instrument(parent)
    .await;
    container.close().await.unwrap();
    let report = owner.shutdown(Duration::from_secs(10)).await;
    assert!(report.is_success(), "{report:?}");
    if file {
        assert_eq!(report.file_metrics().unwrap().total_dropped(), 0);
    } else {
        let spans = report.span_metrics.as_ref().unwrap();
        let logs = report.log_metrics.as_ref().unwrap();
        assert_eq!(
            (
                spans.accepted,
                spans.exported,
                spans.dropped,
                spans.in_flight
            ),
            (if debug { 4 } else { 1 }, if debug { 4 } else { 1 }, 0, 0)
        );
        assert!(logs.accepted >= 2);
        assert_eq!(
            (logs.exported, logs.dropped, logs.in_flight),
            (logs.accepted, 0, 0)
        );
    }
    std::fs::write(directory.join(format!("{profile}-expected.json")), serde_json::to_vec(&json!({
        "trace_id": cx.trace_id().to_string(), "span_id": cx.span_id().to_string(),
        "singleton": std::any::type_name::<Singleton>(), "missing": std::any::type_name::<Missing>(),
        "scoped": std::any::type_name::<Scoped>(),
    })).unwrap()).unwrap();
    std::fs::write(
        directory.join(format!("{profile}-shutdown.txt")),
        format!("{report:#?}"),
    )
    .unwrap();
}

async fn subprocess(test: &str, profile: &str, directory: &Path, endpoint: &str) {
    let output = tokio::time::timeout(
        Duration::from_secs(30),
        tokio::process::Command::new(std::env::current_exe().unwrap())
            .args(["--include-ignored", "--exact", test, "--nocapture"])
            .env("LILY_DI_EXPORT_CHILD", profile)
            .env("LILY_DI_EXPORT_DIRECTORY", directory)
            .env("LILY_DI_EXPORT_ENDPOINT", endpoint)
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    std::fs::write(
        directory.join(format!("{profile}-stdout.log")),
        &output.stdout,
    )
    .unwrap();
    std::fs::write(
        directory.join(format!("{profile}-stderr.log")),
        &output.stderr,
    )
    .unwrap();
    assert!(
        output.status.success(),
        "{profile}: {}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

async fn is_child() -> bool {
    if let Ok(profile) = std::env::var("LILY_DI_EXPORT_CHILD") {
        child(
            &profile,
            Path::new(&std::env::var("LILY_DI_EXPORT_DIRECTORY").unwrap()),
            &std::env::var("LILY_DI_EXPORT_ENDPOINT").unwrap(),
        )
        .await;
        true
    } else {
        false
    }
}

fn verify_file(directory: &Path, profile: &str) {
    let expected: Value = serde_json::from_slice(
        &std::fs::read(directory.join(format!("{profile}-expected.json"))).unwrap(),
    )
    .unwrap();
    let records: Vec<Value> = std::fs::read_to_string(directory.join(format!("{profile}.jsonl")))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let records: Vec<_> = records
        .iter()
        .filter(|r| r["fields"]["lily.operation"] == "di.service.resolve")
        .collect();
    let debug = profile.ends_with("debug");
    assert_eq!(records.len(), if debug { 8 } else { 2 });
    let mut phases = BTreeMap::new();
    let mut codes = BTreeMap::new();
    for record in records {
        assert_eq!(record["trace_id"], expected["trace_id"]);
        assert_eq!(record["trace_flags"], "01");
        let fields = &record["fields"];
        if fields["lily.lifecycle"].is_string() {
            assert_eq!(record["level"], "DEBUG");
            let span = &record["span"];
            assert_eq!(span["name"], "di.service.resolve");
            let requested = span["di.requested_type"].as_str().unwrap();
            *phases
                .entry((
                    requested.to_owned(),
                    fields["lily.lifecycle"].as_str().unwrap().to_owned(),
                ))
                .or_insert(0) += 1;
            if requested == expected["missing"] {
                assert!(span.get("di.implementation_type").is_none());
            } else {
                assert_eq!(span["di.implementation_type"], requested);
                assert_eq!(
                    span["di.lifetime"],
                    if requested == expected["singleton"] {
                        "singleton"
                    } else {
                        "scoped"
                    }
                );
            }
            if fields["lily.lifecycle"] == "completed" {
                assert!(fields["lily.duration_ms"].is_number());
                assert!(fields["lily.duration_ms"].as_f64().unwrap().is_finite());
            }
        } else {
            assert_eq!(record["level"], "ERROR");
            assert_eq!(fields["message"], "DI service resolution failed");
            *codes
                .entry(fields["lily.error_code"].as_str().unwrap().to_owned())
                .or_insert(0) += 1;
            assert!(
                fields["di.requested_type"] == expected["missing"]
                    || fields["di.requested_type"] == expected["scoped"]
            );
        }
        if debug {
            assert_ne!(record["span_id"], expected["span_id"]);
        } else {
            assert_eq!(record["span_id"], expected["span_id"]);
        }
    }
    assert_eq!(
        codes,
        BTreeMap::from([
            ("di.scope_required".into(), 1),
            ("di.service_not_found".into(), 1)
        ])
    );
    if debug {
        assert_eq!(phases.len(), 6);
        for key in ["singleton", "scoped", "missing"] {
            for phase in ["started", "completed"] {
                assert_eq!(
                    phases[&(expected[key].as_str().unwrap().into(), phase.into())],
                    1
                );
            }
        }
    } else {
        assert!(phases.is_empty());
    }
}

#[tokio::test]
async fn installed_file_profiles_keep_di_failures_and_debug_details() {
    if is_child().await {
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    for profile in ["file-info", "file-debug"] {
        subprocess(
            "installed_file_profiles_keep_di_failures_and_debug_details",
            profile,
            directory.path(),
            "",
        )
        .await;
        verify_file(directory.path(), profile);
    }
}

fn wire_records(
    path: &Path,
    resource_key: &str,
    scope_key: &str,
    record_key: &str,
    service: &str,
) -> Vec<Value> {
    let text = std::fs::read_to_string(path).unwrap();
    let mut records = Vec::new();
    for batch in serde_json::Deserializer::from_str(&text).into_iter::<Value>() {
        for resource in batch.unwrap()[resource_key].as_array().unwrap() {
            if !resource["resource"]["attributes"]
                .as_array()
                .unwrap()
                .iter()
                .any(|kv| kv["key"] == "service.name" && kv["value"]["stringValue"] == service)
            {
                continue;
            }
            for scope in resource[scope_key].as_array().unwrap() {
                records.extend(scope[record_key].as_array().unwrap().iter().cloned());
            }
        }
    }
    records
}

fn attr<'a>(record: &'a Value, key: &str) -> Option<&'a Value> {
    let attributes = record["attributes"].as_array()?;
    let mut keys = HashSet::new();
    for kv in attributes {
        assert!(
            keys.insert(kv["key"].as_str().unwrap()),
            "duplicate raw attribute"
        );
    }
    attributes
        .iter()
        .find(|kv| kv["key"] == key)
        .map(|kv| &kv["value"])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker and the pinned Collector; qualifies INFO and console+OTLP DEBUG"]
async fn live_collector_preserves_di_filtering_and_shutdown_delivery() {
    if is_child().await {
        return;
    }
    let mut collector = collector::Collector::new();
    let result = std::panic::AssertUnwindSafe(async {
        let endpoint = collector.start().await.unwrap();
        for profile in ["otlp-info", "otlp-console-debug"] {
            subprocess(
                "live_collector_preserves_di_filtering_and_shutdown_delivery",
                profile,
                &collector.report,
                &endpoint,
            )
            .await;
        }
        collector.stop_and_capture().await.unwrap();
        for profile in ["otlp-info", "otlp-console-debug"] {
            let debug = profile.ends_with("debug");
            let expected: Value = serde_json::from_slice(
                &std::fs::read(collector.report.join(format!("{profile}-expected.json"))).unwrap(),
            )
            .unwrap();
            let spans = wire_records(
                &collector.report.join("capture/traces.jsonl"),
                "resourceSpans",
                "scopeSpans",
                "spans",
                profile,
            );
            let logs = wire_records(
                &collector.report.join("capture/logs.jsonl"),
                "resourceLogs",
                "scopeLogs",
                "logRecords",
                profile,
            );
            assert_eq!(spans.len(), if debug { 4 } else { 1 });
            let details: Vec<_> = logs
                .iter()
                .filter(|r| {
                    attr(r, "lily.operation") == Some(&json!({"stringValue":"di.service.resolve"}))
                })
                .collect();
            assert_eq!(details.len(), if debug { 8 } else { 2 });
            let console =
                std::fs::read_to_string(collector.report.join(format!("{profile}-stdout.log")))
                    .unwrap();
            for span in &spans {
                assert_eq!(span["traceId"], expected["trace_id"]);
                assert_eq!(span["droppedAttributesCount"].as_u64().unwrap_or(0), 0);
                if span["name"] == "di.service.resolve" {
                    assert_eq!(span["parentSpanId"], expected["span_id"]);
                    assert_eq!(
                        attr(span, "lily.lifecycle"),
                        Some(&json!({"stringValue":"completed"}))
                    );
                    assert!(attr(span, "lily.duration_ms").unwrap()["doubleValue"].is_number());
                    assert!(attr(span, "di.requested_type").unwrap()["stringValue"].is_string());
                }
            }
            let mut severity = BTreeMap::new();
            for log in details {
                assert_eq!(log["traceId"], expected["trace_id"]);
                *severity
                    .entry(log["severityText"].as_str().unwrap())
                    .or_insert(0) += 1;
                assert_eq!(log["droppedAttributesCount"].as_u64().unwrap_or(0), 0);
                if debug {
                    let identity = format!(
                        "trace_id={} span_id={}",
                        log["traceId"].as_str().unwrap(),
                        log["spanId"].as_str().unwrap()
                    );
                    assert!(
                        console.contains(&identity),
                        "console identity must match the actual OTLP record"
                    );
                    assert_eq!(
                        spans
                            .iter()
                            .filter(|s| s["spanId"] == log["spanId"]
                                && s["name"] == "di.service.resolve")
                            .count(),
                        1
                    );
                } else {
                    assert_eq!(log["spanId"], expected["span_id"]);
                }
            }
            assert_eq!(
                severity,
                if debug {
                    BTreeMap::from([("DEBUG", 6), ("ERROR", 2)])
                } else {
                    BTreeMap::from([("ERROR", 2)])
                }
            );
            let metrics = wire_records(
                &collector.report.join("capture/metrics.jsonl"),
                "resourceMetrics",
                "scopeMetrics",
                "metrics",
                profile,
            );
            let counters: Vec<_> = metrics
                .iter()
                .filter(|m| m["name"] == "di.service.resolutions.total")
                .collect();
            assert!(!counters.is_empty());
            for counter in counters {
                let points = counter["sum"]["dataPoints"].as_array().unwrap();
                assert_eq!(points.len(), 3);
                assert_eq!(
                    points
                        .iter()
                        .map(|p| p["asInt"].as_str().unwrap().parse::<u64>().unwrap())
                        .sum::<u64>(),
                    3
                );
            }
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
