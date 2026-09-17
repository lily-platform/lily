//! Qualify the installed file/console profiles in separate processes. Assertions
//! pair individual invocations by SDK identity, including concurrent names.

use async_trait::async_trait;
use futures_util::FutureExt;
use lily_trace::runtime::{
    ExportConfig, FileExportConfig, FileRotation, SamplingConfig, SamplingStrategy,
};
use lily_trace::{
    lily_trace, TraceConfig, TraceFailure, TraceInstallOutcome, TraceResultError,
    TracingRuntimeOwner,
};
use opentelemetry::{propagation::TextMapPropagator, trace::TraceContextExt};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};
use tracing::Instrument;

const CHILD: &str = "LILY_TRACE_CORRELATION_CHILD";
const DIRECTORY: &str = "LILY_TRACE_CORRELATION_DIRECTORY";
const REMOTE_TRACE: &str = "4bf92f3577b34da6a3ce929d0e0e4736";
static EXPECTED: Mutex<Vec<Expected>> = Mutex::new(Vec::new());

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Expected {
    trace_id: String,
    span_id: String,
    trace_flags: String,
    operation: String,
    lifecycle: String,
    outcome: Option<String>,
}

fn capture(lifecycle: &str, outcome: Option<&str>) -> Expected {
    let span = tracing::Span::current();
    let context = lily_trace::context_for_span(&span);
    let reference = context.span();
    let identity = reference.span_context();
    assert!(identity.is_valid());
    let expected = Expected {
        trace_id: identity.trace_id().to_string(),
        span_id: identity.span_id().to_string(),
        trace_flags: format!("{:02x}", identity.trace_flags()),
        operation: span.metadata().unwrap().name().into(),
        lifecycle: lifecycle.into(),
        outcome: outcome.map(str::to_owned),
    };
    EXPECTED.lock().unwrap().push(expected.clone());
    expected
}

fn worker(expected: &Expected) {
    let context = lily_trace::current_context();
    let reference = context.span();
    let actual = reference.span_context();
    assert_eq!(actual.trace_id().to_string(), expected.trace_id);
    assert_eq!(actual.span_id().to_string(), expected.span_id);
    tracing::info!(probe = "worker");
}

struct Failure(bool);
impl TraceResultError for Failure {
    fn trace_failure(&self) -> TraceFailure {
        if self.0 {
            TraceFailure::Error {
                code: "unavailable",
            }
        } else {
            TraceFailure::Rejected {
                code: "invalid_input",
            }
        }
    }
}
#[async_trait]
trait Work {
    async fn run(&self, case: usize, barrier: Arc<tokio::sync::Barrier>) -> Result<(), Failure>;
}
struct Service;
#[async_trait]
impl Work for Service {
    #[lily_trace(name = "correlation.concurrent", result)]
    async fn run(&self, case: usize, barrier: Arc<tokio::sync::Barrier>) -> Result<(), Failure> {
        let outcome = ["success", "rejected", "error"][case % 3];
        let expected = capture("completed", Some(outcome));
        if outcome == "success" || case == 7 {
            assert_eq!(expected.trace_id, REMOTE_TRACE);
        } else {
            assert_ne!(expected.trace_id, REMOTE_TRACE);
        }
        assert_eq!(expected.trace_flags, if case == 7 { "00" } else { "01" });
        barrier.wait().await;
        let for_async = expected.clone();
        lily_trace::spawn(async move {
            worker(&for_async);
        })
        .await
        .unwrap();
        lily_trace::spawn_blocking(move || worker(&expected))
            .await
            .unwrap();
        match case % 3 {
            0 => Ok(()),
            1 => Err(Failure(false)),
            _ => Err(Failure(true)),
        }
    }
}

#[lily_trace(name = "correlation.pending")]
async fn pending(ready: tokio::sync::oneshot::Sender<()>) {
    capture("dropped", None);
    ready.send(()).unwrap();
    std::future::pending::<()>().await;
}
#[lily_trace(name = "correlation.panic")]
async fn panics() {
    capture("panicked", None);
    tokio::task::yield_now().await;
    panic!("intentional correlation fixture panic");
}
#[lily_trace(name = "correlation.sync")]
fn sync_method() {
    capture("completed", None);
}
#[lily_trace(name = "correlation.never_polled")]
async fn never_polled() {
    panic!("must not run");
}
#[lily_trace(name = "correlation.disabled")]
async fn disabled() {
    tracing::info!(probe = "disabled");
}

async fn run_child(profile: &str, directory: &Path) {
    let config = TraceConfig {
        enabled: profile != "disabled",
        level: "info".into(),
        sampling: SamplingConfig {
            strategy: SamplingStrategy::Always,
            rate: 1.0,
        },
        export: ExportConfig {
            console: profile == "console",
            file: (profile != "console").then(|| FileExportConfig {
                path: directory.join("trace.jsonl").display().to_string(),
                rotation: FileRotation::Daily,
                max_file_bytes: 1024 * 1024,
                max_files: 2,
                buffered_lines: 1024,
                max_record_bytes: 16_384,
            }),
            ..Default::default()
        },
        ..Default::default()
    };
    let installed = TracingRuntimeOwner::install(&config).unwrap();
    if profile == "disabled" {
        assert!(matches!(installed, TraceInstallOutcome::Disabled));
        disabled().await;
        assert!(!directory.join("trace.jsonl").exists());
        return;
    }
    let TraceInstallOutcome::Owned(owner) = installed else {
        panic!("missing runtime owner")
    };
    let barrier = Arc::new(tokio::sync::Barrier::new(8));
    let mut tasks = Vec::new();
    for case in 0usize..8 {
        let transport = tracing::info_span!(parent: None, "correlation.transport");
        let parent = transport.in_scope(|| tracing::info_span!("correlation.request"));
        let mut headers = HashMap::new();
        if case.is_multiple_of(3) || case == 7 {
            headers.insert(
                "traceparent".into(),
                format!(
                    "00-{REMOTE_TRACE}-00f067aa0ba902b7-{}",
                    if case == 7 { "00" } else { "01" }
                ),
            );
        } else if case % 3 == 2 {
            headers.insert("traceparent".into(), "invalid".into());
        }
        lily_trace::set_parent(
            &parent,
            opentelemetry_sdk::propagation::TraceContextPropagator::new().extract(&headers),
        );
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(
            async move {
                let result = Service.run(case, barrier).await;
                assert_eq!(result.is_ok(), case.is_multiple_of(3));
            }
            .instrument(parent),
        ));
    }
    for task in tasks {
        task.await.unwrap();
    }
    let (send, receive) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(pending(send));
    receive.await.unwrap();
    // Drop while a different span is current; the terminal event must retain
    // the pending method's own dispatcher/span, not inherit the cancelling task.
    let unrelated = tracing::info_span!(parent: None, "unrelated");
    unrelated.in_scope(|| task.abort());
    assert!(task.await.unwrap_err().is_cancelled());
    drop(unrelated);
    assert!(std::panic::AssertUnwindSafe(panics())
        .catch_unwind()
        .await
        .is_err());
    sync_method();
    drop(never_polled());
    tracing::info!(probe = "outside");
    let report = owner.shutdown(Duration::from_secs(5)).await;
    assert!(report.is_success(), "{report:?}");
    if let Some(metrics) = report.file_metrics() {
        assert_eq!(metrics.total_dropped(), 0);
    }
    std::fs::write(
        directory.join("expected.json"),
        serde_json::to_vec(&*EXPECTED.lock().unwrap()).unwrap(),
    )
    .unwrap();
}

#[test]
fn installed_profiles_preserve_per_invocation_identity_and_lifecycle() {
    if let Ok(profile) = std::env::var(CHILD) {
        let directory = std::path::PathBuf::from(std::env::var_os(DIRECTORY).unwrap());
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                tokio::time::timeout(Duration::from_secs(20), run_child(&profile, &directory))
                    .await
                    .unwrap();
            });
        return;
    }
    for profile in ["file", "console", "disabled"] {
        let directory = tempfile::tempdir().unwrap();
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "installed_profiles_preserve_per_invocation_identity_and_lifecycle",
                "--nocapture",
            ])
            .env(CHILD, profile)
            .env(DIRECTORY, directory.path())
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{profile}: {}\n{}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        );
        let stdout = String::from_utf8(result.stdout).unwrap();
        if profile == "disabled" {
            assert!(!directory.path().join("trace.jsonl").exists());
            assert!(!stdout.contains("correlation.disabled"));
            continue;
        }
        let expected: Vec<Expected> =
            serde_json::from_slice(&std::fs::read(directory.path().join("expected.json")).unwrap())
                .unwrap();
        assert_eq!(expected.len(), 11);
        assert_eq!(
            expected
                .iter()
                .map(|e| (&e.trace_id, &e.span_id))
                .collect::<HashSet<_>>()
                .len(),
            11
        );
        let output = if profile == "file" {
            std::fs::read_to_string(directory.path().join("trace.jsonl")).unwrap()
        } else {
            stdout
        };
        assert!(!output.contains("correlation.never_polled"));
        if profile == "file" {
            let records: Vec<serde_json::Value> = output
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
            let methods: Vec<_> = records
                .iter()
                .filter(|r| r["fields"].get("lily.operation").is_some())
                .collect();
            assert_eq!(methods.len(), 22);
            for expected in &expected {
                let events: Vec<_> = methods
                    .iter()
                    .filter(|r| {
                        r["trace_id"] == expected.trace_id && r["span_id"] == expected.span_id
                    })
                    .collect();
                assert_eq!(events.len(), 2, "{expected:?}");
                assert_eq!(events[0]["fields"]["lily.lifecycle"], "started");
                assert_eq!(events[1]["fields"]["lily.lifecycle"], expected.lifecycle);
                for event in &events {
                    assert_eq!(event["trace_flags"], expected.trace_flags);
                    assert_eq!(event["fields"]["lily.operation"], expected.operation);
                    assert_eq!(event["span"]["name"], expected.operation);
                }
                let duration = events[1]["fields"]["lily.duration_ms"].as_f64().unwrap();
                assert!(duration.is_finite() && duration >= 0.0);
                assert_eq!(
                    events[1]["span"]["lily.duration_ms"].as_f64().unwrap(),
                    duration
                );
                assert_eq!(
                    events[1]["fields"]["lily.outcome"].as_str(),
                    expected.outcome.as_deref()
                );
                let workers: Vec<_> = records
                    .iter()
                    .filter(|r| {
                        r["fields"]["probe"] == "worker" && r["span_id"] == expected.span_id
                    })
                    .collect();
                assert_eq!(
                    workers.len(),
                    if expected.operation == "correlation.concurrent" {
                        2
                    } else {
                        0
                    }
                );
                for event in workers {
                    assert_eq!(event["trace_id"], expected.trace_id);
                }
            }
            let outside = records
                .iter()
                .find(|r| r["fields"]["probe"] == "outside")
                .unwrap();
            assert!(outside.get("trace_id").is_none() && outside.get("span_id").is_none());
        } else {
            let methods: Vec<_> = output
                .lines()
                .filter(|l| l.contains("lily.operation="))
                .collect();
            assert_eq!(methods.len(), 22);
            for expected in &expected {
                let prefix = format!(
                    "trace_id={} span_id={} trace_flags={} ",
                    expected.trace_id, expected.span_id, expected.trace_flags
                );
                let events: Vec<_> = methods.iter().filter(|l| l.contains(&prefix)).collect();
                assert_eq!(events.len(), 2, "{expected:?}");
                assert!(events[0].contains("method started"));
                assert!(events[1].contains("method finished"));
                assert!(events[1].contains(&format!("lily.lifecycle=\"{}\"", expected.lifecycle)));
                assert!(
                    !output
                        .lines()
                        .any(|line| line.contains(&prefix) && line.contains(": close")),
                    "duplicate method CLOSE"
                );
                assert_eq!(
                    output
                        .lines()
                        .filter(|line| line.contains(&prefix) && line.contains("probe=\"worker\""))
                        .count(),
                    if expected.operation == "correlation.concurrent" {
                        2
                    } else {
                        0
                    }
                );
            }
        }
    }
}
