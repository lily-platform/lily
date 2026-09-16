//! Run explicitly with Docker; see LIVE_OTLP_QUALIFICATION.md.

#[path = "support/collector.rs"]
mod collector;
#[path = "support/otlp_capture.rs"]
mod otlp_capture;

use async_trait::async_trait;
use futures_util::FutureExt;
use lily_trace::runtime::{
    ExportConfig, FilterRule, OtlpExportConfig, SamplingConfig, SamplingStrategy,
};
use lily_trace::{
    lily_trace, TraceConfig, TraceFailure, TraceInstallOutcome, TraceResultError,
    TracingRuntimeOwner,
};
use opentelemetry::trace::TraceContextExt;
use otlp_capture::{Capture, ExpectedMethod, Identity};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tokio::sync::oneshot;
use tracing::Instrument;

const SECRET: &str = "lily-live-test-payload-must-never-be-exported";
const CASES: usize = 9;
static CLASSIFICATIONS: AtomicUsize = AtomicUsize::new(0);
type Recorder = Arc<Mutex<HashMap<u64, Identity>>>;

// Deliberately no Debug, Display, Clone, or std::error::Error implementation.
struct Secret(String);
enum Failure {
    Rejected(String),
    Unavailable(String),
}

impl TraceResultError for Failure {
    fn trace_failure(&self) -> TraceFailure {
        CLASSIFICATIONS.fetch_add(1, Ordering::SeqCst);
        match self {
            Self::Rejected(_) => TraceFailure::Rejected {
                code: "invalid_credentials",
            },
            Self::Unavailable(_) => TraceFailure::Error {
                code: "service_unavailable",
            },
        }
    }
}

fn identity(span: &tracing::Span) -> Identity {
    let context = lily_trace::context_for_span(span);
    let reference = context.span();
    let context = reference.span_context();
    assert!(context.is_valid() && context.is_sampled());
    Identity {
        trace_id: context.trace_id().to_string(),
        span_id: context.span_id().to_string(),
    }
}

fn enter(case_id: u64, recorder: &Recorder) -> Identity {
    let current = identity(&tracing::Span::current());
    assert!(recorder
        .lock()
        .unwrap()
        .insert(case_id, current.clone())
        .is_none());
    current
}

#[async_trait]
trait QualificationService: Send + Sync {
    async fn call(
        &self,
        case_id: u64,
        failure: Option<Failure>,
        recorder: Recorder,
        ready: oneshot::Sender<()>,
        release: oneshot::Receiver<()>,
    ) -> Result<Secret, Failure>;
}

struct Service;

#[async_trait]
impl QualificationService for Service {
    #[lily_trace(name = "qualification.live.classified", result, fields(case_id))]
    async fn call(
        &self,
        case_id: u64,
        failure: Option<Failure>,
        recorder: Recorder,
        ready: oneshot::Sender<()>,
        release: oneshot::Receiver<()>,
    ) -> Result<Secret, Failure> {
        let before = enter(case_id, &recorder);
        ready.send(()).unwrap();
        release.await.unwrap();
        tokio::task::yield_now().await;
        assert_eq!(identity(&tracing::Span::current()), before);
        if let Some(failure) = failure {
            return Err(failure);
        }
        Ok(Secret(SECRET.into()))
    }
}

#[lily_trace(name = "qualification.live.native", result, fields(case_id))]
async fn native(case_id: u64, recorder: &Recorder) -> Result<Secret, Failure> {
    let before = enter(case_id, recorder);
    tokio::task::yield_now().await;
    assert_eq!(identity(&tracing::Span::current()), before);
    Ok(Secret(SECRET.into()))
}

#[lily_trace(name = "qualification.live.unclassified", fields(case_id))]
fn unclassified(case_id: u64, recorder: &Recorder) -> Result<(), Secret> {
    enter(case_id, recorder);
    Err(Secret(SECRET.into()))
}

#[lily_trace(name = "qualification.live.pending", result, fields(case_id))]
async fn pending(
    case_id: u64,
    recorder: Recorder,
    ready: Option<oneshot::Sender<()>>,
) -> Result<(), Failure> {
    enter(case_id, &recorder);
    if let Some(ready) = ready {
        ready.send(()).unwrap();
    }
    std::future::pending().await
}

#[lily_trace(name = "qualification.live.panicked", result, fields(case_id))]
async fn panicked(case_id: u64, recorder: Recorder) -> Result<(), Failure> {
    enter(case_id, &recorder);
    tokio::task::yield_now().await;
    panic!("intentional qualification panic");
}

#[lily_trace(name = "qualification.live.shutdown_pending", result, fields(case_id))]
fn shutdown_pending(case_id: u64, recorder: &Recorder) -> Result<(), std::convert::Infallible> {
    enter(case_id, recorder);
    Ok(())
}

fn expected(case_id: u64, recorder: &Recorder, caller_started: Instant) -> ExpectedMethod {
    let (name, lifecycle, outcome, code, status) = match case_id {
        1 => (
            "qualification.live.classified",
            "completed",
            Some("success"),
            None,
            1,
        ),
        2 => (
            "qualification.live.classified",
            "completed",
            Some("rejected"),
            Some("invalid_credentials"),
            1,
        ),
        3 => (
            "qualification.live.classified",
            "completed",
            Some("error"),
            Some("service_unavailable"),
            2,
        ),
        4 => (
            "qualification.live.native",
            "completed",
            Some("success"),
            None,
            1,
        ),
        5 => (
            "qualification.live.unclassified",
            "completed",
            None,
            None,
            0,
        ),
        6 | 7 => ("qualification.live.pending", "dropped", None, None, 0),
        8 => ("qualification.live.panicked", "panicked", None, None, 2),
        9 => (
            "qualification.live.shutdown_pending",
            "completed",
            Some("success"),
            None,
            1,
        ),
        _ => panic!("unknown qualification case"),
    };
    ExpectedMethod {
        case_id,
        name,
        lifecycle,
        outcome,
        code,
        status,
        identity: recorder.lock().unwrap()[&case_id].clone(),
        parent: None,
        min_ms: 0.0,
        max_ms: caller_started.elapsed().as_secs_f64() * 1000.0,
    }
}

async fn exercise(collector: &mut collector::Collector) -> Value {
    let endpoint = collector.start().await.expect("start disposable Collector");
    let service_name = format!(
        "lily-qualification-{}",
        collector.report.file_name().unwrap().to_str().unwrap()
    );
    let config = TraceConfig {
        enabled: true,
        service_name: service_name.clone(),
        // Restrict routine dependency chatter; errors still fail exact counts.
        level: "error".into(),
        filters: vec![FilterRule {
            target: module_path!().into(),
            level: "info".into(),
        }],
        sampling: SamplingConfig {
            strategy: SamplingStrategy::Always,
            rate: 1.0,
        },
        export: ExportConfig {
            console: std::env::var_os("LILY_TEST_OTLP_CONSOLE").is_some(),
            otlp: Some(OtlpExportConfig {
                endpoint,
                protocol: "grpc".into(),
                headers: Default::default(),
                timeout: 5,
                max_queue_items: 1024,
                max_queue_bytes: 4 * 1024 * 1024,
                max_batch_size: 128,
                flush_interval_millis: 60_000,
                metrics_export_interval_millis: 300_000,
                max_export_retries: 1,
                retry_backoff_millis: 25,
            }),
            ..ExportConfig::default()
        },
        ..TraceConfig::default()
    };
    let owner = match TracingRuntimeOwner::install(&config).unwrap() {
        TraceInstallOutcome::Owned(owner) => owner,
        TraceInstallOutcome::Disabled => panic!("full sampling unexpectedly disabled"),
    };
    let recorder = Recorder::default();
    let service: Arc<dyn QualificationService> = Arc::new(Service);
    let mut invocations = Vec::new();
    let mut gates = Vec::new();
    let mut ready = Vec::new();
    for case_id in 1..=3 {
        let started = Instant::now();
        let parent = tracing::info_span!("qualification.live.request", case_id);
        let parent_identity = identity(&parent);
        let service = Arc::clone(&service);
        let recorder = Arc::clone(&recorder);
        let (entered, wait) = oneshot::channel();
        let (release, resume) = oneshot::channel();
        let failure = match case_id {
            1 => None,
            2 => Some(Failure::Rejected(SECRET.into())),
            _ => Some(Failure::Unavailable(SECRET.into())),
        };
        let task = tokio::spawn(
            async move {
                service
                    .call(case_id, failure, recorder, entered, resume)
                    .await
            }
            .instrument(parent),
        );
        invocations.push((case_id, started, parent_identity, task));
        ready.push(wait);
        gates.push(release);
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        for entered in ready {
            entered.await.unwrap();
        }
    })
    .await
    .expect("all service methods must enter before releasing any");
    let held = Instant::now();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let minimum_ms = held.elapsed().as_secs_f64() * 1000.0;
    for gate in gates {
        gate.send(()).unwrap();
    }
    let mut methods = Vec::new();
    for (case_id, started, parent, task) in invocations {
        match task.await.unwrap() {
            Ok(Secret(value)) => {
                assert_eq!(case_id, 1);
                assert_eq!(value, SECRET);
            }
            Err(Failure::Rejected(value)) => {
                assert_eq!(case_id, 2);
                assert_eq!(value, SECRET);
            }
            Err(Failure::Unavailable(value)) => {
                assert_eq!(case_id, 3);
                assert_eq!(value, SECRET);
            }
        }
        let mut method = expected(case_id, &recorder, started);
        method.parent = Some(parent);
        method.min_ms = minimum_ms;
        methods.push(method);
    }
    let started = Instant::now();
    assert!(matches!(native(4, &recorder).await, Ok(Secret(value)) if value == SECRET));
    methods.push(expected(4, &recorder, started));
    let started = Instant::now();
    assert!(matches!(unclassified(5, &recorder), Err(Secret(value)) if value == SECRET));
    methods.push(expected(5, &recorder, started));

    // An unpolled method must not create any span or event.
    drop(pending(999, Arc::clone(&recorder), None));
    let started = Instant::now();
    let mut future = Box::pin(pending(6, Arc::clone(&recorder), None));
    assert!(futures_util::poll!(&mut future).is_pending());
    tracing::subscriber::with_default(tracing::subscriber::NoSubscriber::default(), || {
        drop(future)
    });
    methods.push(expected(6, &recorder, started));

    let started = Instant::now();
    let (ready, wait) = oneshot::channel();
    let task = tokio::spawn(pending(7, Arc::clone(&recorder), Some(ready)));
    tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .unwrap()
        .unwrap();
    task.abort();
    assert!(matches!(task.await, Err(error) if error.is_cancelled()));
    methods.push(expected(7, &recorder, started));
    let started = Instant::now();
    assert!(
        matches!(tokio::spawn(panicked(8, Arc::clone(&recorder))).await, Err(error) if error.is_panic())
    );
    methods.push(expected(8, &recorder, started));
    assert_eq!(CLASSIFICATIONS.load(Ordering::SeqCst), 2);

    let counter = opentelemetry::global::meter("lily.live.qualification")
        .u64_counter("lily.qualification.methods")
        .build();
    counter.add(CASES as u64, &[]);
    let started = Instant::now();
    shutdown_pending(9, &recorder).unwrap();
    // This test uses one Tokio thread. There is no await between the last
    // synchronous span/log enqueue and shutdown: those records are necessarily
    // pending for the owned exporter tasks when shutdown is requested.
    let terminal_expected = expected(9, &recorder, started);
    let report = owner.shutdown(Duration::from_secs(10)).await;
    methods.push(terminal_expected);
    std::fs::write(
        collector.report.join("shutdown.txt"),
        format!("{report:#?}"),
    )
    .unwrap();
    assert!(report.is_success(), "shutdown report: {report:?}");
    assert!(matches!(
        report.log_export,
        lily_trace::ExportTaskShutdownStatus::Completed(_)
    ));
    assert!(matches!(
        report.span_export,
        lily_trace::SpanExportTaskShutdownStatus::Completed(_)
    ));
    let spans = report.span_metrics.expect("span terminal ledger");
    let logs = report.log_metrics.expect("log terminal ledger");
    assert_eq!(spans.accepted, (CASES + 3) as u64);
    assert_eq!(spans.exported, spans.accepted);
    assert_eq!(
        (
            spans.dropped,
            spans.rejected,
            spans.in_flight,
            spans.queued_bytes,
            spans.export_failures,
            spans.retry_attempts
        ),
        (0, 0, 0, 0, 0, 0)
    );
    assert_eq!(logs.accepted, (CASES * 2) as u64);
    assert_eq!(logs.exported, logs.accepted);
    assert_eq!(
        (
            logs.dropped,
            logs.rejected,
            logs.in_flight,
            logs.queued_bytes,
            logs.export_failures,
            logs.retry_attempts
        ),
        (0, 0, 0, 0, 0, 0)
    );
    assert_eq!(recorder.lock().unwrap().len(), CASES);
    std::fs::write(
        collector.report.join("expected.json"),
        serde_json::to_vec_pretty(&methods).unwrap(),
    )
    .unwrap();
    collector
        .stop_and_capture()
        .await
        .expect("flush and capture Collector output");
    let capture = Capture::read(&collector.report.join("capture"), &service_name)
        .expect("read real OTLP payloads");
    let mut summary = capture
        .verify(&methods, SECRET)
        .expect("Collector payload contract");
    summary["negative_controls"] = json!(capture
        .negative_controls(&methods, SECRET)
        .expect("corrupted payloads must fail validation"));
    summary["collector_image"] = json!(collector::IMAGE);
    summary["service_name"] = json!(service_name);
    summary["shutdown_pending_probe"] = json!("passed");
    summary
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "live OTLP: requires Docker and the pinned Collector image; see LIVE_OTLP_QUALIFICATION.md"]
async fn full_sampling_exports_and_awaits_terminal_ledger() {
    let mut collector = collector::Collector::new();
    // Assert failures and test panics still remove only our owned container.
    let result = std::panic::AssertUnwindSafe(exercise(&mut collector))
        .catch_unwind()
        .await;
    let cleanup = collector.cleanup().await;
    let summary = match &result {
        Ok(summary) if cleanup.is_ok() => summary.clone(),
        _ => json!({"status": "failed", "cleanup_error": cleanup.as_ref().err(),
        "failure": result.as_ref().err().map(|panic| {
            panic.downcast_ref::<String>().cloned()
                .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_else(|| "non-string panic".into())
        })}),
    };
    std::fs::write(
        collector.report.join("summary.json"),
        serde_json::to_vec_pretty(&summary).unwrap(),
    )
    .unwrap();
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
    cleanup.expect("remove qualification Collector");
    println!("Verified {CASES} method cases, 12 spans, 18 logs, 13 corruption controls and shutdown delivery.");
}

#[tokio::test]
#[ignore = "live OTLP + console: requires Docker and the pinned Collector image"]
async fn console_identities_match_the_live_otlp_method_records() {
    // The ordinary live test runs in a fresh process because runtime ownership
    // is process-global. Keep its full Collector assertions and raw evidence.
    let result = tokio::time::timeout(
        Duration::from_secs(120),
        tokio::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "full_sampling_exports_and_awaits_terminal_ledger",
                "--nocapture",
            ])
            .env("LILY_TEST_OTLP_CONSOLE", "1")
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    let stdout = String::from_utf8(result.stdout).unwrap();
    let stderr = String::from_utf8(result.stderr).unwrap();
    let report_line = stdout
        .lines()
        .find_map(|line| {
            line.split_once("OTLP qualification evidence: ")
                .map(|(_, path)| path)
        })
        .expect("child evidence path");
    let report = std::path::Path::new(report_line);
    std::fs::write(report.join("console.stdout"), &stdout).unwrap();
    std::fs::write(report.join("console.stderr"), &stderr).unwrap();
    assert!(
        result.status.success(),
        "child OTLP qualification failed: {stdout}\n{stderr}"
    );
    let methods: Vec<Value> =
        serde_json::from_slice(&std::fs::read(report.join("expected.json")).unwrap()).unwrap();
    assert_eq!(methods.len(), CASES);
    let lines: Vec<_> = stdout
        .lines()
        .filter(|line| line.contains("lily.operation="))
        .collect();
    assert_eq!(lines.len(), CASES * 2);
    for method in methods {
        let prefix = format!(
            "trace_id={} span_id={} trace_flags=01 ",
            method["identity"]["trace_id"].as_str().unwrap(),
            method["identity"]["span_id"].as_str().unwrap()
        );
        let matching: Vec<_> = lines.iter().filter(|line| line.contains(&prefix)).collect();
        assert_eq!(matching.len(), 2, "method: {method}");
        assert!(matching[0].contains("method started"));
        assert!(matching[1].contains("method finished"));
        assert!(matching[1].contains(&format!(
            "lily.lifecycle=\"{}\"",
            method["lifecycle"].as_str().unwrap()
        )));
    }
    assert!(!stdout.contains(SECRET));
    std::fs::write(
        report.join("console-correlation.json"),
        serde_json::to_vec_pretty(
            &json!({"status": "passed", "methods": CASES, "matching_console_events": CASES * 2}),
        )
        .unwrap(),
    )
    .unwrap();
    println!("Console/OTLP correlation evidence: {}", report.display());
}
