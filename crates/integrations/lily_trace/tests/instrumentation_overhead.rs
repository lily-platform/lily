//! Short, socketless instrumentation-cost characterization.
//!
//! This is not the R9 load/soak qualification result. It gives developers a
//! repeatable early warning before running the canonical external workload.

use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use lily_monitoring::ProcessResourceSnapshot;
use tracing::Subscriber;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{Layer, Registry};

const OPERATIONS: usize = 10_000;

#[derive(Default)]
struct CountingLayer {
    spans: Arc<AtomicU64>,
    events: Arc<AtomicU64>,
}

impl<S> Layer<S> for CountingLayer
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_new_span(
        &self,
        _attributes: &tracing::span::Attributes<'_>,
        _id: &tracing::Id,
        _context: Context<'_, S>,
    ) {
        self.spans.fetch_add(1, Ordering::Relaxed);
    }

    fn on_event(&self, _event: &tracing::Event<'_>, _context: Context<'_, S>) {
        self.events.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Debug)]
struct WorkloadResult {
    elapsed: Duration,
    p95: Duration,
    p99: Duration,
    resources_before: ProcessResourceSnapshot,
    resources_after: ProcessResourceSnapshot,
    spans: u64,
    events: u64,
}

fn workload(dispatch: &tracing::Dispatch, spans: u64, events: u64) -> WorkloadResult {
    let resources_before = ProcessResourceSnapshot::capture();
    let started = Instant::now();
    let mut samples = Vec::with_capacity(OPERATIONS);
    tracing::dispatcher::with_default(dispatch, || {
        for operation in 0..OPERATIONS {
            let operation_started = Instant::now();
            let span = tracing::info_span!(
                "qualification.overhead.operation",
                lily.outcome = "success",
                lily.operation_class = "synthetic"
            );
            let _entered = span.enter();
            tracing::info!(lily.phase = "terminal", "qualification operation complete");
            black_box(operation.wrapping_mul(31));
            samples.push(operation_started.elapsed());
        }
    });
    let elapsed = started.elapsed();
    samples.sort_unstable();
    let percentile = |numerator: usize| {
        let index = (samples.len() * numerator / 100).min(samples.len() - 1);
        samples[index]
    };
    WorkloadResult {
        elapsed,
        p95: percentile(95),
        p99: percentile(99),
        resources_before,
        resources_after: ProcessResourceSnapshot::capture(),
        spans,
        events,
    }
}

#[test]
#[ignore = "short qualification characterization; run explicitly with --ignored --nocapture"]
fn compares_disabled_and_full_capture_without_network() {
    let disabled = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
    let baseline = workload(&disabled, 0, 0);

    let spans = Arc::new(AtomicU64::new(0));
    let events = Arc::new(AtomicU64::new(0));
    let subscriber = Registry::default().with(CountingLayer {
        spans: Arc::clone(&spans),
        events: Arc::clone(&events),
    });
    let full = tracing::Dispatch::new(subscriber);
    let instrumented = workload(
        &full,
        spans.load(Ordering::Acquire),
        events.load(Ordering::Acquire),
    );
    // Counters are read again because workload deliberately has no exporter
    // queue and therefore no asynchronous terminal race.
    let instrumented = WorkloadResult {
        spans: spans.load(Ordering::Acquire),
        events: events.load(Ordering::Acquire),
        ..instrumented
    };

    assert_eq!(instrumented.spans, OPERATIONS as u64);
    assert_eq!(instrumented.events, OPERATIONS as u64);
    println!(
        "mode=disabled operations={OPERATIONS} elapsed={:?} p95={:?} p99={:?} resources_before={:?} resources_after={:?}",
        baseline.elapsed,
        baseline.p95,
        baseline.p99,
        baseline.resources_before,
        baseline.resources_after,
    );
    println!(
        "mode=qualification_full_capture operations={OPERATIONS} elapsed={:?} p95={:?} p99={:?} spans={} events={} exporter_queue=not_configured dropped=not_applicable resources_before={:?} resources_after={:?}",
        instrumented.elapsed,
        instrumented.p95,
        instrumented.p99,
        instrumented.spans,
        instrumented.events,
        instrumented.resources_before,
        instrumented.resources_after,
    );
}

#[lily_trace::lily_trace(name = "qualification.overhead.method", result)]
fn method_workload(operation: usize) -> Result<usize, std::convert::Infallible> {
    Ok(black_box(operation).wrapping_mul(31))
}

#[test]
#[ignore = "macro cost characterization; run explicitly with --ignored --nocapture"]
fn compares_actual_result_macro_disabled_and_enabled() {
    let measure = |dispatch: &tracing::Dispatch| {
        tracing::dispatcher::with_default(dispatch, || {
            let started = Instant::now();
            for operation in 0..OPERATIONS {
                assert_eq!(
                    black_box(method_workload(operation)).unwrap(),
                    operation.wrapping_mul(31)
                );
            }
            started.elapsed()
        })
    };
    let disabled = measure(&tracing::Dispatch::new(
        tracing::subscriber::NoSubscriber::default(),
    ));
    let spans = Arc::new(AtomicU64::new(0));
    let events = Arc::new(AtomicU64::new(0));
    let subscriber = Registry::default().with(CountingLayer {
        spans: Arc::clone(&spans),
        events: Arc::clone(&events),
    });
    let enabled = measure(&tracing::Dispatch::new(subscriber));
    assert_eq!(spans.load(Ordering::Relaxed), OPERATIONS as u64);
    assert_eq!(events.load(Ordering::Relaxed), (OPERATIONS * 2) as u64);
    // Timing is diagnostic evidence; machine-dependent latency is not a test
    // threshold. Exact emissions and preserved results are the hard contract.
    println!("macro=result operations={OPERATIONS} disabled={disabled:?} enabled={enabled:?} spans={} events={} exporter=none",
        spans.load(Ordering::Relaxed), events.load(Ordering::Relaxed));
}
