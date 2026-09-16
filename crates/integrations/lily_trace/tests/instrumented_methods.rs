use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use lily_trace::{lily_trace, tracing};
use serde_json::Value;
use tracing::instrument::WithSubscriber;
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{Layer, Registry};

#[derive(Debug)]
struct CapturedSpan {
    name: &'static str,
    parent: Option<Id>,
    fields: BTreeMap<String, Value>,
    closed: bool,
}

#[derive(Debug)]
struct CapturedEvent {
    name: &'static str,
    target: &'static str,
    span: Option<Id>,
    fields: BTreeMap<String, Value>,
}

#[derive(Default, Debug)]
struct Capture {
    spans: HashMap<Id, CapturedSpan>,
    events: Vec<CapturedEvent>,
}

#[derive(Clone, Default)]
struct CaptureLayer(Arc<Mutex<Capture>>);

struct Fields<'a>(&'a mut BTreeMap<String, Value>);

impl tracing::field::Visit for Fields<'_> {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.insert(field.name().to_owned(), value.into());
    }

    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.0.insert(field.name().to_owned(), value.into());
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().to_owned(), format!("{value:?}").into());
    }
}

impl<S> Layer<S> for CaptureLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attributes: &Attributes<'_>, id: &Id, context: Context<'_, S>) {
        let mut fields = BTreeMap::new();
        attributes.record(&mut Fields(&mut fields));
        self.0.lock().unwrap().spans.insert(
            id.clone(),
            CapturedSpan {
                name: attributes.metadata().name(),
                parent: attributes.parent().cloned().or_else(|| {
                    attributes
                        .is_contextual()
                        .then(|| context.current_span().id().cloned())
                        .flatten()
                }),
                fields,
                closed: false,
            },
        );
    }

    fn on_record(&self, id: &Id, record: &Record<'_>, _: Context<'_, S>) {
        let mut capture = self.0.lock().unwrap();
        record.record(&mut Fields(&mut capture.spans.get_mut(id).unwrap().fields));
    }

    fn on_event(&self, event: &Event<'_>, context: Context<'_, S>) {
        let mut fields = BTreeMap::new();
        event.record(&mut Fields(&mut fields));
        self.0.lock().unwrap().events.push(CapturedEvent {
            name: event.metadata().name(),
            target: event.metadata().target(),
            span: context.event_span(event).map(|span| span.id().clone()),
            fields,
        });
    }

    fn on_close(&self, id: Id, _: Context<'_, S>) {
        self.0.lock().unwrap().spans.get_mut(&id).unwrap().closed = true;
    }
}

fn capture() -> (tracing::Dispatch, Arc<Mutex<Capture>>) {
    let layer = CaptureLayer::default();
    let capture = Arc::clone(&layer.0);
    (
        tracing::Dispatch::new(Registry::default().with(layer)),
        capture,
    )
}

fn current_name() -> Option<&'static str> {
    tracing::Span::current()
        .metadata()
        .map(|metadata| metadata.name())
}

#[async_trait]
trait Service {
    async fn run(&self) -> (&'static str, &'static str);
}

struct Implementation;

#[async_trait]
impl Service for Implementation {
    #[lily_trace(name = "qualification.async_trait")]
    async fn run(&self) -> (&'static str, &'static str) {
        let before = current_name().expect("async-trait body must enter its method span");
        tokio::task::yield_now().await;
        let after = current_name().expect("method span must be reentered after suspension");
        (before, after)
    }
}

#[tokio::test]
async fn async_trait_body_is_instrumented_across_suspension() {
    let (dispatch, capture) = capture();
    let result = Implementation.run().with_subscriber(dispatch).await;
    assert_eq!(
        result,
        ("qualification.async_trait", "qualification.async_trait")
    );
    let capture = capture.lock().unwrap();
    assert_eq!(capture.spans.len(), 1);
    assert!(capture.spans.values().all(|span| span.closed));
}

fn assert_terminal<'a>(capture: &'a Capture, name: &str, phase: &str) -> &'a CapturedEvent {
    let matching = capture
        .spans
        .iter()
        .filter(|(_, span)| span.name == name)
        .collect::<Vec<_>>();
    assert_eq!(matching.len(), 1, "{capture:?}");
    let (id, span) = matching[0];
    let events = capture
        .events
        .iter()
        .filter(|event| event.span.as_ref() == Some(id) && event.name.starts_with("lily.method."))
        .collect::<Vec<_>>();
    assert_eq!(events.len(), 2, "{capture:?}");
    assert_eq!(events[0].name, "lily.method.started");
    assert_eq!(events[0].fields["lily.lifecycle"], "started");
    assert!(!events[0].fields.contains_key("lily.outcome"));
    assert!(!events[0].fields.contains_key("lily.error_code"));
    let terminal = events[1];
    assert_eq!(terminal.name, "lily.method.finished");
    assert_eq!(
        terminal.target,
        module_path!(),
        "events must retain the caller's filter target"
    );
    assert_eq!(terminal.fields["lily.operation"], name);
    assert_eq!(terminal.fields["lily.lifecycle"], phase);
    assert_eq!(span.fields["lily.lifecycle"], phase);
    assert_eq!(
        span.fields["lily.duration_ms"],
        terminal.fields["lily.duration_ms"]
    );
    let duration = terminal.fields["lily.duration_ms"].as_f64().unwrap();
    assert!(duration.is_finite() && duration >= 0.0);
    terminal
}

// Deliberately neither Debug nor Display nor Clone.
struct Secret(String);
struct Failure {
    technical: bool,
    classified: Arc<std::sync::atomic::AtomicUsize>,
}
impl lily_trace::TraceResultError for Failure {
    fn trace_failure(&self) -> lily_trace::TraceFailure {
        self.classified
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if self.technical {
            lily_trace::TraceFailure::Error {
                code: "service_unavailable",
            }
        } else {
            lily_trace::TraceFailure::Rejected {
                code: "invalid_credentials",
            }
        }
    }
}

type DomainResult<T> = Result<T, Failure>;

#[lily_trace(name = "qualification.classified", result)]
async fn classified(value: Secret, failure: Option<Failure>) -> DomainResult<Secret> {
    tokio::task::yield_now().await;
    if let Some(error) = failure {
        Err(error)?;
    }
    Ok(value)
}

#[tokio::test]
async fn result_alias_preserves_values_and_classifies_once_without_formatting_payloads() {
    for outcome in ["success", "rejected", "error"] {
        let (dispatch, capture) = capture();
        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let error = (outcome != "success").then(|| Failure {
            technical: outcome == "error",
            classified: Arc::clone(&count),
        });
        let result = classified(Secret("never-export-this-token".to_owned()), error)
            .with_subscriber(dispatch)
            .await;
        match result {
            Ok(secret) => {
                assert_eq!(outcome, "success");
                assert_eq!(secret.0, "never-export-this-token");
            }
            Err(error) => assert_eq!(error.technical, outcome == "error"),
        }
        assert_eq!(
            count.load(std::sync::atomic::Ordering::SeqCst),
            usize::from(outcome != "success")
        );
        let capture = capture.lock().unwrap();
        let event = assert_terminal(&capture, "qualification.classified", "completed");
        assert_eq!(event.fields["lily.outcome"], outcome);
        let span = capture.spans.values().next().unwrap();
        assert_eq!(span.fields["lily.outcome"], outcome);
        if outcome == "success" {
            assert!(!event.fields.contains_key("lily.error_code"));
            assert!(!span.fields.contains_key("lily.error_code"));
        } else {
            let code = if outcome == "error" {
                "service_unavailable"
            } else {
                "invalid_credentials"
            };
            assert_eq!(event.fields["lily.error_code"], code);
            assert_eq!(span.fields["lily.error_code"], code);
        }
        assert!(!format!("{capture:?}").contains("never-export-this-token"));
    }
}

#[lily_trace(name = "qualification.unclassified")]
fn unclassified() -> Result<(), Secret> {
    Err(Secret("not-a-trace-error".to_owned()))
}

#[test]
fn missing_result_flag_accepts_unclassified_errors_and_emits_only_lifecycle() {
    let (dispatch, capture) = capture();
    let result = tracing::dispatcher::with_default(&dispatch, unclassified);
    assert!(matches!(result, Err(Secret(message)) if message == "not-a-trace-error"));
    let capture = capture.lock().unwrap();
    let event = assert_terminal(&capture, "qualification.unclassified", "completed");
    assert!(!event.fields.contains_key("lily.outcome"));
    assert!(!event.fields.contains_key("lily.error_code"));
    assert!(!capture
        .spans
        .values()
        .next()
        .unwrap()
        .fields
        .contains_key("lily.outcome"));
}

#[lily_trace(name = "qualification.sync", result)]
fn sync_borrow<'a>(
    __lily_span: &'a str,
    __lily_result: bool,
) -> Result<&'a str, std::convert::Infallible> {
    if __lily_result {
        return Ok(__lily_span);
    }
    Ok("fallback")
}

#[test]
fn sync_early_return_borrow_and_macro_binding_hygiene_are_preserved() {
    let (dispatch, capture) = capture();
    let value = String::from("owned-by-caller");
    let result =
        tracing::dispatcher::with_default(&dispatch, || sync_borrow(&value, true)).unwrap();
    assert!(std::ptr::eq(result.as_ptr(), value.as_ptr()));
    let capture = capture.lock().unwrap();
    assert_eq!(
        assert_terminal(&capture, "qualification.sync", "completed").fields["lily.outcome"],
        "success"
    );
}

#[async_trait(?Send)]
trait LocalService {
    async fn borrow<'a>(&self, value: &'a str) -> Result<&'a str, std::convert::Infallible>;
}
struct LocalImplementation(std::rc::Rc<()>);
#[async_trait(?Send)]
impl LocalService for LocalImplementation {
    #[lily_trace(name = "qualification.local", result, skip(self))]
    async fn borrow<'a>(&self, value: &'a str) -> Result<&'a str, std::convert::Infallible> {
        let retained = std::rc::Rc::clone(&self.0);
        tokio::task::yield_now().await;
        assert_eq!(std::rc::Rc::strong_count(&retained), 2);
        assert_eq!(current_name(), Some("qualification.local"));
        Ok(value)
    }
}

#[tokio::test]
async fn async_trait_non_send_and_borrowed_return_do_not_gain_new_bounds() {
    let (dispatch, capture) = capture();
    let service = LocalImplementation(std::rc::Rc::new(()));
    let value = String::from("borrowed");
    assert_eq!(
        service
            .borrow(&value)
            .with_subscriber(dispatch)
            .await
            .unwrap(),
        "borrowed"
    );
    assert_terminal(&capture.lock().unwrap(), "qualification.local", "completed");
}

type Boxed<'a> = std::pin::Pin<Box<dyn std::future::Future<Output = &'a str> + 'a>>;
#[lily_trace(name = "qualification.boxed")]
fn boxed(value: &str) -> Boxed<'_> {
    Box::pin(async move {
        tokio::task::yield_now().await;
        assert_eq!(current_name(), Some("qualification.boxed"));
        value
    })
}

#[tokio::test]
async fn boxed_future_starts_only_when_polled_and_keeps_its_borrow() {
    let (dispatch, capture) = capture();
    let value = String::from("borrowed");
    let future = tracing::dispatcher::with_default(&dispatch, || boxed(&value));
    assert!(capture.lock().unwrap().spans.is_empty());
    assert_eq!(future.with_subscriber(dispatch).await, "borrowed");
    assert_terminal(&capture.lock().unwrap(), "qualification.boxed", "completed");
}

#[lily_trace(name = "qualification.pending")]
async fn pending() {
    std::future::pending::<()>().await;
}

#[tokio::test]
async fn unpolled_future_emits_nothing_and_dropped_future_finishes_on_its_original_dispatch() {
    let (dispatch, capture) = capture();
    drop(pending());
    assert!(capture.lock().unwrap().events.is_empty());
    let mut future = Box::pin(pending().with_subscriber(dispatch));
    assert!(futures_util::poll!(&mut future).is_pending());
    assert_eq!(capture.lock().unwrap().events.len(), 1);
    let (different_dispatch, different_capture) = self::capture();
    tracing::dispatcher::with_default(&different_dispatch, || drop(future));
    let capture = capture.lock().unwrap();
    let event = assert_terminal(&capture, "qualification.pending", "dropped");
    assert!(!event.fields.contains_key("lily.outcome"));
    assert!(!event.fields.contains_key("lily.error_code"));
    assert!(capture.spans.values().all(|span| span.closed));
    assert!(different_capture.lock().unwrap().spans.is_empty());
    assert!(different_capture.lock().unwrap().events.is_empty());
}

#[lily_trace(name = "qualification.panicked")]
fn panicked() {
    panic!("business panic must propagate");
}

#[test]
fn panic_is_not_swallowed_or_reported_as_an_application_rejection() {
    let (dispatch, capture) = capture();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tracing::dispatcher::with_default(&dispatch, panicked);
    }));
    assert!(result.is_err());
    let capture = capture.lock().unwrap();
    let event = assert_terminal(&capture, "qualification.panicked", "panicked");
    assert!(!event.fields.contains_key("lily.outcome"));
    assert!(!event.fields.contains_key("lily.error_code"));
    assert!(capture.spans.values().all(|span| span.closed));
}

#[lily_trace(name = "qualification.retained")]
fn retained_span() -> tracing::Span {
    tracing::Span::current()
}

#[test]
fn method_finishes_before_retained_span_closes_without_duplicate_terminal_events() {
    let (dispatch, capture) = capture();
    let retained = tracing::dispatcher::with_default(&dispatch, retained_span);
    {
        let capture = capture.lock().unwrap();
        assert_terminal(&capture, "qualification.retained", "completed");
        assert!(!capture.spans.values().next().unwrap().closed);
    }
    drop(retained);
    let capture = capture.lock().unwrap();
    assert_terminal(&capture, "qualification.retained", "completed");
    assert!(capture.spans.values().next().unwrap().closed);
}

#[lily_trace(name = "qualification.timed")]
async fn timed(release: tokio::sync::oneshot::Receiver<()>) {
    release.await.unwrap();
}

#[tokio::test]
async fn duration_includes_suspension_and_is_in_milliseconds() {
    let (dispatch, capture) = capture();
    let (release, wait) = tokio::sync::oneshot::channel();
    let started = std::time::Instant::now();
    let mut future = Box::pin(timed(wait).with_subscriber(dispatch));
    assert!(futures_util::poll!(&mut future).is_pending());
    let suspended = std::time::Instant::now();
    std::thread::sleep(std::time::Duration::from_millis(15));
    let waited_ms = suspended.elapsed().as_secs_f64() * 1000.0;
    release.send(()).unwrap();
    future.await;
    let total_ms = started.elapsed().as_secs_f64() * 1000.0;
    let capture = capture.lock().unwrap();
    let terminal = assert_terminal(&capture, "qualification.timed", "completed");
    let measured_ms = terminal.fields["lily.duration_ms"].as_f64().unwrap();
    assert!(
        measured_ms >= waited_ms,
        "suspension was not included: {measured_ms} < {waited_ms}"
    );
    assert!(
        measured_ms <= total_ms,
        "wrong units: {measured_ms} > {total_ms}"
    );
}

struct NeverFormat;
impl std::fmt::Debug for NeverFormat {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        panic!("a disabled span must not format its arguments");
    }
}
#[lily_trace(
    name = "qualification.filtered",
    level = "debug",
    fields(secret),
    result
)]
fn filtered(secret: NeverFormat, failure: Failure) -> DomainResult<()> {
    let _ = secret;
    Err(failure)
}
#[lily_trace(
    name = "qualification.environment",
    env = "unsupported-environment",
    result
)]
fn wrong_environment(failure: Failure) -> DomainResult<()> {
    Err(failure)
}

#[test]
fn disabled_and_environment_filtered_methods_do_not_classify_or_write_to_the_parent() {
    let layer = CaptureLayer::default();
    let capture = Arc::clone(&layer.0);
    let dispatch = tracing::Dispatch::new(
        Registry::default()
            .with(tracing_subscriber::EnvFilter::new("info"))
            .with(layer),
    );
    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    tracing::dispatcher::with_default(&dispatch, || {
        let parent = tracing::info_span!(
            "parent",
            lily.outcome = tracing::field::Empty,
            lily.error_code = tracing::field::Empty
        );
        parent.in_scope(|| {
            assert!(filtered(
                NeverFormat,
                Failure {
                    technical: true,
                    classified: Arc::clone(&count)
                }
            )
            .is_err());
            assert!(wrong_environment(Failure {
                technical: true,
                classified: Arc::clone(&count)
            })
            .is_err());
        });
    });
    assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 0);
    let capture = capture.lock().unwrap();
    assert_eq!(capture.spans.len(), 1);
    let span = capture.spans.values().next().unwrap();
    assert_eq!(span.name, "parent");
    assert!(span.fields.is_empty());
    assert!(capture.events.is_empty());
}

#[async_trait]
trait ConcurrentService: Send + Sync {
    async fn call(&self, technical: bool, barrier: Arc<tokio::sync::Barrier>) -> DomainResult<()>;
}

struct ConcurrentImplementation(Arc<std::sync::atomic::AtomicUsize>);

#[async_trait]
impl ConcurrentService for ConcurrentImplementation {
    #[lily_trace(
        name = "qualification.concurrent",
        result,
        fields(technical),
        skip(self)
    )]
    async fn call(&self, technical: bool, barrier: Arc<tokio::sync::Barrier>) -> DomainResult<()> {
        let span = tracing::Span::current().id().unwrap();
        assert_eq!(current_name(), Some("qualification.concurrent"));
        barrier.wait().await;
        tokio::task::yield_now().await;
        assert_eq!(tracing::Span::current().id(), Some(span));
        Err(Failure {
            technical,
            classified: Arc::clone(&self.0),
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_async_trait_calls_keep_results_events_and_request_parents_isolated() {
    const CALLS: usize = 16;
    let (dispatch, capture) = capture();
    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let service: Arc<dyn ConcurrentService> =
        Arc::new(ConcurrentImplementation(Arc::clone(&count)));
    let barrier = Arc::new(tokio::sync::Barrier::new(CALLS));
    let mut tasks = tokio::task::JoinSet::new();
    for index in 0..CALLS {
        let service = Arc::clone(&service);
        let barrier = Arc::clone(&barrier);
        tasks.spawn(
            async move {
                let parent = tracing::info_span!("request", index);
                tracing::Instrument::instrument(
                    service.call(index.is_multiple_of(2), barrier),
                    parent,
                )
                .await
            }
            .with_subscriber(dispatch.clone()),
        );
    }
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while let Some(result) = tasks.join_next().await {
            assert!(result.unwrap().is_err());
        }
    })
    .await
    .expect("all concurrent method calls must finish");
    assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), CALLS);
    let capture = capture.lock().unwrap();
    assert_eq!(capture.spans.len(), CALLS * 2);
    assert_eq!(capture.events.len(), CALLS * 2);
    assert!(capture.spans.values().all(|span| span.closed));
    let mut parents = std::collections::HashSet::new();
    for (id, span) in capture
        .spans
        .iter()
        .filter(|(_, span)| span.name == "qualification.concurrent")
    {
        let parent_id = span
            .parent
            .as_ref()
            .expect("method needs its request parent");
        assert!(parents.insert(parent_id));
        let parent = &capture.spans[parent_id];
        assert_eq!(parent.name, "request");
        let index: usize = parent.fields["index"].as_str().unwrap().parse().unwrap();
        let technical = index.is_multiple_of(2);
        assert_eq!(span.fields["technical"], technical.to_string());
        let expected_code = if technical {
            "service_unavailable"
        } else {
            "invalid_credentials"
        };
        let events = capture
            .events
            .iter()
            .filter(|event| event.span.as_ref() == Some(id))
            .collect::<Vec<_>>();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].fields["lily.lifecycle"], "started");
        assert_eq!(events[1].fields["lily.lifecycle"], "completed");
        assert_eq!(events[1].fields["lily.error_code"], expected_code);
        assert_eq!(span.fields["lily.error_code"], expected_code);
        assert_eq!(
            events[1].fields["lily.outcome"],
            if technical { "error" } else { "rejected" }
        );
    }
    assert_eq!(parents.len(), CALLS);
}

#[lily_trace(name = "qualification.abort", result)]
async fn abortable(ready: tokio::sync::oneshot::Sender<()>) -> DomainResult<()> {
    ready.send(()).unwrap();
    std::future::pending().await
}

#[tokio::test]
async fn tokio_abort_after_start_emits_one_dropped_terminal_and_no_application_outcome() {
    let (dispatch, capture) = capture();
    let (ready, wait) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(abortable(ready).with_subscriber(dispatch));
    tokio::time::timeout(std::time::Duration::from_secs(5), wait)
        .await
        .unwrap()
        .unwrap();
    task.abort();
    let result = task.await;
    assert!(matches!(result, Err(error) if error.is_cancelled()));
    let capture = capture.lock().unwrap();
    let event = assert_terminal(&capture, "qualification.abort", "dropped");
    assert!(!event.fields.contains_key("lily.outcome"));
    assert!(!event.fields.contains_key("lily.error_code"));
    assert!(capture.spans.values().all(|span| span.closed));
}

#[lily_trace(name = "qualification.async_panic", result)]
async fn async_panic() -> DomainResult<()> {
    tokio::task::yield_now().await;
    panic!("async business panic");
}

#[tokio::test]
async fn async_panic_after_suspension_emits_one_panicked_terminal_and_propagates() {
    let (dispatch, capture) = capture();
    let result = tokio::spawn(async_panic().with_subscriber(dispatch)).await;
    assert!(matches!(result, Err(error) if error.is_panic()));
    let capture = capture.lock().unwrap();
    let event = assert_terminal(&capture, "qualification.async_panic", "panicked");
    assert!(!event.fields.contains_key("lily.outcome"));
    assert!(!event.fields.contains_key("lily.error_code"));
    assert!(capture.spans.values().all(|span| span.closed));
}

#[test]
fn lifecycle_events_respect_the_original_module_filter() {
    let layer = CaptureLayer::default();
    let capture = Arc::clone(&layer.0);
    let subscriber = Registry::default()
        .with(tracing_subscriber::EnvFilter::new(format!(
            "off,{}=info",
            module_path!()
        )))
        .with(layer);
    tracing::subscriber::with_default(subscriber, || {
        let _ = unclassified();
    });
    assert_terminal(
        &capture.lock().unwrap(),
        "qualification.unclassified",
        "completed",
    );
}

#[test]
fn manual_record_result_uses_the_application_mapping_without_emitting_lifecycle() {
    let (dispatch, capture) = capture();
    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    tracing::dispatcher::with_default(&dispatch, || {
        let span = tracing::info_span!(
            "manual.result",
            lily.outcome = tracing::field::Empty,
            lily.error_code = tracing::field::Empty,
            otel.status_code = tracing::field::Empty
        );
        let error: Box<dyn lily_trace::TraceResultError + Send + Sync> = Box::new(Failure {
            technical: false,
            classified: Arc::clone(&count),
        });
        let result = Err::<(), _>(Arc::new(error));
        span.in_scope(|| lily_trace::record_result(&result.as_ref()));
        assert!(result.is_err());
    });
    assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 1);
    let capture = capture.lock().unwrap();
    assert_eq!(capture.spans.len(), 1);
    assert!(capture.events.is_empty());
    let span = capture.spans.values().next().unwrap();
    assert_eq!(span.fields["lily.outcome"], "rejected");
    assert_eq!(span.fields["lily.error_code"], "invalid_credentials");
    assert_eq!(span.fields["otel.status_code"], "OK");
}
