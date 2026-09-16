//! Real SDK checks across the container's owned cleanup task boundary.
use std::time::Duration;

use lily_injectable_derive::Injectable;
use lily_injection::{
    ApplicationContainer, ApplicationScope, InjectionError, ProcessContext, ServiceTrait,
};
use opentelemetry::trace::{SpanId, TraceContextExt, TraceId, TracerProvider as _};
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider, SpanData};
use tokio::sync::Notify;
use tracing::{Instrument, instrument::WithSubscriber};
use tracing_subscriber::prelude::*;

static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

static DISPOSING: Notify = Notify::const_new();
static RELEASE: Notify = Notify::const_new();

#[derive(Default, Injectable)]
#[service(lifetime = "Scoped")]
struct Resource {
    process_id: u64,
    mode: String,
}

struct DisposalDrop(u64);
impl Drop for DisposalDrop {
    fn drop(&mut self) {
        assert_eq!(ProcessContext::current().unwrap().process_id, self.0);
        tracing::info!(probe = "disposal_dropped", process_id = self.0);
    }
}

#[async_trait::async_trait]
impl ServiceTrait for Resource {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        let context = ProcessContext::current().unwrap();
        self.process_id = context.process_id;
        self.mode = context.metadata["mode"].clone();
        Ok(())
    }

    #[lily_trace::lily_trace(name = "test.scope.dispose")]
    async fn dispose(&self) -> Result<(), InjectionError> {
        assert_eq!(
            ProcessContext::current().unwrap().process_id,
            self.process_id
        );
        let _drop = DisposalDrop(self.process_id);
        tracing::info!(probe = "dispose", process_id = self.process_id);
        if self.mode == "block" {
            DISPOSING.notify_one();
            RELEASE.notified().await;
        }
        if self.mode == "error" {
            return Err(InjectionError::DisposeError(
                "intentional disposal failure".into(),
            ));
        }
        Ok(())
    }
}

async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .expect("cleanup test deadline")
}

struct Owner {
    scope: ApplicationScope,
    trace: TraceId,
    span: SpanId,
}

async fn open(
    container: &ApplicationContainer,
    dispatch: &tracing::Dispatch,
    id: u64,
    mode: &str,
) -> Owner {
    let span = tracing::dispatcher::with_default(
        dispatch,
        || tracing::info_span!(parent: None, "test.scope.owner"),
    );
    let context = lily_trace::context_for_span(&span);
    let trace = context.span().span_context().trace_id();
    let span_id = context.span().span_context().span_id();
    let scope = async {
        let scope = container
            .create_scope(
                ProcessContext::with_process_id(id).with_metadata("mode".into(), mode.into()),
            )
            .unwrap();
        scope
            .run(async {
                drop(
                    container
                        .services()
                        .get_service::<Resource>(None)
                        .await
                        .unwrap(),
                );
            })
            .await
            .unwrap();
        scope
    }
    .instrument(span)
    .with_subscriber(dispatch.clone())
    .await;
    Owner {
        scope,
        trace,
        span: span_id,
    }
}

fn owner_exported(exports: &InMemorySpanExporter, owner: &Owner) {
    assert_eq!(
        exports
            .get_finished_spans()
            .unwrap()
            .iter()
            .filter(|span| span.span_context.span_id() == owner.span)
            .count(),
        1,
        "retaining a scope must not keep the request/owner span open"
    );
}

fn verify(exports: &InMemorySpanExporter, owner: &Owner, lifecycle: &str) {
    verify_ids(exports, owner.trace, owner.span, lifecycle);
}

fn verify_ids(exports: &InMemorySpanExporter, trace: TraceId, parent: SpanId, lifecycle: &str) {
    let spans = exports.get_finished_spans().unwrap();
    let owned: Vec<_> = spans
        .iter()
        .filter(|span| span.span_context.trace_id() == trace)
        .collect();
    let exactly = |name: &str| -> &SpanData {
        let matches: Vec<_> = owned.iter().filter(|span| span.name == name).collect();
        assert_eq!(
            matches.len(),
            1,
            "one {name} in the owner's trace: {owned:#?}"
        );
        matches[0]
    };
    let cleanup = exactly("di.scope.dispose");
    assert_eq!(cleanup.parent_span_id, parent);
    let dispose = exactly("test.scope.dispose");
    assert_eq!(dispose.parent_span_id, cleanup.span_context.span_id());
    let terminal: Vec<_> = dispose
        .attributes
        .iter()
        .filter(|kv| kv.key.as_str() == "lily.lifecycle")
        .collect();
    assert_eq!(terminal.len(), 1);
    assert_eq!(terminal[0].value.as_str(), lifecycle);
    for probe in ["dispose", "disposal_dropped"] {
        assert_eq!(
            dispose
                .events
                .iter()
                .filter(|event| event
                    .attributes
                    .iter()
                    .any(|kv| kv.key.as_str() == "probe" && kv.value.as_str() == probe))
                .count(),
            1,
            "event {probe} must use the original dispatcher, including on abort"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cleanup_keeps_owner_identity_across_close_drop_shutdown_reuse_and_cancelled_waiter() {
    let _lock = TEST_LOCK.lock().await;
    let exports = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exports.clone())
        .build();
    let dispatch = tracing::Dispatch::new(
        tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("scope-telemetry"))),
    );
    // No global subscriber: spawned cleanup must explicitly restore its owner.
    let container = ApplicationContainer::builder().build().await.unwrap();
    let mut first = open(&container, &dispatch, 7001, "ok").await;
    let mut second = open(&container, &dispatch, 7002, "ok").await;
    owner_exported(&exports, &first);
    owner_exported(&exports, &second);
    bounded(async {
        tokio::try_join!(first.scope.close(), second.scope.close()).unwrap();
    })
    .await;
    verify(&exports, &first, "completed");
    verify(&exports, &second, "completed");

    let dropped = open(&container, &dispatch, 7003, "block").await;
    owner_exported(&exports, &dropped);
    let Owner { scope, trace, span } = dropped;
    // Drop under another subscriber/trace, then join via container close.
    bounded(tokio::spawn(async move {
        drop(scope);
    }))
    .await
    .unwrap();
    bounded(DISPOSING.notified()).await;
    RELEASE.notify_one();
    bounded(container.close()).await.unwrap();
    verify_ids(&exports, trace, span, "completed");

    let container = ApplicationContainer::builder().build().await.unwrap();
    let waiting = open(&container, &dispatch, 7010, "block").await;
    let Owner {
        mut scope,
        trace,
        span,
    } = waiting;
    let waiter = tokio::spawn(async move { scope.close().await });
    bounded(DISPOSING.notified()).await;
    waiter.abort();
    assert!(bounded(waiter).await.unwrap_err().is_cancelled());
    assert_eq!(
        exports
            .get_finished_spans()
            .unwrap()
            .iter()
            .filter(|s| s.name == "test.scope.dispose" && s.span_context.trace_id() == trace)
            .count(),
        0,
        "cancelling the observer cannot terminate owned disposal"
    );
    RELEASE.notify_one();
    bounded(container.close()).await.unwrap();
    verify_ids(&exports, trace, span, "completed");

    let container = ApplicationContainer::builder().build().await.unwrap();
    let mut old = open(&container, &dispatch, 7020, "ok").await;
    old.scope.close().await.unwrap();
    let mut replacement = open(&container, &dispatch, 7020, "ok").await;
    assert_ne!(old.trace, replacement.trace);
    old.scope.close().await.unwrap();
    replacement.scope.close().await.unwrap();
    verify(&exports, &old, "completed");
    verify(&exports, &replacement, "completed");
    container.close().await.unwrap();

    let container = ApplicationContainer::builder().build().await.unwrap();
    let mut failure = open(&container, &dispatch, 7021, "error").await;
    assert!(failure.scope.close().await.is_err());
    verify(&exports, &failure, "completed");
    assert!(container.close().await.is_err());
    provider.shutdown().unwrap();
}

#[tokio::test(start_paused = true)]
async fn cleanup_deadline_drops_disposer_under_its_owner_dispatcher() {
    let _lock = TEST_LOCK.lock().await;
    let exports = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exports.clone())
        .build();
    let dispatch = tracing::Dispatch::new(
        tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("scope-timeout"))),
    );
    let container = ApplicationContainer::builder().build().await.unwrap();
    let mut aborted = open(&container, &dispatch, 7030, "block").await;
    owner_exported(&exports, &aborted);
    let closing = aborted
        .scope
        .close_before(tokio::time::Instant::now() + Duration::from_millis(100));
    let (result, ()) = bounded(async { tokio::join!(closing, DISPOSING.notified()) }).await;
    assert!(matches!(
        result,
        Err(InjectionError::ScopeCleanupTimedOut { .. })
    ));
    verify(&exports, &aborted, "dropped");
    assert!(
        container.close().await.is_err(),
        "cleanup timeout remains in the shutdown ledger"
    );
    provider.force_flush().unwrap();
    provider.shutdown().unwrap();
}
