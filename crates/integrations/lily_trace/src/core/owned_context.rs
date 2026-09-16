//! Trace identity and dispatcher propagation for framework-owned task futures.
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use opentelemetry::trace::TraceContextExt;
use tracing::{Dispatch, Span};

/// Retains parent IDs and dispatcher without extending the parent span lifetime.
#[derive(Clone, Debug)]
pub struct TraceContextSnapshot {
    parent: opentelemetry::Context,
    dispatch: Dispatch,
}

impl TraceContextSnapshot {
    /// Capture the current tracing owner.
    pub fn capture() -> Self {
        let span = Span::current();
        let context = crate::context_for_span(&span);
        // A non-recording SpanContext carries IDs, flags and tracestate without
        // retaining a tracing Span or an SDK recording span's lifetime.
        let identity = context.span().span_context().clone();
        let parent = if identity.is_valid() {
            opentelemetry::Context::new().with_remote_span_context(identity)
        } else {
            // A context containing an invalid non-recording span still counts
            // as active to OTel. Keep it empty so the SDK generates a root ID.
            opentelemetry::Context::new()
        };
        let dispatch = span
            .with_subscriber(|(_, dispatch)| dispatch.clone())
            .unwrap_or_else(|| tracing::dispatcher::get_default(Clone::clone));
        Self { parent, dispatch }
    }

    /// Create a child under the retained dispatcher and bind both poll and drop.
    /// The span factory must use `parent: None`; the captured parent is attached
    /// before the future runs or creates children.
    pub fn bind<F: Future>(
        &self,
        future: F,
        create_span: impl FnOnce() -> Span,
    ) -> ContextFuture<F> {
        let span = tracing::dispatcher::with_default(&self.dispatch, || {
            // The creation task may now belong to another request or to
            // shutdown. Neither is the parent of this generation's cleanup.
            let span = create_span();
            crate::set_parent(&span, self.parent.clone());
            span
        });
        ContextFuture {
            inner: Some(future),
            span,
            dispatch: self.dispatch.clone(),
        }
    }
}

pin_project_lite::pin_project! {
    /// Framework-owned future with dispatch and span restored on poll and drop.
    pub struct ContextFuture<F> {
        #[pin]
        inner: Option<F>,
        span: Span,
        dispatch: Dispatch,
    }

    impl<F> PinnedDrop for ContextFuture<F> {
        fn drop(this: Pin<&mut Self>) {
            let mut this = this.project();
            // WithSubscriber restores the dispatcher on poll only. Destructors
            // and macro lifecycle events also need it when the owned task is
            // aborted, including before its first poll. No guard crosses await.
            tracing::dispatcher::with_default(this.dispatch, || {
                this.span.in_scope(|| this.inner.set(None));
            });
        }
    }
}

impl<F: Future> Future for ContextFuture<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        tracing::dispatcher::with_default(this.dispatch, || {
            this.span.in_scope(|| {
                this.inner
                    .as_pin_mut()
                    .expect("cleanup future exists until drop")
                    .poll(cx)
            })
        })
    }
}
