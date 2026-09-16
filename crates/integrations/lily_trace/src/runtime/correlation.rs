//! W3C identities shared by log exporters and local formatters.

use opentelemetry::trace::{SpanContext, TraceContextExt};
use opentelemetry_sdk::trace::Tracer;
use tracing::{span::Id, Event, Subscriber};
use tracing_opentelemetry::{OtelData, PreSampledTracer};
use tracing_subscriber::{
    fmt::{format::Writer, FmtContext, FormatEvent, FormatFields},
    layer::Context,
    registry::{LookupSpan, SpanRef},
    Layer,
};

struct ClosingContext(SpanContext);

/// Resolve from registry extensions, never by reentering the dispatcher from
/// a subscriber callback. Use the same tracer as the OTel layer for sampling.
pub(super) fn span_context<S>(span: &SpanRef<'_, S>, tracer: &Tracer) -> Option<SpanContext>
where
    S: for<'lookup> LookupSpan<'lookup>,
{
    let mut extensions = span.extensions_mut();
    if let Some(data) = extensions.get_mut::<OtelData>() {
        let context = tracer.sampled_context(data);
        let reference = context.span();
        let identity = reference.span_context();
        return identity.is_valid().then(|| identity.clone());
    }
    extensions
        .get_mut::<ClosingContext>()
        .map(|value| value.0.clone())
}

/// Install BEFORE the OTel layer: its on_close removes OtelData before the
/// outer fmt layer emits synthetic CLOSE events. Do not cache in on_new_span;
/// protocol boundaries may still assign a remote parent after span creation.
pub(super) struct CloseContextLayer(pub(super) Tracer);

impl<S> Layer<S> for CloseContextLayer
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_close(&self, id: Id, context: Context<'_, S>) {
        if let Some(span) = context.span(&id) {
            if let Some(identity) = span_context(&span, &self.0) {
                span.extensions_mut().insert(ClosingContext(identity));
            }
        }
    }
}

pub(super) struct ConsoleFormat<F> {
    pub(super) inner: F,
    pub(super) tracer: Tracer,
}

impl<S, N, F> FormatEvent<S, N> for ConsoleFormat<F>
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    N: for<'writer> FormatFields<'writer> + 'static,
    F: FormatEvent<S, N>,
{
    fn format_event(
        &self,
        context: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> std::fmt::Result {
        if let Some(identity) = context
            .parent_span()
            .and_then(|span| span_context(&span, &self.tracer))
        {
            write!(
                writer,
                "trace_id={} span_id={} trace_flags={:02x} ",
                identity.trace_id(),
                identity.span_id(),
                identity.trace_flags()
            )?;
        }
        self.inner.format_event(context, writer, event)
    }
}
