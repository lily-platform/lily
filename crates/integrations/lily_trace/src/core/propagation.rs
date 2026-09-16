use opentelemetry::propagation::{Extractor, Injector, TextMapPropagator};
use opentelemetry::trace::TraceContextExt;
use opentelemetry::Context;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Installs W3C Trace Context as the process-wide text-map propagator.
///
/// Call this once during process startup before accepting HTTP requests or
/// consuming messages.
pub fn install_w3c_propagator() {
    opentelemetry::global::set_text_map_propagator(
        opentelemetry_sdk::propagation::TraceContextPropagator::new(),
    );
}

/// Extracts an OpenTelemetry context from an HTTP or messaging carrier.
#[inline]
pub fn extract_context(carrier: &dyn Extractor) -> Context {
    opentelemetry::global::get_text_map_propagator(|propagator| propagator.extract(carrier))
}

/// Injects `context` into an HTTP or messaging carrier.
#[inline]
pub fn inject_context(context: &Context, carrier: &mut dyn Injector) {
    opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.inject_context(context, carrier);
    });
}

/// Returns the OpenTelemetry context associated with the current tracing span.
#[inline]
pub fn current_context() -> Context {
    tracing::Span::current().context()
}

/// Returns the OpenTelemetry context represented by `span`.
#[inline]
pub fn context_for_span(span: &tracing::Span) -> Context {
    span.context()
}

/// Assigns an extracted remote context as the OpenTelemetry parent of `span`.
/// Assign it before entering the span or creating children. An empty context
/// starts a new root, including when the tracing span had a local parent.
#[inline]
pub fn set_parent(span: &tracing::Span, parent: Context) {
    let is_root = !parent.has_active_span();
    span.set_parent(parent);
    if is_root {
        stabilize_root_identity(span);
    }
}

fn stabilize_root_identity(span: &tracing::Span) {
    use tracing_opentelemetry::OtelData;
    use tracing_subscriber::{registry::LookupSpan, Registry};

    // tracing-opentelemetry 0.31 assigns a builder trace ID only to spans born
    // as roots. Clearing a local parent later leaves it absent, so each
    // sampled_context() call (and final export) could generate a different ID.
    // Keep the first ID supplied by the installed SDK; do not invent another
    // generator or reuse the transport's trace. Existing root IDs are untouched.
    span.with_subscriber(|(id, dispatch)| {
        let Some(registry) = dispatch.downcast_ref::<Registry>() else {
            return;
        };
        let Some(stored) = registry.span(id) else {
            return;
        };
        let needs_identity = stored.extensions().get::<OtelData>().is_some_and(|data| {
            !data.parent_cx.has_active_span() && data.builder.trace_id.is_none()
        });
        if !needs_identity {
            return;
        }

        // Release the extensions lock before context() reenters this span's
        // dispatcher. This function runs at the propagation boundary, never
        // inside a formatter or layer callback.
        let context = span.context();
        let reference = context.span();
        let identity = reference.span_context();
        if identity.is_valid() {
            if let Some(data) = stored.extensions_mut().get_mut::<OtelData>() {
                data.builder.trace_id.get_or_insert(identity.trace_id());
            }
        }
    });
}

/// Injects the current tracing span's OpenTelemetry context into a carrier.
#[inline]
pub fn inject_current_context(carrier: &mut dyn Injector) {
    inject_context(&current_context(), carrier);
}

/// A validated W3C `traceparent` value.
///
/// Prefer [`extract_context`] and [`inject_context`] at protocol boundaries.
/// This owned value is useful when an application must retain a trace context
/// in a request or message object before attaching it to a `tracing` span.
/// Its fields are private so a value cannot become invalid after parsing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct W3CTraceContext {
    version: String,
    trace_id: String,
    parent_id: String,
    trace_flags: String,
}

impl W3CTraceContext {
    /// Parses and validates a W3C `traceparent` header value.
    ///
    /// # Example
    /// ```
    /// use lily_trace::W3CTraceContext;
    ///
    /// let header = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    /// let ctx = W3CTraceContext::from_traceparent(header).expect("valid traceparent");
    ///
    /// assert_eq!(ctx.trace_id(), "4bf92f3577b34da6a3ce929d0e0e4736");
    /// assert_eq!(ctx.parent_id(), "00f067aa0ba902b7");
    /// ```
    pub fn from_traceparent(header: &str) -> Result<Self, TraceError> {
        let mut carrier = std::collections::HashMap::new();
        carrier.insert("traceparent".to_string(), header.to_string());
        let context =
            opentelemetry_sdk::propagation::TraceContextPropagator::new().extract(&carrier);
        let span = context.span();
        let span_context = span.span_context();
        if !span_context.is_valid() {
            return Err(TraceError::InvalidFormat(
                "traceparent does not satisfy the W3C Trace Context rules".to_string(),
            ));
        }

        Ok(Self {
            version: "00".to_string(),
            trace_id: span_context.trace_id().to_string(),
            parent_id: span_context.span_id().to_string(),
            trace_flags: format!("{:02x}", span_context.trace_flags().to_u8()),
        })
    }

    /// Returns the W3C `traceparent` header representation.
    ///
    /// # Example
    /// ```
    /// use lily_trace::W3CTraceContext;
    ///
    /// let ctx = W3CTraceContext::from_traceparent(
    ///     "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    /// ).unwrap();
    /// let header = ctx.to_traceparent();
    /// assert!(header.starts_with("00-"));
    /// ```
    pub fn to_traceparent(&self) -> String {
        format!(
            "{}-{}-{}-{}",
            self.version, self.trace_id, self.parent_id, self.trace_flags
        )
    }

    /// Returns the W3C Trace Context version.
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Returns the 32-character hexadecimal trace identifier.
    pub fn trace_id(&self) -> &str {
        &self.trace_id
    }

    /// Returns the 16-character hexadecimal parent span identifier.
    pub fn parent_id(&self) -> &str {
        &self.parent_id
    }

    /// Returns the two-character hexadecimal trace flags.
    pub fn trace_flags(&self) -> &str {
        &self.trace_flags
    }

    /// Attaches this remote context as the OpenTelemetry parent of `span`.
    pub fn attach_to_span(&self, span: &tracing::Span) -> Result<(), TraceError> {
        let mut carrier = std::collections::HashMap::new();
        carrier.insert("traceparent".to_string(), self.to_traceparent());
        let context =
            opentelemetry_sdk::propagation::TraceContextPropagator::new().extract(&carrier);
        if !context.span().span_context().is_valid() {
            return Err(TraceError::InvalidFormat(
                "traceparent does not satisfy the W3C Trace Context rules".to_string(),
            ));
        }
        span.set_parent(context);
        Ok(())
    }

    /// Captures the valid OpenTelemetry context of the current tracing span.
    ///
    /// Returns `None` when no OpenTelemetry span context is active.
    pub fn from_current_span() -> Option<Self> {
        let context = current_context();
        let span = context.span();
        let span_context = span.span_context();
        if !span_context.is_valid() {
            return None;
        }

        Some(Self {
            version: "00".to_string(),
            trace_id: span_context.trace_id().to_string(),
            parent_id: span_context.span_id().to_string(),
            trace_flags: format!("{:02x}", span_context.trace_flags().to_u8()),
        })
    }
}

impl std::fmt::Display for W3CTraceContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_traceparent())
    }
}

/// Error returned when a W3C `traceparent` value is invalid.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TraceError {
    /// The value does not satisfy the W3C Trace Context grammar or identifier rules.
    #[error("invalid traceparent: {0}")]
    InvalidFormat(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::trace::TracerProvider as _;
    use tracing_subscriber::prelude::*;

    #[test]
    fn test_parse_valid_traceparent() {
        let header = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let ctx = W3CTraceContext::from_traceparent(header).unwrap();

        assert_eq!(ctx.version(), "00");
        assert_eq!(ctx.trace_id(), "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(ctx.parent_id(), "00f067aa0ba902b7");
        assert_eq!(ctx.trace_flags(), "01");
    }

    #[test]
    fn test_to_traceparent() {
        let ctx = W3CTraceContext {
            version: "00".to_string(),
            trace_id: "4bf92f3577b34da6a3ce929d0e0e4736".to_string(),
            parent_id: "00f067aa0ba902b7".to_string(),
            trace_flags: "01".to_string(),
        };

        assert_eq!(
            ctx.to_traceparent(),
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        );
    }

    #[test]
    fn parsed_context_can_be_attached_as_a_parent() {
        let context = W3CTraceContext::from_traceparent(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        )
        .unwrap();
        assert!(context
            .attach_to_span(&tracing::info_span!("child"))
            .is_ok());
    }

    #[test]
    fn rejects_invalid_and_all_zero_w3c_identifiers() {
        assert!(W3CTraceContext::from_traceparent("invalid").is_err());
        assert!(W3CTraceContext::from_traceparent(
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01"
        )
        .is_err());
        assert!(W3CTraceContext::from_traceparent(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01"
        )
        .is_err());
    }

    #[test]
    fn remote_parent_and_child_injection_preserve_trace_identity() {
        install_w3c_propagator();

        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder().build();
        let tracer = provider.tracer("propagation-test");
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::new("info"))
            .with(tracing_opentelemetry::layer().with_tracer(tracer));

        let mut incoming = std::collections::HashMap::new();
        incoming.insert(
            "traceparent".to_string(),
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string(),
        );
        let remote_parent = extract_context(&incoming);

        tracing::subscriber::with_default(subscriber, || {
            let server = tracing::info_span!("http.server", otel.kind = "server");
            set_parent(&server, remote_parent);
            let server_context = context_for_span(&server);
            let server_span = server_context.span();
            let server_span_context = server_span.span_context();

            assert_eq!(
                server_span_context.trace_id().to_string(),
                "4bf92f3577b34da6a3ce929d0e0e4736"
            );
            assert_ne!(
                server_span_context.span_id().to_string(),
                "00f067aa0ba902b7"
            );

            let _server_guard = server.enter();
            let client = tracing::info_span!("http.client", otel.kind = "client");
            let client_context = context_for_span(&client);
            let client_span = client_context.span();
            let client_span_context = client_span.span_context();

            assert_eq!(
                client_span_context.trace_id(),
                server_span_context.trace_id()
            );
            assert_ne!(client_span_context.span_id(), server_span_context.span_id());

            let mut outgoing = std::collections::HashMap::new();
            inject_context(&client_context, &mut outgoing);
            let traceparent = outgoing.get("traceparent").unwrap();
            assert!(traceparent.contains("4bf92f3577b34da6a3ce929d0e0e4736"));
            assert!(traceparent.contains(&client_span_context.span_id().to_string()));
        });
    }
}
