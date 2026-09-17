//! Preserve ordinary span diagnostics without duplicating method completion.

use std::fmt;

use tracing::{Event, Subscriber};
use tracing_subscriber::{
    fmt::{format::Writer, FmtContext, FormatEvent, FormatFields},
    registry::LookupSpan,
};

pub(super) struct MethodFormat<F>(pub(super) F);

impl<S, N, F> FormatEvent<S, N> for MethodFormat<F>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
    F: FormatEvent<S, N>,
{
    fn format_event(
        &self,
        context: &FmtContext<'_, S, N>,
        writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        // FmtSpan's synthetic events retain the originating span's metadata.
        // Real events (including method lifecycle events) have event metadata.
        let metadata = event.metadata();
        if metadata.is_span() && metadata.fields().field("lily.instrumentation").is_some() {
            return Ok(());
        }
        self.0.format_event(context, writer, event)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io,
        sync::{Arc, Mutex},
    };
    use tracing_subscriber::{fmt::format::FmtSpan, prelude::*};

    #[derive(Clone, Default)]
    struct Buffer(Arc<Mutex<Vec<u8>>>);

    impl io::Write for Buffer {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[crate::lily_trace(name = "managed.method", crate_path = "crate")]
    fn method() -> tracing::Span {
        tracing::info!("application event");
        tracing::Span::current()
    }

    #[test]
    fn suppresses_only_synthetic_method_events_and_keeps_manual_span_close() {
        let buffer = Buffer::default();
        let writer = buffer.clone();
        let layer = tracing_subscriber::fmt::layer()
            .with_ansi(false)
            .without_time()
            .with_writer(move || writer.clone())
            .with_span_events(FmtSpan::CLOSE)
            .map_event_format(MethodFormat);
        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, || {
            let retained = method();
            // Method completion is already present while the span remains open.
            assert_eq!(
                String::from_utf8(buffer.0.lock().unwrap().clone())
                    .unwrap()
                    .matches("method finished")
                    .count(),
                1
            );
            drop(retained);
            let manual = tracing::info_span!("manual.span");
            manual.in_scope(|| tracing::info!("manual event"));
        });
        let output = String::from_utf8(buffer.0.lock().unwrap().clone()).unwrap();
        let lines: Vec<_> = output.lines().collect();
        assert_eq!(lines.len(), 5, "{output}");
        assert_eq!(output.matches("method started").count(), 1, "{output}");
        assert_eq!(output.matches("method finished").count(), 1, "{output}");
        assert_eq!(output.matches("application event").count(), 1, "{output}");
        let close: Vec<_> = lines
            .iter()
            .filter(|line| line.contains(": close"))
            .collect();
        assert_eq!(close.len(), 1, "{output}");
        assert!(close[0].contains("manual.span"), "{output}");
    }
}
