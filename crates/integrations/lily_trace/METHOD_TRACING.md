## Method tracing

Use `#[lily_trace]` for a method span, elapsed time, and lifecycle events. Add
the bare `result` flag when the application explicitly wants its return value
classified. Existing attributes remain valid; they do not acquire a new error
trait requirement.

```rust
use lily_trace::{lily_trace, TraceFailure, TraceResultError};

enum AuthenticationError {
    InvalidCredentials,
    Unavailable,
}

impl TraceResultError for AuthenticationError {
    fn trace_failure(&self) -> TraceFailure {
        match self {
            Self::InvalidCredentials => TraceFailure::Rejected {
                code: "invalid_credentials",
            },
            Self::Unavailable => TraceFailure::Error {
                code: "service_unavailable",
            },
        }
    }
}

#[lily_trace(name = "iam.session.lock_refresh_identity")]
async fn lock_refresh_identity() {
    // Only lifecycle and timing; any return type can be used here.
}

#[lily_trace(name = "iam.session.refresh", result)]
async fn refresh(available: bool) -> Result<(), AuthenticationError> {
    lock_refresh_identity().await;
    if !available {
        return Err(AuthenticationError::Unavailable);
    }
    Err(AuthenticationError::InvalidCredentials)
}
```

The trait is named `TraceResultError` because the existing public `TraceError`
enum represents W3C propagation/parsing failures. That enum remains available
with its existing meaning. Framework errors are not automatically classified.

| Returned value | `lily.outcome` | `lily.error_code` | OpenTelemetry status |
| --- | --- | --- | --- |
| `Ok(value)` | `success` | absent | `OK` |
| `Err` mapped to `Rejected` | `rejected` | application code | `OK` |
| `Err` mapped to `Error` | `error` | application code | `ERROR` |

Classification is authoritative for the method span's final OpenTelemetry
status, including when an inner error event or the configured event level would
otherwise mark it as failed. `rejected` represents an expected application
decision. This does not change HTTP response statuses or parent span outcomes.

The original result is returned unchanged. Neither result values nor errors
need `Debug`, `Display`, `Clone`, or `std::error::Error`. No extra `Send`, `Sync`,
or `'static` bound is introduced. Implement the trait as a cheap mapping to
static codes; avoid request data or secrets. References, `Box`, `Arc`, and
`Infallible` have forwarding implementations.

`result` accepts `Result<T, E>` and aliases of that type; other return types
produce a compiler error. `E` must implement `TraceResultError` even when the
span is disabled at runtime. Omitting `result` removes the requirement entirely.
`result = true`, `result(...)`, and duplicate flags are rejected.

### `async_trait` methods

```rust
use async_trait::async_trait;
use lily_trace::lily_trace;
use std::convert::Infallible;

#[async_trait]
trait SessionService {
    async fn refresh(&self) -> Result<(), Infallible>;
}

struct Sessions;

#[async_trait]
impl SessionService for Sessions {
    #[lily_trace(name = "iam.session.refresh", result)]
    async fn refresh(&self) -> Result<(), Infallible> {
        tokio::task::yield_now().await;
        Ok(())
    }
}
```

The macro instruments the async body in the `Box::pin(async move { ... })`
produced by `async_trait`. It preserves the generated signature and existing
allocation. Native async functions, ordinary synchronous functions,
`async_trait(?Send)`, and directly returned async/boxed async blocks are
supported. An arbitrary synchronous factory returning a future stored in a
variable is timed as a factory; the macro does not infer arbitrary future
construction or rewrite unrelated async blocks.

### Events and timing

Each enabled invocation emits one `lily.method.started` event and one
`lily.method.finished` event. Events use the method's configured level and
original module target so existing module filters continue to apply. Their
fields are:

| Field | Started event | Finished event |
| --- | --- | --- |
| `lily.operation` | static span name | static span name |
| `lily.lifecycle` | `started` | `completed`, `dropped`, or `panicked` |
| `lily.duration_ms` | absent | numeric elapsed milliseconds (`f64`) |
| `lily.outcome` | absent | final classification when `result` is enabled |
| `lily.error_code` | absent | code of a classified error |

The span declares `lily.lifecycle`, `lily.duration_ms`, `lily.outcome`,
`lily.error_code`, and `otel.status_code` before execution, then records the final
values. `lily.instrumentation = "method"` is reserved for macro-managed spans.
`lily.lifecycle` stays empty on the span until it terminates; the start event
carries `started`. This keeps the exported span's lifecycle attribute unique.
Terminal events carry their own outcome, code, and duration, including in OTLP
logs; consumers need not rely on span fields being copied into log attributes.

`Instant` measures elapsed wall time starting at execution (the first poll for
async code), including suspension. The measurement excludes waiting before the
future's first poll. Completion is recorded before the result is returned;
retaining a cloned span or spawning a child task does not delay this method's
duration or produce a second completion event.

A future dropped after it starts, including Tokio task abort, gets a terminal
`dropped` event. Rust unwinding produces `panicked` and propagates the panic.
Neither path fabricates an application outcome or code. A cancellation returned
as an ordinary `Err` follows the application's `TraceResultError` mapping.
Never-polled futures have no lifecycle; process termination or `panic=abort`
cannot run drop cleanup. Span and event filtering, sampling, and bounded exporter
queues still apply to delivery.

The Lily console formatter omits its synthetic close event for macro-managed
spans, which already have a terminal event. Ordinary manual span close records
are preserved. A custom subscriber using `FmtSpan::CLOSE` controls its own
synthetic events and may also emit a close record.

Lily's file and console profiles also write the event's W3C `trace_id`,
`span_id`, and `trace_flags` automatically. Use `(trace_id, span_id)` to pair
each invocation's lifecycle events when concurrent methods share a name.
The same SDK identity correlates OTLP logs and spans. No additional macro flag
is required. Events outside a valid span omit these fields. See
`OTLP_TRACE_LOG_GUIDE.md` for output shape, sampling, and external subscribers.

### Manual recording and compatibility

`record_result(&result)` records only on the current span. Declare its fields
first and call it once with that span's final result. It does not end a method
or supply classification to a later macro terminal event; use the `result`
flag for automatic, consistent span/event classification. Tracing cannot clear
an earlier field value, so repeated manual calls cannot erase an old error code.

The existing `name`, `level`, `fields`, `skip`, `env`, and `crate_path` options
remain supported. Values are only formatted when explicitly listed in `fields`.
Disabled spans and environment exclusions skip lifecycle measurement and result
classification, without writing into an enclosing span.

Existing applications require no source migration. Telemetry volume does
change: enabled macro calls now emit the lifecycle pair, including calls without
`result`. Manual HTTP/queue/WebSocket spans and native `tracing::instrument`
uses retain their own instrumentation contracts. Facades that re-export the
`lily_trace` crate continue to work with `crate_path`; the macro and runtime
crate must be updated together.

The real Collector integration test and its execution/evidence contract are
documented in `LIVE_OTLP_QUALIFICATION.md` next to this guide.
