# lily_trace

Tracing and OpenTelemetry export for Lilyrs applications. This crate owns
subscriber installation, W3C context propagation, console/file/OTLP output and
bounded telemetry shutdown. It also re-exports the `lily_trace` attribute and
the compatible `tracing` crate; no separate macro dependency is required.

```toml
[dependencies]
lily_trace = "0.1.0"
```

The umbrella path is `lilyrs::trace` with feature `trace`.

## Method instrumentation

```rust
use lily_trace::{lily_trace, TraceFailure, TraceResultError};

enum LoginError { InvalidCredentials, Unavailable }

impl TraceResultError for LoginError {
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

#[lily_trace(name = "login", result)]
async fn login(available: bool) -> Result<(), LoginError> {
    if !available { return Err(LoginError::Unavailable); }
    Err(LoginError::InvalidCredentials)
}
```

Without `result`, the macro records lifecycle and elapsed time without inspecting
the return value. The bare `result` flag opts into `success`, `rejected` or `error`
classification and a static `lily.error_code`. `TraceResultError` is the
application classification trait; `TraceError` is a separate propagation error.
Rejected results have OpenTelemetry status `OK`; technical failures have `ERROR`.

Each enabled, polled invocation emits started/finished events and numeric
`lily.duration_ms` measured with `Instant`. Native async, sync and `async_trait`
methods are supported. A never-polled future emits nothing. Dropped or panicking
work records its lifecycle without inventing an application result.
Arguments are recorded only when named in `fields(...)`; keep secrets and
unbounded request data out of those fields.

## Runtime ownership

HTTP, WebSocket and Consumer builders accept `TracingMode` and normally own
installation and shutdown. `Disabled` is the default; choose `owned_path(...)`
or `owned_config(...)` explicitly, or `External` when a process composition root
already owns tracing. Installing the attribute alone does not install a subscriber.

Standalone hosts use `TracingRuntimeOwner::install`, handle
`TraceInstallOutcome`, and await the owner's bounded `shutdown` report.
`TraceConfig::try_load()` strictly loads `lily_trace.toml`; missing, malformed or
unsupported configuration is an error. `console` is the default Cargo feature,
but exporter activation remains an explicit runtime configuration choice.
The file and console exporters cannot be enabled together; OTLP is independently
configured. Ingestion, storage and dashboards belong to the selected backend.

W3C context helpers and span-preserving task helpers are re-exported at the root.
Lily's runtime supplies trace/span correlation to its file and console exporters
as well as OTLP. See [method tracing](METHOD_TRACING.md) for field semantics,
async-trait expansion, filtering and lifecycle details.

## Documentation and license

Full documentation and canonical application examples: [lilyrs.com](https://lilyrs.com).
Published API reference: [docs.rs/lily_trace](https://docs.rs/lily_trace).

Licensed under either [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
