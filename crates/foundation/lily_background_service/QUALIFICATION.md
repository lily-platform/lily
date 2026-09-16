# Background service qualification

## Coverage

| Boundary | Evidence |
| --- | --- |
| Provider scope | `lily_injection/tests/service_scope.rs`: callback construction and async execution use the same ProcessContext; repeated resolution shares an instance; scopes and reused IDs remain isolated. |
| Cleanup result | Application errors survive successful cleanup; disposal failure takes precedence; panic resumes after disposal; cancelled/unused scopes retain cleanup ownership. |
| Public lifetime | `ServiceScope` compile-fail doctest rejects an escaping `&Extensions`. Existing ApplicationScope tests still pass. |
| Construction and start | Runtime tests prove construction once, registration deduplication, no execution before start, normal one-shot completion, failed/panicking/cancelled constructors. |
| Cooperative shutdown | Workers observe cancellation, can open a finalization scope, and release dependencies before host disposal. |
| Forced shutdown | Cancellation-insensitive workers share one absolute budget; cancelling/replacing a waiter does not extend it. Actual task joins and disposer drops are asserted. |
| Scheduler delay | `tests/deadline_observation.rs` advances time past the execution cutoff before the owner can poll ready work; the joined result remains a missed-deadline failure. |
| HTTP ownership | `lily_http_api/src/app/background_tests.rs`: real bind success/failure, close-before-start, worker faults, abandoned App/build, cancelled start/close waiters, external-container isolation. |
| Unconfirmed termination | A real worker destructor blocks an OS thread. HTTP reports incomplete, leaves DI open, retains the actual join and preserves the frozen report even after release. The fixture explicitly joins its watchdog and worker before teardown. |
| Telemetry ordering | `lily_http_api/tests/background_telemetry.rs`: four isolated processes exercise cooperative/forced shutdown and disposer error/timeout with the real JSONL exporter. Job and cleanup trace IDs match; scope and dependency events precede flush completion. |

## Executed checks

- Broad regression: `cargo test -p lily_injection -p lily_background_service -p lily_http_api --offline` — 533 passed. Its 17 ignored entries were 15 documentation examples and the two environment-dependent tests explicitly executed below.
- Final runtime qualification: `cargo test -p lily_background_service --offline` — 9 passed, including the additional delayed-join scenario.
- Final HTTP qualification: `cargo test -p lily_http_api --lib --test background_telemetry background --offline` — 13 HTTP tests and the exporter test passed; the latter asserts all four subprocess scenarios.
- Existing loopback qualification: `cargo test -p lily_http_api --lib port_in_use_preserves_addr_in_use_and_listener_context --offline -- --ignored` — passed.
- Existing live DI exporter regression: `cargo test -p lily_injection --test resolution_exporters live_collector_preserves_di_filtering_and_shutdown_delivery --offline -- --ignored --nocapture` — passed for INFO and console+OTLP DEBUG. The pinned Collector was stopped, its wire records captured, and its container removed.
- `cargo clippy -p lily_background_service -p lily_injection -p lily_http_api --all-targets --offline -- -D warnings` — passed.
- `cargo fmt -p lily_background_service -p lily_injection -p lily_http_api -- --check` and scoped `git diff --check` — passed.

Counts above are per command and overlap; the runtime/HTTP qualification is also
part of the broad regression command. The final focused runs cover the final
deadline and force-admission behavior.

Local execution logs are retained in `/tmp/lily-background-tests.log`,
`/tmp/lily-background-runtime-final.log`, `/tmp/lily-background-http-final.log`,
`/tmp/lily-background-port.log`, `/tmp/lily-background-collector.log` and
`/tmp/lily-background-clippy.log`.

The live DI Collector evidence is in
`target/otlp-qualification/1655096-1789328818477671955/`, including captured
traces, logs, metrics, Collector version/image identity and shutdown results.
This Collector case qualifies the existing DI exporters; the new background
job/cleanup trace-identity checks use the real file exporter.
