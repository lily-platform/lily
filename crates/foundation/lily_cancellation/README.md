# lily_cancellation

Shared, read-only cancellation of an accepted execution. This crate has no
dependency on HTTP, database, or error crates.

```toml
[dependencies]
lily_cancellation = "0.1.0"
```

The facade alternative is feature `cancellation`, imported through
`lilyrs::cancellation`. There are no optional Cargo features. This crate is
useful for library APIs that accept the same read-only execution signal as
Lily hosts without depending on a host.

Application code receives an `ExecutionCancellation` from the execution owner
and observes it with `is_cancelled()` or `cancelled().await`. Clones share the
same signal; cloning or dropping a view cannot cancel the execution. The view
does not expose a cancellation source, raw token, or child-token constructor.

Cancellation requests cooperative completion. It does not itself stop a task,
roll back a transaction, or cancel cleanup. Each component remains responsible
for finishing its own resources.

Framework integrations create and bind signals through the hidden `__private`
construction API. That API also provides an inactive view for objects created
outside a managed execution. An inactive view never signals cancellation.

The HTTP facades re-export this same type as
`lily_web_core::ExecutionCancellation` and `lily_http_api::ExecutionCancellation`.
Database integrations can depend directly on `lily_cancellation`.

## Documentation and license

Full documentation and canonical application examples: [lilyrs.com](https://lilyrs.com).
Published API reference: [docs.rs/lily_cancellation](https://docs.rs/lily_cancellation).

Licensed under either [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
