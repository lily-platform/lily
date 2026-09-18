# lily_process

Task-local request and job context for Lilyrs. `ProcessContext` associates a
process identifier and string metadata with an async future, supporting DI scope
selection without depending on which executor thread polls the future.

```toml
[dependencies]
lily_process = "0.1.0"
```

```rust
use lily_process::ProcessContext;

async fn run_job() {
    let context = ProcessContext::new();
    let id = context.process_id;
    ProcessContext::scope(context, async move {
        assert_eq!(ProcessContext::current().unwrap().process_id, id);
    }).await;
}
```

`new()` assigns a process-local numeric ID. `with_process_id()` accepts a
caller-owned ID, while `with_metadata()` adds application metadata. `current()`
returns `None` outside an active context; `current_async()` and
`current_process_id()` provide convenience accessors.

Nested scopes restore their outer context, and dropping the scoped future
clears its task-local value. New Tokio tasks do not automatically inherit the
context: explicitly scope a spawned future when propagation is required.
This identifier is not an OpenTelemetry trace ID.

`ProcessContext::scope` alone does not create or dispose a DI service scope.
Use the managed scope APIs in `lily_injection` for that lifecycle; HTTP,
WebSocket and Consumer hosts normally establish it for application work.
This crate has no optional Cargo features.

## Documentation and license

Full documentation and canonical application examples: [lilyrs.com](https://lilyrs.com).
Published API reference: [docs.rs/lily_process](https://docs.rs/lily_process).

Licensed under either [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
