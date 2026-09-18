# lily_trace_macros

Implementation of the `#[lily_trace]` instrumentation attribute. Applications
import it from `lily_trace` or `lilyrs::trace`; the runtime supplies the generated
code's support paths, so a separate macro dependency is unnecessary.

```toml
[dependencies]
lily_trace = "0.1.0"
```

```rust
use lily_trace::lily_trace;

#[lily_trace(name = "inventory.lookup", level = "debug", fields(product_id))]
async fn lookup(product_id: u64) -> u64 {
    product_id
}
```

## Attribute options

| Option | Meaning |
| --- | --- |
| `name = "..."` | Static span name; defaults to the function name |
| `level = "..."` | `trace`, `debug`, `info`, `warn`, `error`; default `info` |
| `fields(arg, ...)` | Explicitly record named arguments with `Debug` |
| `skip(arg, ...)` | Declare unrecorded arguments; cannot overlap `fields` |
| `result` | Classify `Result<T, E>` through `TraceResultError` |
| `env = "..."` / `env = ["...", "..."]` | Filter by process `LILY_ENV` |
| `crate_path = "..."` | Override runtime-path discovery for facade support |

`result = true` is not accepted: use the bare flag. Without it, the return type
requires no tracing trait. Cargo-renamed runtime dependencies are discovered
automatically. `env` does not inspect `TraceConfig::environment`.

Synchronous functions, native async bodies and `async_trait`-generated boxed
bodies are supported. Timing and lifecycle belong to future execution, not
unpolled construction. Arbitrary future factories stored in variables are not
rewritten as async bodies. Early returns and `?` preserve the original result.
The macro installs no global subscriber; configure the runtime or application host.

This implementation crate has no optional features. Runtime-dependent tests are
in the unpublished `tests/fixtures/macro_contracts/trace` package.

## Documentation and license

Full documentation and canonical application examples: [lilyrs.com](https://lilyrs.com).
Published API reference: [docs.rs/lily_trace_macros](https://docs.rs/lily_trace_macros).

Licensed under either [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
