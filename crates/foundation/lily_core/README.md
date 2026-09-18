# lily_core

Small shared primitives used by the Lilyrs framework. This crate contains
environment classification, HTTP protocol selection, raw-header metadata and
the development diagnostic macro; it is not an application host or a DI container.

```toml
[dependencies]
lily_core = "0.1.0"
```

## Public surface

- `RuntimeEnvironment` and `EnvironmentError`: explicit parsing and process-level
  environment detection through `LILY_ENV`.
- `HttpProtocol`: `Http1_1`, `Http2`, and `Auto` listener policy.
- `RawHeader`: header name, value, original line and line number.
- `debug_log!`: a structured `tracing` DEBUG event enabled in development.

```rust
use lily_core::RuntimeEnvironment;

assert_eq!(
    RuntimeEnvironment::parse("dev").unwrap(),
    RuntimeEnvironment::Development,
);
assert!(RuntimeEnvironment::parse("unknown").is_err());
```

`RuntimeEnvironment::current()` treats absent or invalid `LILY_ENV` as production.
The debug macro also needs a subscriber/filter that accepts DEBUG events.
Application configuration is provided separately by `lily_config`; selecting
`LILY_ENV` does not load a configuration file.

This crate has no optional Cargo features. Most applications receive these
types through the framework components that use them.

## Documentation and license

Full documentation and canonical application examples: [lilyrs.com](https://lilyrs.com).
Published API reference: [docs.rs/lily_core](https://docs.rs/lily_core).

Licensed under either [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
