# lily_config

Strict, immutable configuration for Lilyrs applications. `ConfigService` loads
a TOML document, applies environment overrides, resolves secret/file references,
validates `LilyConfig`, and publishes one `ConfigSnapshot` for the process.

```toml
[dependencies]
lily_config = "0.1.0"
```

The umbrella facade exposes this API as `lilyrs::config` with feature `config`.
HTTP, WebSocket and Consumer builders normally own configuration initialization.
Application services inject `Arc<ConfigService>` from their application's DI
container. DI APIs come from `lily_injection` or a framework crate, not from
`lily_config`.

## Standalone loading

```rust
use lily_config::{ConfigError, ConfigOptions, ConfigService};

async fn server_port() -> Result<u16, ConfigError> {
    let config = ConfigService::new(
        ConfigOptions::production("/etc/lily/lily.toml")
            .require_key("server.port"),
    );
    config.load().await?;
    config.get("server.port").await
}
```

Run asynchronous loading on Tokio. When DI constructs the default service,
set both `LILY_CONFIG_PATH` and `LILY_CONFIG_MODE`; the latter accepts
`development`, `test`, or `production`.

```toml
[server]
host = "127.0.0.1"
port = 8080

[custom]
enable_audit = true
```

Overrides use double underscores for nesting, for example
`LILY__SERVER__PORT=9090`. A single underscore stays in the field name.
`${secret:key}` values are resolved by an application-provided `SecretResolver`;
`${file:/absolute/path}` reads a protected file reference. No vendor-specific
secret client is installed automatically.

Unknown typed fields and invalid configured values are errors. `[database]`
configures MongoDB; PostgreSQL uses `[postgresql]`. Runtime reload is unsupported:
restart the application to publish a new snapshot. Use
`redacted_effective_config()` for diagnostics instead of logging raw values.

## Features

No features are enabled by default. `transactional-inbox-mongodb` and
`transactional-inbox-postgresql` enable their queue configuration schema. Each
transactional queue must explicitly select its storage backend; enabling a
feature does not infer a backend or install the database adapter.

## Documentation and license

Full documentation and canonical application examples: [lilyrs.com](https://lilyrs.com).
Published API reference: [docs.rs/lily_config](https://docs.rs/lily_config).

Licensed under either [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
