# Lilyrs

Lilyrs is a Rust workspace for application infrastructure, framework components,
and integrations.

- `crates/foundation`: shared infrastructure and application contracts.
- `crates/framework`: HTTP, WebSocket, and consumer frameworks.
- `crates/integrations`: database, messaging, and other integrations.
- `crates/lilyrs`: the application-facing facade.

See individual crate READMEs and API documentation for available functionality.
The [connected examples](examples/README.md) run HTTP, WebSocket and Consumer
applications with shared DI services, real databases, messaging and facade clients.
The [facade qualification guide](tests/qualification/FACADE.md) describes the
compatibility, regression and live example checks.

## License

Lily is dual-licensed under either the [MIT License](LICENSE-MIT) or the
[Apache License, Version 2.0](LICENSE-APACHE), at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in Lily by you shall be dual-licensed as above, without any
additional terms or conditions.
