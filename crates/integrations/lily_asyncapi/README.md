# lily_asyncapi

Transport-neutral AsyncAPI 3.1 document infrastructure for Lilyrs. It provides
bounded document configuration, a Draft 7 schema registry, deterministic
projection and an immutable, attach-once `AsyncApiService<K>` snapshot.

Applications normally use the host's public API. For RabbitMQ, enable
`lily_consumer`'s `asyncapi` feature and configure
`Consumer::builder().asyncapi(config)`. The consumer projects the accepted
queue execution plan and registers `ConsumerAsyncApiService` in its container.
The corresponding queue macro/runtime feature is enabled with it.

After successful startup, resolve that service and read `snapshot()`,
`document()` or `canonical_json()`. The application owns any HTTP endpoint or
file export. This crate does not deploy topology, create network listeners or
provide a public general-purpose operation builder.

## Scope

- The generated document version is exactly AsyncAPI `3.1.0`.
- The document model supports WebSocket and AMQP bindings. The current Consumer
  adapter supplies the AMQP projection from validated topology and handlers.
- WebSocket controller macros currently retain AsyncAPI metadata, but
  `WsAppBuilder` does not yet expose a document-publication API or an `asyncapi`
  Cargo feature. Metadata registration alone does not publish a snapshot.
- `lily_asyncapi` itself has no optional Cargo features. Its hidden projection
  support is for Lily adapters; application code uses host re-exports.

For a standalone library that only needs the public document types:

```toml
[dependencies]
lily_asyncapi = "0.1.0"
```

## Documentation and license

Full documentation and canonical application examples: [lilyrs.com](https://lilyrs.com).
Published API reference: [docs.rs/lily_asyncapi](https://docs.rs/lily_asyncapi).

Licensed under either [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
