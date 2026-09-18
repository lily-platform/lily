# lily_postgresql

Asynchronous PostgreSQL integration for Lilyrs using Diesel and diesel-async.
Lily owns configuration, connection pooling, TLS, cancellation and lifecycle;
schemas, entities and queries remain ordinary Diesel code.

```toml
[dependencies]
lily_postgresql = "0.1.0"
lily_injection = "0.1.0"
```

The umbrella path is `lilyrs::postgresql` with feature `postgresql`.
`diesel`, `diesel_async` and the optional `PgRepository` derive are re-exported;
an additional `lily_postgresql_derive` dependency is unnecessary.

## Choose connection ownership

- `PgDatabaseService` is the singleton pool owner. `with_connection(operation,
  cancellation)` lends a pooled connection for one callback.
- `PgDbContext` is a lazy scoped service. Inject the same `Arc<PgDbContext>` into
  a workflow and its repositories to share a transaction without connection
  parameters on repository methods.
- `PgRepository` generates optional entity CRUD and explicit-executor `*_in`
  methods. Its ordinary CRUD methods use the database service and do not join a
  context transaction implicitly.

```rust
use lily_postgresql::{diesel, diesel_async::RunQueryDsl, PgDbContext, PgResult};

async fn ping(context: &PgDbContext) -> PgResult<()> {
    context.with_connection(|connection, _cancellation| Box::pin(async move {
        diesel::sql_query("SELECT 1").execute(connection).await?;
        Ok(())
    }), None).await
}
```

`context.transaction(work, cancellation)` runs a callback receiving only the
optional cancellation view. Repository calls through that same context use its
active connection. Outside a transaction, each operation obtains and releases a
pooled connection. Within a transaction, the transaction's token takes precedence
over each operation's token, including when the transaction token is `None`.

Overlapping operations on one context return `ContextBusy`; nested transactions
return `ContextTransactionActive`. Failed operations make the transaction
rollback-only. Both context callbacks support `Result<T, E>` with
`E: From<PgError> + Send + 'static`. Successful rollback preserves the application
error; a cleanup failure takes precedence and is converted to `E`.

The context's transaction owner retains cleanup if its caller is dropped, and
scope disposal waits for that owner. An already-started commit is awaited to its
actual result. This is separate from the lower-level
`database.transaction(cancellation, operation)` API, whose callback receives the
borrowed connection and token. Keep awaiting that lower-level call after
signalling cancellation; forcefully dropping it does not guarantee completed
rollback. Cleanup is bounded independently of the cancelled execution signal.

## Features and configuration

The default `single` feature registers `PgDatabaseService` and scoped
`PgDbContext`, using `[postgresql]` configuration. Named databases use `PgFactory`:

```toml
lily_postgresql = { version = "0.1.0", default-features = false, features = ["factory"] }
```

`single` and `factory` are mutually exclusive. Factory applications select a
database with `PgFactory::get`. To use a context there, own
`PgDbContext::from(factory.get(name)?)` inside an application-scoped service and
forward its `ServiceTrait::dispose`. `factory-api` exposes the factory API without
selecting the factory DI registration. `test-support` exposes qualification hooks.

`PgConnectionLease` and `acquire_connection` support explicit ownership without
exposing the pool; dropping the lease releases its ownership and shutdown count.
Transactions and pooled operations still need their normal completion/cleanup
protocol before a connection is reusable.

## Migrations

Use Diesel's `schema.rs`, `diesel.toml` and SQL migrations. A deployment or
migration executable explicitly calls `run_pending_migrations`; DI startup
does not perform DDL. Repository batch methods do not impose an application
batch size, so enforce limits appropriate for your queries and request boundary.

## Documentation and license

Full documentation and canonical application examples: [lilyrs.com](https://lilyrs.com).
Published API reference: [docs.rs/lily_postgresql](https://docs.rs/lily_postgresql).

Licensed under either [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
