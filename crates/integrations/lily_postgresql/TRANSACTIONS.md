# Transactions and execution cancellation

This page describes the existing connection-callback transaction API. For scoped
repository sharing without connection parameters, see [PgDbContext](DB_CONTEXT.md).
Its owner uses Diesel directly and also coordinates caller drop and scope disposal.

`with_connection` now takes `(callback, Option<ExecutionCancellation>)`, with
`(connection, cancellation)` callback arguments. The transaction signatures below
remain unchanged.

`PgDatabaseService` stays a singleton holding a connection pool. Each
`transaction()` call acquires its own connection. Diesel owns BEGIN, COMMIT and
ROLLBACK; Lily connects execution cancellation to that lifecycle.

```rust
database.transaction(Some(cancellation), move |connection, cancellation| {
    Box::pin(async move {
        orders.create_in(&mut *connection, order).await?;
        audit.create_in(&mut *connection, event).await?;
        // cancellation: Option<ExecutionCancellation>, sharing the caller's signal.
        Ok(())
    })
}).await?;
```

Pass `None` when there is no execution signal. Both the service and the
`PgRepository::transaction` helper use the same signature. Migrate an existing
`transaction(|connection| ...)` call to `transaction(None, |connection, _| ...)`.
The callback still returns `PgConnectionFuture<'_, T>` (`PgResult<T>` inside
`Box::pin`) and does not gain a `'static` bound. Native Diesel transactions and
the hidden framework transaction-handle APIs keep their own signatures.

`ExecutionCancellation` comes from `lily_cancellation` and is also re-exported by
`lily_postgresql` and the HTTP facades. Pass the view obtained by your action or
execution owner; applications do not construct or cancel the read-only view.
Generated repository `*_in` methods use the supplied transaction connection.
Ordinary CRUD methods acquire another connection and do not enlist implicitly.

## Cancellation boundaries

| Boundary | Behavior |
| --- | --- |
| Before entry | An already-cancelled view returns `PgError::TransactionCancelled`; the callback is not invoked and no connection is acquired. |
| Pool acquisition | Cancellation stops waiting, creating or recycling a connection. No callback is invoked. |
| BEGIN | Diesel is allowed to finish BEGIN and then roll back. If it cannot finish within the cancellation cleanup budget, the connection is discarded. |
| Callback | The callback future is dropped when cancellation is observed. A PostgreSQL query-cancellation request is attempted, then the callback returns an error to Diesel so it executes rollback. |
| Callback completion | The signal is checked again before allowing commit. Cancellation raised by the callback itself, even when it returns `Ok`, chooses rollback. |
| COMMIT or ordinary error rollback already started | Finalization is awaited without token interruption. The actual result is returned; a commit can succeed after cancellation is signalled. |

Without cancellation, `Ok(value)` commits and returns the value; `Err(error)`
rolls back and returns that error, subject to Diesel's rollback-error precedence.
SQL statements use the transaction's borrowed connection. Callback code doing
CPU work must yield for cooperative cancellation to be observed. Dropping a
callback cannot undo side effects it already performed outside this database
transaction or stop tasks it spawned independently.

## Cleanup and errors

The pool policy includes `transaction_cleanup_timeout_secs`, defaulting to
**5 seconds**. Existing configuration files receive the default automatically;
zero is rejected. Configure it under `[postgresql.pool]` or a factory cell's
`pool` section. This limits cancellation cleanup, not normal query duration or
an already-started commit.

The budget starts when the enclosing transaction waiter observes cancellation
before ordinary finalization. Query cancellation and Diesel rollback share this
one budget, independently of the cancelled execution signal. Opening the cancel
connection is bounded by the smaller of `connect_timeout_secs` and half the
cleanup budget, leaving time to attempt rollback even if the cancel request
fails. The pool's TLS mode and CA trust configuration also apply to the separate
PostgreSQL cancel connection.

- `PgError::TransactionCancelled`: cancellation won before commit. If BEGIN
  completed, Diesel rollback was awaited and the connection was checked for
  broken state.
- `PgError::TransactionCleanupTimeout`: cleanup did not complete in time. The
  connection was discarded; server-side rollback completion is unconfirmed.
- A query/rollback failure takes precedence over `TransactionCancelled`. A broken
  transaction manager is reported in `PgQueryErrorKind::Transaction` rather than
  being presented as completed cancellation cleanup.

PostgreSQL does not acknowledge whether a cancel request interrupted a query.
Lily therefore discards connections cancelled during a transaction after the
rollback attempt, even on successful cleanup. A delayed cancel request cannot
then affect another request reusing that connection. Normal successful calls
continue to reuse pooled connections.

Continue awaiting `transaction()` after the execution owner signals cancellation.
An outer `select!`, `timeout`, task abort or panic that drops the entire method is
a different lifecycle: it does **not** guarantee completed asynchronous rollback.
This method uses the caller's task and Diesel's transaction API directly.

References: [Diesel transaction contract](https://docs.rs/diesel-async/latest/diesel_async/trait.AsyncConnection.html#method.transaction),
[PostgreSQL driver cancellation contract](https://docs.rs/tokio-postgres/latest/tokio_postgres/struct.CancelToken.html).

## Live verification

The ignored unit fixtures require a dedicated database and permission to create
schemas and terminate their own test backends. They drop their generated schemas
on success. Set `LILY_PG_TRANSACTION_TEST_URL` to the test database connection
string. For TLS, also set `LILY_PG_TRANSACTION_TEST_CA` to an absolute CA PEM path.

```sh
# Plaintext fixture (the transport-failure test specifically requires TLS):
cargo test -p lily_postgresql --lib database::cancellation_tests::live -- --ignored --skip cleanup_timeout_discards_connection_when_cancel_transport_fails

# TLS fixture, including failed query cancellation and bounded cleanup:
cargo test -p lily_postgresql --lib database::cancellation_tests::live -- --ignored
```

Add `--no-default-features --features factory` before `--` to verify factory mode.
The fixtures check pool waiting, callback cancellation, a real `pg_sleep` query,
same-poll cancellation before commit, cancellation during a deferred commit
trigger, rollback failure, connection reuse/discard and cleanup timeout.
