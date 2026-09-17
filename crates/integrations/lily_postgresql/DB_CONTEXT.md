# Scoped PostgreSQL context

`PgDatabaseService` owns the singleton pool. `PgDbContext` owns the current
operation or transaction of one DI scope. It acquires no connection during
initialization. Services and repositories inject the same `Arc<PgDbContext>`;
repositories always use `with_connection`, whether a transaction exists or not.

```rust
use std::sync::Arc;
use lily_injectable_derive::Injectable;
use lily_injection::ServiceTrait;
use lily_postgresql::{
    diesel, diesel_async::RunQueryDsl, ExecutionCancellation, PgDbContext, PgResult,
};

#[derive(Default, Injectable)]
#[service(lifetime = "Scoped")]
pub struct OrderRepository {
    #[inject]
    context: Arc<PgDbContext>,
}
impl ServiceTrait for OrderRepository {}

impl OrderRepository {
    pub async fn accept(&self, id: i64) -> PgResult<()> {
        self.context.with_connection(|connection, _cancellation| Box::pin(async move {
            diesel::sql_query("UPDATE orders SET status = 'accepted' WHERE id = $1")
                .bind::<diesel::sql_types::BigInt, _>(id)
                .execute(connection).await?;
            Ok(())
        }), None).await
    }
}

#[derive(Default, Injectable)]
#[service(lifetime = "Scoped")]
pub struct OrderService {
    #[inject]
    context: Arc<PgDbContext>,
    #[inject]
    orders: Arc<OrderRepository>,
}
impl ServiceTrait for OrderService {}

impl OrderService {
    pub async fn accept_pair(&self, cancellation: Option<ExecutionCancellation>) -> PgResult<()> {
        let orders = Arc::clone(&self.orders);
        self.context.transaction(move |_cancellation| async move {
            orders.accept(1).await?;
            orders.accept(2).await?;
            Ok(())
        }, cancellation).await
    }
}
```

Calling `orders.accept(1)` alone acquires, uses and releases a connection.
Calling both repository methods inside `accept_pair` uses exactly one connection
and transaction. `Ok` commits; `Err` rolls back. No connection parameter crosses
the service/repository boundary.

A callback error makes the transaction rollback-only. If work swallows that
error and returns `Ok`, the transaction still rolls back. A `PgResult` callback
retains its first query error; a custom error records `TransactionRollbackOnly`.
The recorded error is converted into the workflow's error type. If work maps it and returns
its own application error, that explicit error wins. Handle expected absence
within the query callback, for example with
Diesel's `OptionalExtension::optional()`. Cleanup errors take precedence because
successful rollback could not be confirmed.

## Application errors

`PgDbContext::transaction` and `PgDbContext::with_connection` accept callbacks
returning `Result<T, E>` and return that same result type. The error must
implement `From<PgError> + Send + 'static`. It does not need
`Clone`, `Sync`, `Debug`, `Display`, or `std::error::Error`. Framework failures
such as acquisition, cancellation or finalization errors use `E::from`.
Transactions return application errors intact after successful rollback,
including their variant and owned payload.

`with_connection` callbacks use `PgConnectionFuture<'_, T, E>`, normally created
with `Box::pin`. The alias defaults to `E = PgError`, so existing
`PgConnectionFuture<'_, T>` annotations remain valid. The connection callback
and its success value still need only `Send`, without a new `'static` bound.

```rust
pub enum OrderError {
    SubscriptionRequired { order_id: i64 },
    OrderNotFound { order_id: i64 },
    Database(lily_postgresql::PgError),
}

impl From<lily_postgresql::PgError> for OrderError {
    fn from(error: lily_postgresql::PgError) -> Self {
        Self::Database(error)
    }
}

impl OrderRepository {
    pub async fn accept_if_present(&self, id: i64) -> Result<(), OrderError> {
        self.context.with_connection(|connection, _| Box::pin(async move {
            let affected = diesel::sql_query(
                "UPDATE orders SET status = 'accepted' WHERE id = $1"
            )
                .bind::<diesel::sql_types::BigInt, _>(id)
                .execute(connection).await
                .map_err(lily_postgresql::PgError::from)?;
            if affected == 0 {
                return Err(OrderError::OrderNotFound { order_id: id });
            }
            Ok(())
        }), None).await
    }
}

impl OrderService {
    pub async fn accept_with_policy(
        &self,
        id: i64,
        subscription_active: bool,
        cancellation: Option<ExecutionCancellation>,
    ) -> Result<(), OrderError> {
        let orders = Arc::clone(&self.orders);
        self.context.transaction(move |_cancellation| async move {
            orders.accept(id).await?;
            if !subscription_active {
                // Roll back the preceding write and preserve this business error.
                return Err(OrderError::SubscriptionRequired { order_id: id });
            }
            Ok(())
        }, cancellation).await
    }
}
```

Diesel errors must first become `PgError` when the application implements only
`From<PgError>`: use `.await.map_err(PgError::from)?`. Rust's `?` does not chain
`From<diesel::result::Error>` through `PgError` automatically.

Connection callbacks return their original application error unless cancellation
or another framework stop reason supersedes it. Framework-owned conversions run
after the query lock and permit are released and the failure is recorded.
Outside transactions, the lease and context admission are also released before
conversion. Inside transactions, the owner still performs rollback when work
finishes. Callback errors outside transactions cannot undo autocommitted SQL.

Each callback can use a different error type from the surrounding transaction.
Since arbitrary errors cannot be cloned or converted back to `PgError`, a custom
callback error records `PgError::TransactionRollbackOnly`. Subsequent queries
are rejected with that error. If work propagates or maps the callback error,
successful rollback preserves the error it returns. If work swallows the error
and returns `Ok`, finalization converts the recorded marker into the workflow's
error type. Even a custom error wrapping a database failure uses this marker;
callbacks whose error type is exactly `PgError` retain the original `PgError`.

The transaction's error precedence is:

| Condition | Returned result |
| --- | --- |
| Work succeeds, no recorded failure or stop reason, COMMIT succeeds | `Ok(value)` |
| Work returns `Err(e)`, no stop reason, ROLLBACK succeeds | Original `Err(e)` |
| Work swallows a `PgResult` callback error, returns `Ok`, ROLLBACK succeeds | First recorded `PgError` converted into `E` |
| Work swallows a custom callback error, returns `Ok`, ROLLBACK succeeds | `TransactionRollbackOnly` converted into `E` |
| Cancellation, scope disposal, panic or detached work prevents commit | Framework stop error converted into `E`, unless cleanup fails |
| BEGIN, COMMIT or ROLLBACK fails, or rollback cleanup times out | Framework finalization error converted into `E`; context closes and connection cannot be reused |

A rollback failure takes precedence over the application's error; the original
application error is dropped, not bundled into a new public error type. This
preserves the existing cleanup policy and avoids reporting a business rejection
as though rollback had been confirmed. The context retains the `PgError` cleanup
failure for subsequent disposal. An already-started COMMIT is still awaited to
its actual result despite later cancellation or disposal.

Transaction-boundary conversion happens after the owner finishes its cleanup attempt
and updates the context state. User conversion code does not run under the
context's metadata or connection locks. If cleanup times out while a detached
query still holds the lease, that lease remains reserved and unusable until the
query is dropped. Application code's own `?` and `map_err` conversions inside
work naturally execute as part of that work.

Existing callbacks and enclosing functions typed as `PgResult<T>` remain valid.
When neither the callback nor its surrounding expression identifies the error
type (for example, `transaction(|_| async { Ok(()) }, None).await.unwrap()`), add
`Ok::<_, PgError>(())` or annotate the result as `PgResult<_>`. This also applies
to `with_connection` callbacks. Explicit turbofish calls use
`transaction::<T, E, _, _>(...)` or `with_connection::<T, E, _>(...)`.

This support applies to the scoped context. `PgDatabaseService`, public
`PgConnectionLease` operations, `PgExecutor`, `PgTransaction` and generated
repository contracts retain their `PgError` signatures. Do not encode business
failures as `Ok(Err(e))`: the outer `Ok` is a successful callback result and may
commit.

## Cancellation and call signatures

| API | Signature shape |
| --- | --- |
| Database acquisition | `database.acquire_connection(cancellation)` |
| Database query | `database.with_connection(callback, cancellation)` |
| Lease query | `lease.with_connection(callback, cancellation)` |
| Context query | `context.with_connection(callback, cancellation)` |
| Context transaction | `context.transaction(work, cancellation)` |
| Existing database transaction | `database.transaction(cancellation, callback)` |

Query callbacks receive `(&mut AsyncPgConnection, Option<ExecutionCancellation>)`
and return `PgConnectionFuture<'_, T>` (or `PgConnectionFuture<'_, T, E>` for
context queries), normally using `Box::pin`. Context
transaction work receives only `Option<ExecutionCancellation>` and returns an
ordinary future of `Result<T, E>` with `E: From<PgError> + Send + 'static`.

Migrate previous database/repository queries from
`with_connection(|connection| ...)` to
`with_connection(|connection, _| ..., None)`. `PgRepository::with_connection`
follows the database signature. `PgExecutor` and the existing framework
`PgTransaction` handle keep their one-argument callback contract; the database
executor delegates with `None`.

| Context state | Selected callback token |
| --- | --- |
| No transaction | Token passed to this `with_connection` call |
| Transaction with `Some(token)` | That transaction token; the repository argument is ignored |
| Transaction with `None` | `None`; even an already-cancelled repository argument is ignored |

Cancellation covers pool acquisition and callback execution. An already
cancelled selected view prevents callback invocation. `None` disables external
cancellation, but framework disposal and caller detachment still stop context
work through a separate internal signal. An ordinary query returns
`OperationCancelled`; a transaction cancelled before commit returns
`TransactionCancelled`, unless cleanup fails.

Cancellation outside a transaction cannot undo statements already committed by
PostgreSQL. Cancelling a callback does not undo its other external side effects.
It also does not terminate tasks spawned by application code. Await repository
operations within transaction work; do not detach them or run them concurrently.

## Ownership, admission and finalization

The private pool exposes an owned `PgConnectionLease`, never the pool itself.
The lease owns both the deadpool object and Lily's in-flight reservation.
Healthy Drop returns the connection to the pool. Interrupted callbacks, dirty
transaction state, cleanup failure, or explicit `lease.discard()` permanently
remove it using deadpool's `Object::take`. Its borrowed connection cannot escape
a callback. A cancelled or dropped lease operation cannot run another query.
The acquisition token applies only to acquisition; lease callbacks accept their
own token.

Context metadata uses a short `std::sync::Mutex`. Each admitted operation has
its own identity and a `tokio::sync::Mutex<Option<PgConnectionLease>>` for the
connection. The connection lock covers a query or native transaction command,
never the entire workflow. There is no task or command queue per repository
query. Transactions have one owner task, so their work and result require
`Send + 'static`; clone injected `Arc`s into work. Ordinary connection callbacks
do not gain that bound. This design avoids an async lock for metadata; it is
not a throughput benchmark.

| Phase | Behavior |
| --- | --- |
| Idle | Admit one pooled operation or reserve a transaction before acquisition |
| Ordinary operation | Reject overlapping queries and transaction starts with `ContextBusy` |
| Starting transaction | Acquire and run native BEGIN; repository queries return `ContextBusy` |
| Active transaction | Borrow the same connection for one query at a time; overlapping queries fail immediately |
| Finalizing | Reject new queries; finish the transaction before admitting new work |
| Closed | Reject all new work with `ContextClosed` |

A second or nested context transaction returns `ContextTransactionActive`.
Queries never fall back to another pool connection during a transaction
transition. Already admitted work from an old execution cannot borrow a later
transaction's lease. Do not issue raw BEGIN/COMMIT/ROLLBACK or leave unmatched
native savepoints inside callbacks: the owner requires exactly one transaction
level when finalizing. A detected mismatch closes the context and discards the
connection. Transactions managed outside this context are not automatically
joined.

The owner calls Diesel's `TransactionManager` directly for BEGIN, COMMIT and
ROLLBACK. It does not use `PgDatabaseService::transaction`, its handle methods,
or the implementation in `transaction.rs`.

- Dropping the transaction caller signals the owner, which keeps the connection
  and shutdown reservation until cleanup completes.
- Cancellation during BEGIN retains and awaits that native future within the
  cleanup budget, then rolls back. A timed-out native control future makes the
  connection unusable.
- Cancellation before commit, workflow errors, query errors, panics and dropped
  query futures select rollback. An unfinished separately spawned query also
  prevents commit.
- Once COMMIT starts, it is awaited without interruption. Late cancellation,
  caller drop or scope disposal cannot turn a successful commit into a reported
  rollback. The actual commit result is returned.
- Rollback uses one absolute `transaction_cleanup_timeout_secs` deadline,
  independent of the cancelled view. It includes waiting for the connection,
  driver cancellation where needed, and native rollback. The default is five
  seconds. A normal running COMMIT does not start this timer.
- Driver cancellation follows the pool's TLS policy. Rollback is polled before
  a necessary cancel request is driven; completed idle queries do not need
  cancellation. PostgreSQL cannot acknowledge which command a cancel reached,
  so rollback failure stays an error and cancelled connections are discarded.
- With panic unwinding enabled, callback panics become `OperationPanicked` and
  trigger cleanup. Process abort, runtime shutdown and non-yielding code cannot
  be given an async cleanup guarantee; a dropped dirty lease is discarded.

`TransactionCleanupTimeout` means server rollback was not confirmed within the
budget. The context closes permanently. An abandoned, unpolled query future may
retain its dirty lease and in-flight reservation until that future is dropped;
it can never return that connection to normal reuse.

## DI disposal and task context

With feature `single`, `PgDbContext` is automatically registered as Scoped and
injects Singleton `PgDatabaseService`. Repositories depending on the context
must be Scoped or resolved within a scope as Transient. Different scopes own
different contexts while sharing the pool.

`ServiceTrait::dispose` closes admission and asks the existing owner to stop.
It joins that cleanup; it does not issue a second rollback or close the singleton
pool. Concurrent/repeated disposal shares one deadline and preserves a cleanup
failure. If COMMIT is still running when disposal's deadline expires, disposal
returns `ContextCleanupTimeout` through `InjectionError::DisposeError`; the owner
continues awaiting COMMIT. A later successful commit does not erase the disposal
failure.

The owner task explicitly carries the caller's current Lily `ProcessContext`
and tracing span. Existing injected `Arc`s already point to the same scope's
objects. Carrying the task-local context also makes dynamic
`get_service(None)` calls resolve within that same scope. No new DI scope is
created and the existing scope's lifetime is not extended.

With feature `factory`, there is no implicit context registration because Lily
cannot select a database cell for the application. An application-owned scoped
service injects `PgFactory`, initializes one `PgDbContext::from(factory.get(name)?)`,
and shares that stored context with repositories. Its `dispose()` forwards to
`ServiceTrait::dispose` on the stored context. Do not construct a fresh context
per repository if those repositories must share a transaction.

Generated `PgRepository` ordinary CRUD still uses its selected database service.
It does not automatically enlist in `PgDbContext`. This change supports custom
repository methods using the context; migrating the derive's storage contract
is a separate change. Existing `*_in` methods continue accepting an explicitly
supplied connection or executor.

## Validation

Socket-free tests cover precancellation, admission transitions and disposal's
shared deadline. The [live qualification suite](CONTEXT_QUALIFICATION.md) uses a
one-connection pool and an independent observer to verify backend/transaction
identity, exact committed rows, rollback-only errors, token precedence,
concurrent admission, pool/query cancellation, caller drop, BEGIN/COMMIT
boundaries, cleanup failures and actual DI scope closure. The complete run
requires a dedicated TLS PostgreSQL database.

```sh
cargo test -p lily_postgresql --features test-support
# Set these to an isolated PostgreSQL database and its CA bundle:
# LILY_PG_TRANSACTION_TEST_URL, LILY_PG_TRANSACTION_TEST_CA
python3 crates/integrations/lily_postgresql/tests/run_context_qualification.py
```

The runner verifies that all required scenarios are present and executes each
one in three rounds, stopping at the first failure. It targets `single` mode;
factory and generated CRUD integration remain separate work.

Underlying contracts: [Diesel transaction manager](https://docs.rs/diesel-async/latest/diesel_async/trait.TransactionManager.html),
[deadpool object ownership](https://docs.rs/deadpool/latest/deadpool/managed/struct.Object.html),
[PostgreSQL cancellation limitations](https://docs.rs/tokio-postgres/latest/tokio_postgres/struct.CancelToken.html).
