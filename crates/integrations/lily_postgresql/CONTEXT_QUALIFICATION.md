# PgDbContext live qualification

This suite verifies the scoped context with real PostgreSQL and a pool whose
`max_size` is **1**. Each test also has an independent one-connection observer
pool. The observer inspects committed rows, backend activity and advisory locks;
it does not borrow the context's connection or join its transaction. Both
connections must report TLS in `pg_stat_ssl` when a CA bundle is supplied.

The qualification command targets `single,test-support`. It does not migrate
generated CRUD methods, exercise factory integration, or run the separate
`PgDatabaseService::transaction` cancellation suite.

## Running the suite

Use a dedicated, disposable PostgreSQL database with TLS. The test role must be
able to create schemas, tables, functions and deferred constraint triggers,
inspect its sessions, and terminate its own test backends. The full suite
deliberately interrupts queries and terminates individual test connections.
Run it only against that dedicated test database.

Set `LILY_PG_TRANSACTION_TEST_URL` to its connection string and
`LILY_PG_TRANSACTION_TEST_CA` to its PEM CA bundle, then run:

```sh
python3 crates/integrations/lily_postgresql/tests/run_context_qualification.py
```

The runner requires Python 3 and Cargo. It first discovers the ignored live
tests and checks that all 39 required scenarios exist. It then runs the entire
discovered suite three times with four test threads. Every round must execute
the same names with zero failures and zero skipped tests. It stops at the first
failure; a later successful run does not erase an earlier failure.

Each invocation receives a new directory under
`target/pg-context-qualification/`, containing `discovery.log`, individual round
logs and `summary.json`. The report includes the executed test names and counts.
The runner does not print the database URL or save it in its summary.

Optional arguments:

```sh
python3 crates/integrations/lily_postgresql/tests/run_context_qualification.py \
  --offline --rounds 3 --test-threads 4 --timeout-secs 300
```

`--offline` requires Cargo's dependencies to be cached. The 300-second limit
covers each Cargo invocation, including compilation, and kills that invocation's
process group on Unix if it hangs. Test synchronization phases also have bounded
waits. A deadline is a test failure, not a reason to retry until green.

Ordinary checks remain separate:

```sh
cargo test -p lily_postgresql --no-default-features --features single,test-support
cargo clippy -p lily_postgresql --no-default-features --features single,test-support --all-targets -- -D warnings
cargo fmt -p lily_postgresql -- --check
```

The normal `cargo test` command leaves live tests ignored. Only the live runner
provides evidence for the PostgreSQL scenarios below.

## What is asserted

Tests live in [context_tests.rs](src/database/context_tests.rs) and
[production.rs](src/database/context_tests/production.rs), and
[application_errors.rs](src/database/context_tests/application_errors.rs) and
[connection.rs](src/database/context_tests/application_errors/connection.rs). Critical error paths
assert the expected `PgError` variant and fields, rather than accepting any
error. Commit and rollback checks compare exact ordered row IDs through the
observer, including visibility before finalization.

| Boundary | Required behavior |
| --- | --- |
| Lease ownership | A held connection keeps `in_flight` reserved; cancelled acquisition releases its reservation; closing the database waits for held leases. |
| Repository sharing | Queries in one transaction have the same `pg_backend_pid()` and `txid_current()`. A subsequent transaction has a new transaction ID. Healthy finalization reuses the only pooled backend. |
| Commit and rollback | The observer cannot see uncommitted inserts. Success commits exactly the expected rows; application failure and swallowed query failure leave exactly the earlier committed rows. |
| Application error types | Non-Clone, non-Sync application errors retain their variant, payload and allocation identity after rollback. Propagated, explicitly mapped and swallowed query errors all prevent commit. Infrastructure errors convert into the application type; rollback failure/timeout overrides a business error and remains observable during repeated disposal. Even a panic in the framework-triggered `From<PgError>` conversion cannot interrupt rollback or poison a healthy context. |
| Connection callback error types | Custom callback errors retain their payload and allocation identity, while ordinary SQL remains autocommitted. Propagated, mapped and swallowed errors all roll back inside transactions; swallowed custom errors return `TransactionRollbackOnly`. Acquisition failures, cancellation, disposal and callback panics convert into the selected error type. Conversion panics cannot retain a pooled lease or its context admission. |
| Concurrent queries | Two tests each run eight rounds of 16 simultaneous callers. Exactly one callback enters; all 15 others return `ContextBusy`. Another transaction is rejected with the error appropriate to the context state. |
| Token precedence | The transaction's `Some(token)` or `None` overrides a repository token. Cancelling the repository token cannot cancel a live transaction. Cancelling the transaction's token is observed by the callback's selected view and rolls back. |
| Ordinary cancellation | The callback receives its own cancellation view. In-flight SQL is cancelled and its backend discarded. Statements already autocommitted remain committed. |
| Acquisition | Ordinary work and transactions time out with `PoolTimeout { phase: Acquire }`; their callbacks never execute. Reservations drain and the context accepts subsequent work. Cancellation while waiting also releases its reservation. |
| Native BEGIN | Cancellation delivered at Diesel's `BeginTransaction` instrumentation event prevents the work callback. The native event sequence is exactly `begin`, `rollback`; the interrupted connection is discarded. |
| Caller drop | Dropping an awaiting transaction caller during blocked SQL still completes rollback through the owner task. Dropping it after COMMIT has started preserves the eventual committed result in PostgreSQL. |
| COMMIT boundaries | A deferred trigger holds COMMIT at a server-observed lock. Late cancellation cannot interrupt it. Disposal's deadline starts at disposal, and an expired disposal wait does not prematurely release the connection. A deferred constraint violation at COMMIT returns the exact constraint error despite a successful work callback. |
| Cleanup failure | An unusable cancellation transport forces the independent rollback deadline to expire. A terminated backend forces a native rollback failure. Both discard the connection and close the context, preserving the cleanup error. |
| Escaped query future | A retained, unpolled query prevents safe finalization; timeout closes the context while its lease remains reserved until the future is dropped. |
| Panic and invalid native state | Panics and manual corruption of the native transaction state cannot produce a successful commit or recycle a dirty connection. |
| Real DI scopes | Injected repositories share one context within a scope and different contexts across scopes. Owner tasks preserve `ProcessContext` and dynamic scoped resolution. Closing one scope rolls back its blocked SQL and allows an independent waiting scope to proceed. Dropping a scope also triggers disposal without an explicit `context.dispose()` call. Retained service references reject new work after closure. |

The contention tests use barriers and hold the winning callback open until every
loser returns. SQL and COMMIT readiness are observed through PostgreSQL advisory
locks and `pg_stat_activity`. Elapsed time is not used to guess when a query has
started; timing assertions are reserved for timeout semantics.

## Coverage limits

The native-BEGIN test pauses the real Diesel instrumentation callback at manager
entry. It is not a simulation of a network outage after a BEGIN packet is sent.
The cancellation-transport test removes the test runtime's cancellation TLS
configuration; it tests failed cancellation and bounded cleanup, not all network
failure modes. These tests do not establish behavior under process termination,
PostgreSQL restart, arbitrary network partitions, or production load, and they
are not performance benchmarks. A successful run establishes the asserted
boundaries on the tested PostgreSQL installation.
