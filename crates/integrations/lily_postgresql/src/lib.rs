#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

//! Diesel-first asynchronous PostgreSQL integration for Lily Framework.
//!
//! Lily owns configuration, DI registration, pool lifecycle, TLS trust,
//! bounded acquisition and scoped connection access. Schemas, entities,
//! relationships, joins and custom queries remain ordinary Diesel.
//!
//! # Choose the access layer
//!
//! A repository is not required. These application designs are
//! supported:
//!
//! - Inject [`PgDatabaseService`] into a domain service and run ordinary Diesel
//!   queries through [`PgDatabaseService::with_connection`] or
//!   [`PgDatabaseService::transaction`]. This is the direct, fully flexible
//!   path.
//! - Derive [`PgRepository`] for an optional repository adapter when its common
//!   entity CRUD methods are useful. Custom repository methods still use
//!   ordinary Diesel through the same scoped callbacks.
//! - Inject scoped [`PgDbContext`] into a workflow and its repositories to share
//!   a transaction without passing a connection through repository methods.
//!
//! The derive does not own the entity schema, register the repository with DI,
//! or replace Diesel's query DSL. [`diesel`] and [`diesel_async`] are re-exported
//! so application models and queries can use the exact versions supported by
//! Lily.
//!
//! # Direct service composition
//!
//! In the default `single` mode, [`PgDatabaseService`] is an injectable
//! singleton. A service may inject it directly without defining a repository:
//!
//! ```ignore
//! use std::sync::Arc;
//! use lily_injectable_derive::Injectable;
//! use lily_injection::ServiceTrait;
//! use lily_postgresql::{PgDatabaseService, PgResult};
//!
//! #[derive(Injectable, Default)]
//! #[service(lifetime = "Singleton")]
//! struct OrderQueryService {
//!     #[inject]
//!     database: Arc<PgDatabaseService>,
//! }
//!
//! impl ServiceTrait for OrderQueryService {}
//!
//! impl OrderQueryService {
//!     async fn load_domain_projection(&self) -> PgResult<Vec<OrderProjection>> {
//!         self.database
//!             .with_connection(|connection, _| Box::pin(async move {
//!                 // Build and execute an ordinary diesel-async query here.
//!                 load_order_projection(connection).await
//!             }), None)
//!             .await
//!     }
//! }
//! ```
//!
//! # Scoped service and repository composition
//!
//! In `single` mode, inject `Arc<PgDbContext>` into scoped services and
//! repositories. All consumers in the same DI scope receive the same context;
//! different scopes receive independent contexts backed by the singleton pool.
//! Construction performs no database I/O.
//!
//! ```no_run
//! use std::sync::Arc;
//! use lily_postgresql::{diesel, diesel_async::RunQueryDsl, ExecutionCancellation,
//!     PgDbContext, PgResult};
//!
//! struct OrderRepository { context: Arc<PgDbContext> }
//! impl OrderRepository {
//!     async fn accept(&self, id: i64) -> PgResult<()> {
//!         self.context.with_connection(|connection, _| Box::pin(async move {
//!             diesel::sql_query("UPDATE orders SET status = 'accepted' WHERE id = $1")
//!                 .bind::<diesel::sql_types::BigInt, _>(id).execute(connection).await?;
//!             Ok(())
//!         }), None).await
//!     }
//! }
//!
//! async fn accept_pair(context: Arc<PgDbContext>, repository: Arc<OrderRepository>,
//!     cancellation: Option<ExecutionCancellation>) -> PgResult<()> {
//!     context.transaction(move |_cancellation| async move {
//!         repository.accept(1).await?;
//!         repository.accept(2).await?;
//!         Ok(())
//!     }, cancellation).await
//! }
//!
//! // Workflows may keep their own typed business errors.
//! enum OrderError {
//!     SubscriptionRequired,
//!     Database(lily_postgresql::PgError),
//! }
//! impl From<lily_postgresql::PgError> for OrderError {
//!     fn from(error: lily_postgresql::PgError) -> Self { Self::Database(error) }
//! }
//! impl OrderRepository {
//!     async fn accept_with_policy(&self, id: i64, subscription_active: bool)
//!         -> Result<(), OrderError> {
//!         self.context.with_connection(|connection, _| Box::pin(async move {
//!             if !subscription_active {
//!                 return Err(OrderError::SubscriptionRequired);
//!             }
//!             diesel::sql_query("UPDATE orders SET status = 'accepted' WHERE id = $1")
//!                 .bind::<diesel::sql_types::BigInt, _>(id).execute(connection).await
//!                 .map_err(lily_postgresql::PgError::from)?;
//!             Ok(())
//!         }), None).await
//!     }
//! }
//! async fn accept_with_policy(context: Arc<PgDbContext>, repository: Arc<OrderRepository>,
//!     subscription_active: bool) -> Result<(), OrderError> {
//!     context.transaction(move |_| async move {
//!         repository.accept(1).await?;
//!         if !subscription_active {
//!             return Err(OrderError::SubscriptionRequired);
//!         }
//!         Ok(())
//!     }, None).await
//! }
//! ```
//!
//! The repository must hold the same context as the workflow. `with_connection`
//! uses the transaction's optional token, including `None`, ahead of its own
//! argument. Outside a transaction it uses the supplied token. Overlapping work
//! on one context returns [`PgError::ContextBusy`]; nested transactions return
//! [`PgError::ContextTransactionActive`]. Query errors make a transaction
//! rollback-only, so catching one and returning `Ok` cannot accidentally commit.
//! Context transaction work and connection callbacks can return `Result<T, E>`
//! with `E: From<PgError> + Send + 'static`. The connection future alias accepts
//! an optional error type: `PgConnectionFuture<'_, T, E>` (default `PgError`).
//! Swallowing a custom callback error still prevents commit and returns
//! [`PgError::TransactionRollbackOnly`]; `PgResult` callbacks retain their first
//! `PgError`. Framework errors are converted outside the query locks, after
//! recording the outcome and, for ordinary operations, releasing the lease.
//! Successful rollback preserves the application's error; cleanup failure takes
//! precedence and is converted into `E` by [`PgDbContext::transaction`].
//!
//! The transaction owner continues cleanup after the caller is dropped. Scope
//! disposal closes admission and waits for that owner; it never closes the
//! singleton pool. An already-started COMMIT is awaited to its actual result.
//! All rollback cleanup uses a separate bounded budget. In factory mode, use
//! `PgDbContext::from(factory.get(name)?)` inside an application-owned scoped
//! service and forward disposal through `ServiceTrait::dispose`.
//!
//! # Optional generated repository
//!
//! A container-managed repository additionally derives `Injectable`, marks its
//! database field with `#[inject]`, declares a lifetime, derives `Default`, and
//! implements `ServiceTrait`. `PgRepository` itself only generates the
//! repository contract and the public `create`, `create_many`, `find_by_id`,
//! `find_by_ids`, `update`, `delete_by_id`, `count` and `exists` methods, plus
//! their matching `*_in` methods for an explicitly supplied [`PgExecutor`].
//!
//! ```ignore
//! use std::sync::Arc;
//! use lily_injectable_derive::Injectable;
//! use lily_injection::ServiceTrait;
//! use lily_postgresql::{PgDatabaseService, PgRepository};
//!
//! #[derive(Default, Injectable, PgRepository)]
//! #[service(lifetime = "Singleton")]
//! #[pg(entity = Order)]
//! struct OrderRepository {
//!     #[inject]
//!     database: Arc<PgDatabaseService>,
//! }
//!
//! impl ServiceTrait for OrderRepository {}
//! ```
//!
//! Batch methods accept caller-owned vectors and do not impose an application
//! batch limit. The application must choose a limit appropriate for its query,
//! PostgreSQL parameter count and request boundary.
//!
//! # Transactions and cancellation
//!
//! [`PgDatabaseService::transaction`] and [`PgRepository::transaction`] take
//! `Option<ExecutionCancellation>` followed by a callback accepting the borrowed
//! connection and the same optional cancellation view. Pass `None` when no
//! execution signal is available. Existing calls migrate from
//! `transaction(|connection| ...)` to `transaction(None, |connection, _| ...)`.
//!
//! ```no_run
//! use lily_postgresql::{diesel, diesel_async::RunQueryDsl, ExecutionCancellation,
//!     PgDatabaseService, PgError, PgResult};
//!
//! async fn update_orders(
//!     database: &PgDatabaseService,
//!     cancellation: Option<ExecutionCancellation>,
//! ) -> PgResult<usize> {
//!     database.transaction(cancellation, |connection, cancellation| Box::pin(async move {
//!         // The optional view can also be forwarded to other cooperative work.
//!         diesel::sql_query("UPDATE orders SET status = 'accepted' WHERE status = 'created'")
//!             .execute(connection).await.map_err(PgError::from)
//!     })).await
//! }
//! ```
//!
//! Diesel commits `Ok` and rolls back `Err`. Cancellation before callback
//! completion chooses rollback and returns [`PgError::TransactionCancelled`]
//! unless cleanup fails. The callback's connection must also be passed to
//! generated `*_in` CRUD methods to enlist them in this transaction.
//!
//! After cancellation, query cancellation and rollback share the pool's
//! `transaction_cleanup_timeout_secs` budget (default: 5 seconds). This is not a
//! query timeout. PostgreSQL cancellation uses the same TLS trust policy as the
//! pool; cancelled connections are discarded so a delayed cancel request cannot
//! affect another caller. [`PgError::TransactionCleanupTimeout`] means server
//! rollback completion was not confirmed within the budget.
//!
//! BEGIN is allowed to finish within that cleanup budget before rollback. Once
//! callback completion starts COMMIT or ordinary error rollback, the signal no
//! longer interrupts finalization: an already-started commit can still succeed.
//! Keep awaiting the method after signalling cancellation. Forcefully dropping
//! its future, including through an outer `select!` or task abort, does not
//! guarantee completed rollback. CPU-bound callback work must still yield.
//!
//! # Feature modes
//!
//! The default `single` feature publishes one [`PgDatabaseService`]. The
//! mutually exclusive `factory` feature publishes one `PgFactory` that owns
//! named child services; those children are not global DI registrations.
//!
//! ```toml
//! lily_postgresql = { version = "0.1", default-features = false, features = ["factory"] }
//! ```
//!
//! In factory mode, direct callers inject `PgFactory` and select a cell with
//! `PgFactory::get`. A generated repository contains an injected
//! `Arc<PgFactory>` plus an unannotated
//! `OnceLock<Arc<PgDatabaseService>>`; its application-owned `ServiceTrait`
//! initializer selects the intended cell and sets that lock. The derive cannot
//! infer which named cell the application wants.
//!
//! # Explicit migrations
//!
//! Lily uses Diesel's normal `diesel.toml`, `schema.rs`, `up.sql` and `down.sql`
//! workflow. DI startup never executes DDL. A deployment or migration binary
//! explicitly calls [`PgDatabaseService::run_pending_migrations`] before the
//! application is promoted.
//!
//! # Compile-time entity contract
//!
//! Entity-only ID methods are checked by Diesel. Supplying the wrong ID type is
//! therefore a compile-time error:
//!
//! ```compile_fail
//! use std::sync::Arc;
//! use lily_postgresql::{diesel, PgDatabaseService, PgRepository};
//! use diesel::prelude::*;
//!
//! diesel::table! {
//!     orders (id) {
//!         id -> BigInt,
//!         status -> Text,
//!     }
//! }
//!
//! #[derive(Queryable, Selectable, Identifiable, Insertable, AsChangeset)]
//! #[diesel(table_name = orders)]
//! #[diesel(check_for_backend(diesel::pg::Pg))]
//! struct Order {
//!     #[diesel(skip_insertion)]
//!     #[diesel(skip_update)]
//!     id: i64,
//!     status: String,
//! }
//!
//! #[derive(PgRepository)]
//! #[pg(entity = Order)]
//! struct OrderRepository {
//!     database: Arc<PgDatabaseService>,
//! }
//!
//! async fn wrong_id(repository: &OrderRepository) {
//!     let _ = repository.find_by_id("not-an-i64").await;
//! }
//! ```
//!
//! A model without Diesel `Insertable` cannot derive a full base CRUD
//! repository:
//!
//! ```compile_fail
//! use std::sync::Arc;
//! use lily_postgresql::{diesel, PgDatabaseService, PgRepository};
//! use diesel::prelude::*;
//!
//! diesel::table! {
//!     orders (id) {
//!         id -> BigInt,
//!         status -> Text,
//!     }
//! }
//!
//! #[derive(Queryable, Selectable, Identifiable, AsChangeset)]
//! #[diesel(table_name = orders)]
//! #[diesel(check_for_backend(diesel::pg::Pg))]
//! struct Order {
//!     #[diesel(skip_update)]
//!     id: i64,
//!     status: String,
//! }
//!
//! #[derive(PgRepository)]
//! #[pg(entity = Order)]
//! struct OrderRepository {
//!     database: Arc<PgDatabaseService>,
//! }
//! ```

#[cfg(all(feature = "single", feature = "factory"))]
compile_error!("features `single` and `factory` are mutually exclusive");

mod database;
mod db_context;
#[cfg(test)]
extern crate self as lily_postgresql;
mod error;
mod executor;
#[cfg(feature = "factory-api")]
mod factory;
#[cfg(any(feature = "single", feature = "factory"))]
mod plan;
mod repository;
#[cfg(any(feature = "single", feature = "factory"))]
mod tls;
mod transaction;

/// Boxed future returned by scoped PostgreSQL connection callbacks.
pub use database::{PgConnectionFuture, PgConnectionLease, PgDatabaseService, PgPoolStatus};
pub use db_context::PgDbContext;
/// Supported Diesel facade used by application schemas, models and queries.
pub use diesel;
/// Supported diesel-async facade used by application query execution.
pub use diesel_async;
pub use error::{PgError, PgPoolTimeoutPhase, PgQueryErrorKind, PgResult, PgTlsErrorKind};
pub use executor::PgExecutor;
#[cfg(feature = "factory-api")]
pub use factory::PgFactory;
/// Shared execution-cancellation view for database integrations.
pub use lily_cancellation::ExecutionCancellation;
/// PostgreSQL configuration model consumed by Lily's configuration service.
pub use lily_config::{PgCellConfig, PgConfig, PgMode, PgPoolConfig, PgTlsConfig, PgTlsMode};
/// Derive macro generating the optional entity repository adapter.
pub use lily_postgresql_derive::PgRepository;
/// Trace macro/runtime path used by generated repository implementations.
#[doc(hidden)]
pub use lily_trace;
pub use repository::PgRepository;
#[doc(hidden)]
pub use repository::PgRepositoryId;
#[doc(hidden)]
pub use repository::find_entity_by_id;
/// Framework integration transaction handle used by opt-in adapters.
#[doc(hidden)]
pub use transaction::PgTransaction;
