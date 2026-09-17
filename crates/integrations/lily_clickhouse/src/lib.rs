#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

//! ClickHouse integration for Lily applications.
//!
//! Lily owns validated connection plans, bounded concurrent operations,
//! cancellation, identifier allowlists, bound query values, DI lifecycle and
//! explicit migrations. Applications may choose any of three access layers:
//!
//! - inject [`DatabaseService`] and call its public query methods directly;
//! - derive [`ClickhouseTable`] for a typed table adapter;
//! - optionally derive [`ClickhouseRepository`] as a convenience layer over a
//!   typed table.
//!
//! A repository is never required. Domain services may inject either
//! `DatabaseService` or a generated table directly.
//!
//! # Typed table and optional repository
//!
//! ```ignore
//! use std::sync::Arc;
//! use lily_clickhouse::{
//!     ClickhouseRepository, ClickhouseSchema, ClickhouseTable, DatabaseService,
//! };
//! use lily_injectable_derive::Injectable;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Clone, Serialize, Deserialize, clickhouse::Row, ClickhouseSchema)]
//! #[clickhouse(table = "traces", order_by = "trace_id", engine = "MergeTree()")]
//! struct Trace {
//!     trace_id: String,
//!     duration: u64,
//! }
//!
//! #[derive(Injectable, ClickhouseTable, Default)]
//! #[entity_type(Trace)]
//! #[service(lifetime = "Singleton")]
//! struct TraceTable {
//!     #[inject]
//!     db: Arc<DatabaseService>,
//! }
//!
//! // Optional: a domain service may inject TraceTable instead.
//! #[derive(Injectable, ClickhouseRepository, Default)]
//! #[table_type(TraceTable)]
//! #[entity_type(Trace)]
//! #[service(lifetime = "Singleton")]
//! struct TraceRepository {
//!     #[inject]
//!     table: Arc<TraceTable>,
//! }
//! ```
//!
//! Both table and repository derives generate their own `ServiceTrait`
//! implementation. Do not add a competing manual implementation.
//!
//! # Direct database use
//!
//! [`DatabaseService::insert_one`], [`DatabaseService::select`] and the other
//! public database methods are intentional application API. They require an
//! explicit [`ClickhouseOperationContext`], normally created with
//! [`DatabaseService::operation_context`] so request cancellation is retained.
//! Structural identifiers are validated and values remain query parameters.
//!
//! # Feature modes
//!
//! The default `single` feature registers one `DatabaseService`. The mutually
//! exclusive `factory` feature registers `ClickhouseFactory` for named cells.
//! A factory-mode table injects `Arc<ClickhouseFactory>`, declares
//! `#[cell_name("analytics")]`, and keeps an unannotated
//! `db: Arc<DatabaseService>` field that the generated initializer populates.
//!
//! # Schema and migrations
//!
//! [`ClickhouseSchema`] generates metadata and reviewed migration DDL; it does
//! not mutate schema during application startup. Deployment code creates an
//! ordered [`ClickhouseMigration`] plan and explicitly invokes
//! [`ClickhouseMigrationRunner::apply`]. ClickHouse DDL is not transactional,
//! so failed migrations require an operator-reviewed forward fix or restore.
//!
//! The derive output is compiled in the application crate. Until Lily's
//! umbrella facade owns generated paths, downstream crates using table or
//! repository derives must directly declare the crates named by that generated
//! code (`async-trait`, `lily_error`, `lily_injection` and `serde`). This is a
//! macro ABI constraint, not a second lifecycle or registration API.

#[cfg(all(feature = "single", feature = "factory"))]
compile_error!(
    "lily_clickhouse features `single` and `factory` are mutually exclusive; disable default features before enabling `factory`"
);

mod database;
mod error;
mod lifecycle;
mod migration;
mod operation;
mod options;
mod query_plan;
mod schema;

// Factory module - only available with factory feature
#[cfg(feature = "factory")]
mod clickhouse_factory;

#[cfg(feature = "single")]
pub use database::DatabaseService;

// Factory mode exports
#[cfg(feature = "factory")]
pub use clickhouse_factory::ClickhouseFactory;
#[cfg(feature = "factory")]
pub use database::DatabaseService;

pub use error::ClickhouseError;
pub use lifecycle::ClickhouseShutdownHandle;
#[doc(hidden)]
pub use lily_trace;
pub use migration::{ClickhouseMigration, ClickhouseMigrationReport, ClickhouseMigrationRunner};
pub use operation::{ClickhouseOperationContext, ClickhousePageRequest};
pub use options::ClickhouseClientPlan;
pub use query_plan::{ClickhousePredicate, ClickhouseSelectPlan, ClickhouseSort, ClickhouseValue};
pub use schema::ClickhouseSchemaProvider;
pub use tokio_util::sync::CancellationToken;

pub use lily_clickhouse_derive::{ClickhouseRepository, ClickhouseSchema, ClickhouseTable};
