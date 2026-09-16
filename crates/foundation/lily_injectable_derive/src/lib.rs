#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

//! Derive macros for Lily's dependency-injection composition model.
//!
//! Application services normally import `Injectable` and `ServiceTrait` from
//! `lily_injection`. The derive publishes link-time registration
//! metadata and generates constructor injection; the trait defines the
//! instance lifecycle.
//!
//! # Canonical service
//!
//! ```
//! use std::sync::Arc;
//! use lily_injection::{Injectable, ServiceTrait};
//!
//! #[derive(Default, Injectable)]
//! #[service(lifetime = "Singleton")]
//! struct Clock;
//!
//! impl Clock {
//!     fn now(&self) -> &'static str {
//!         "now"
//!     }
//! }
//!
//! impl ServiceTrait for Clock {}
//!
//! // Every field marked #[inject] must be Arc<T> or Arc<dyn Trait>.
//! // A dependency-only named struct does not need Default.
//! #[derive(Injectable)]
//! #[service(lifetime = "Singleton")]
//! struct AuditService {
//!     #[inject]
//!     clock: Arc<Clock>,
//! }
//!
//! impl AuditService {
//!     fn timestamp(&self) -> &'static str {
//!         self.clock.now()
//!     }
//! }
//!
//! impl ServiceTrait for AuditService {}
//! ```
//!
//! No manual registration call is required. `Injectable` emits metadata into
//! `lily_injection_registry`, and the application-owned
//! `lily_injection::ApplicationContainer` discovers and validates the complete
//! graph before starting singleton services.
//!
//! # Construction rules
//!
//! - `#[inject]` accepts only `Arc<T>` and `Arc<dyn Interface>` fields.
//! - If every named field is injected, the derive constructs the struct
//!   directly and `Default` is not required.
//! - If any named field is not injected, the struct must implement `Default`;
//!   the derive creates that default state and then replaces injected fields.
//! - Generic services, tuple structs, enums and unions are rejected.
//! - The service must implement `ServiceTrait + Send + Sync + 'static`.
//!
//! # Lifetimes
//!
//! `#[service(lifetime = "Singleton")]`, `"Scoped"` and `"Transient"` are
//! supported. Omitting the attribute selects `Transient`; production code
//! should state the intended lifetime explicitly. A singleton cannot inject a
//! scoped dependency. The complete graph is checked before startup.
//!
//! # Interface resolution
//!
//! A concrete service can publish one trait-object route:
//!
//! ```
//! use lily_injection::{Injectable, ServiceTrait};
//!
//! trait ClockApi: Send + Sync {
//!     fn name(&self) -> &'static str;
//! }
//!
//! #[derive(Injectable)]
//! #[service(interface = dyn ClockApi, lifetime = "Singleton")]
//! struct SystemClock;
//!
//! impl ClockApi for SystemClock {
//!     fn name(&self) -> &'static str {
//!         "system"
//!     }
//! }
//!
//! impl ServiceTrait for SystemClock {}
//! ```
//!
//! Both `SystemClock` and `dyn ClockApi` resolve to the same allocation. More
//! than one active implementation for the same interface is rejected while
//! the container is built.
//!
//! `#[service(disabled)]` suppresses link-time registration and resolution. It
//! is intended for compile-time candidate selection; a disabled service cannot
//! satisfy another service's dependency.
//!
//! Only `lily_injection` is required as a DI dependency. `lily_http_api`,
//! `lily_websocket` and `lily_consumer` also expose `Injectable`
//! and the DI APIs at their roots. Expansion locates the direct runtime or
//! one of these facades, including Cargo-renamed dependencies. Its hidden
//! registration bridge removes the need for application dependencies on
//! `lily_error`, `lily_injection_registry` or `linkme` solely for DI.
//! Existing direct imports of this crate's `Injectable` remain supported.

use proc_macro::TokenStream;

mod base_service;
mod injectable;
mod runtime_path;
mod service_args;
mod utils;

/// Registers a concrete service and generates constructor injection.
///
/// The target must be a non-generic named or unit struct that implements
/// `lily_injection::ServiceTrait + Send + Sync + 'static`. Fields carrying
/// `#[inject]` must use `Arc<T>` or `Arc<dyn Interface>`.
///
/// Supported service options are:
///
/// - `lifetime = "Singleton" | "Scoped" | "Transient"`;
/// - `interface = dyn Trait` for one trait-object resolution route;
/// - `disabled` (or `enabled = false`) to omit the registration.
///
/// Registration is automatic and belongs to the application container. Do not
/// construct registry metadata manually.
///
/// # Lifecycle example
///
/// ```rust
/// use lily_injection::{Injectable, InjectionError, ServiceTrait, async_trait::async_trait};
///
/// #[derive(Default, Injectable)]
/// #[service(lifetime = "Singleton")]
/// pub struct UserService;
///
/// #[async_trait]
/// impl ServiceTrait for UserService {
///     async fn initialize(&mut self) -> Result<(), InjectionError> {
///         Ok(())
///     }
/// }
/// ```
#[proc_macro_derive(Injectable, attributes(service, inject))]
pub fn derive_injectable(input: TokenStream) -> TokenStream {
    injectable::derive_impl(input)
}

/// Generates a DTO-facing [`lily_mongo_service::BaseService`] implementation
/// backed by `lily_mongo_repository::MongoRepository`.
///
/// This derive only implements the CRUD trait. The normal container-managed
/// shape also derives `Injectable`, marks its repository and optional gateway
/// fields with `#[inject]`, declares an explicit service lifetime and
/// implements `ServiceTrait`. `Injectable` publishes the service to the
/// link-time registry; no manual registration call is required.
///
/// Required contract:
///
/// - the struct has a named `Arc<Repository>` field whose name contains
///   `repository`;
/// - `Repository: MongoRepository<Entity>`;
/// - `Entity: From<Dto>` and `Dto: From<Entity>`;
/// - the converted entity contains a BSON ObjectId `_id` for delete/update
///   operations.
///
/// ```ignore
/// use std::sync::Arc;
/// use lily_injectable_derive::{CrudService, Injectable};
/// use lily_injection::ServiceTrait;
///
/// #[derive(Injectable, CrudService, Default)]
/// #[entity_type(AppManager)]
/// #[dto_type(AppManagerDto)]
/// #[repository_type(AppManagerRepository)]
/// #[service(lifetime = "Singleton")]
/// pub struct AppManagerService {
///     #[inject]
///     repository: Arc<AppManagerRepository>,
/// }
///
/// impl ServiceTrait for AppManagerService {}
/// ```
///
/// Every generated call creates a detached Mongo operation context. Write a
/// domain-specific method when a deadline, cancellation token, transaction or
/// different not-found policy is required. `#[gateway(GatewayType)]` is
/// optional; when present, an injected `Arc<GatewayType>` field whose name
/// contains `gateway` receives best-effort post-write notifications.
#[proc_macro_derive(
    CrudService,
    attributes(entity_type, repository_type, dto_type, gateway)
)]
pub fn derive_base_service(input: TokenStream) -> TokenStream {
    base_service::derive_impl(input)
}
