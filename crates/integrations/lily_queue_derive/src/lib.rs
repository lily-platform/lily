#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

//! Procedural macro implementation for Lily queue handlers.
//!
//! Application code should import [`queue_service`] and [`queue`] from the
//! `lily_queue` facade. This crate is the implementation package behind those
//! re-exports.
//!
//! ## Usage
//!
//! ```rust,ignore
//! use lily_queue::{Json, QueueHandlerError, guard, middleware, queue, queue_service};
//!
//! pub struct UserService {
//!     // fields
//! }
//!
//! #[queue_service]
//! #[middleware(ServiceAudit)]
//! #[guard(ServiceAdmission)]
//! impl UserService {
//!     #[queue(
//!         "user.created",
//!         version = 1,
//!         content = "json",
//!     )]
//!     #[middleware(HandlerMetrics)]
//!     #[guard(CanCreateUser)]
//!     async fn handle_user_created(&self, Json(msg): Json<UserCreated>) -> Result<(), QueueHandlerError> {
//!         println!("User created: {:?}", msg);
//!         Ok(())
//!     }
//!     
//!     #[queue("user.deleted", version = 1, content = "json")]
//!     async fn handle_user_deleted(&self, Json(msg): Json<UserDeleted>) -> Result<(), QueueHandlerError> {
//!         println!("User deleted: {:?}", msg);
//!         Ok(())
//!     }
//! }
//! ```
//!
//! ## Features
//!
//! - **Link-time Registration**: generated metadata is collected without a
//!   runtime registration call
//! - **Type Safety**: Full type checking for message types and handler signatures
//! - **Async Support**: Native async/await support for handler methods
//! - **Single Policy Authority**: Concurrency and retry policy come from `QueueDefinition`
//! - **DI Integration**: Seamless integration with lily_injection DI container

use proc_macro::TokenStream;

mod asyncapi;
mod queue_args;
mod queue_service;
mod runtime_path;
mod utils;

/// Attribute macro for queue service implementation blocks
///
/// This macro processes impl blocks and emits type-erased wrapper functions plus
/// link-time metadata for methods marked with `#[queue(...)]`. `lily_consumer`
/// later selects configured metadata, compiles the immutable execution plan and
/// performs RabbitMQ registration; this macro does not contact the broker.
///
/// # Example
///
/// ```rust,ignore
/// use lily_queue::{Json, QueueHandlerError, guard, middleware, queue, queue_service};
///
/// #[queue_service]
/// #[middleware(ServiceAudit)]
/// #[guard(ServiceAdmission)]
/// impl UserService {
///     #[queue("user.created", version = 1, content = "json")]
///     #[middleware(HandlerMetrics)]
///     #[guard(CanCreateUser)]
///     async fn handle_user_created(&self, Json(msg): Json<UserCreated>) -> Result<(), QueueHandlerError> {
///         // Handler implementation
///         Ok(())
///     }
/// }
/// ```
///
/// # Generated Code
///
/// The macro generates:
/// - Handler wrapper functions (type-erased, async)
/// - Metadata structs with handler identity
/// - Automatic registration in distributed slice (linkme)
///
/// # Requirements
///
/// - Handler methods must be async
/// - each handler must be an inherent `async fn` with `&self` and zero to
///   sixteen owned typed extractor parameters;
/// - parts extractors precede an optional, sole terminal payload extractor;
/// - each handler must return `Result<(), E>` where
///   `E: Into<QueueHandlerError>`;
/// - extractor and handler futures must be `Send`.
/// - the owning service type must be registered in Lily DI, normally through
///   `#[derive(Injectable)]`, `#[service(...)]` and its required `ServiceTrait`
///   implementation. Missing owner registration fails Consumer startup before
///   broker admission.
///
/// `#[queue_service]` must be the outermost implementation attribute. Ordered,
/// repeatable `#[middleware(Type)]` and `#[guard(Type)]` sibling markers below
/// it are consumed by this macro. The same markers may follow one handler's
/// `#[queue(...)]` declaration. Every marker accepts exactly one concrete type.
#[proc_macro_attribute]
pub fn queue_service(args: TokenStream, input: TokenStream) -> TokenStream {
    queue_service::queue_service_impl(args, input)
}

/// Marker attribute for queue handler methods
///
/// This attribute marks a method as a queue handler and specifies its exact
/// queue, schema-version and content-kind contract. Runtime policy belongs to
/// `rabbitmq.topology.queues`. The marker does not generate code by itself; it is
/// consumed by the enclosing [`queue_service`] macro.
///
/// # Syntax
///
/// ```rust,ignore
/// #[queue("queue.name", version = 1, content = "json")]
/// ```
///
/// A handler that must participate in Lily's configured transactional inbox
/// runtime declares that intent explicitly. When application mutations or
/// outbox events must share the atomic boundary, it also extracts the exact
/// transaction before its optional payload:
///
/// ```rust,ignore
/// use lily_queue::{Json, PostgresTransaction, QueueHandlerError, queue, queue_service};
///
/// #[queue_service]
/// impl OrderWorker {
///     #[queue(
///         "orders.created",
///         version = 1,
///         content = "json",
///         delivery_guarantee = "transactional_inbox",
///     )]
///     async fn created(
///         &self,
///         transaction: PostgresTransaction,
///         Json(event): Json<OrderCreated>,
///     ) -> Result<(), QueueHandlerError> {
///         transaction
///             .with_connection(move |connection| Box::pin(async move {
///                 persist_order(connection, event).await
///             }))
///             .await?;
///         Ok(())
///     }
/// }
/// ```
///
/// The attribute itself supplies inbox claim and deduplication only. Covered
/// business mutations and outbox insertion must use the extracted
/// `PostgresTransaction`; an independently acquired pool or repository
/// connection remains outside that atomic boundary. Requesting this extractor
/// from an at-least-once handler fails with
/// `QUEUE_POSTGRES_TRANSACTION_CONTEXT_UNAVAILABLE`.
///
/// # Parameters
///
/// - **queue name** (required): String literal specifying the queue to listen on
/// - **version** (required): Exact positive schema version in `1..=65535`
/// - **content** (required): Exact bounded content-kind token
/// - **delivery_guarantee** (optional): `"at_least_once"` (the default) or
///   `"transactional_inbox"`. This records backend-neutral handler intent;
///   the consumer must bind and validate the concrete transactional backend
///   before it admits broker deliveries.
///
/// Transactional inbox identity includes the generated
/// `module_path::ServiceType::method` name. Moving or renaming any of those
/// elements creates a new deduplication namespace; that refactor and the
/// configured inbox retention horizon are application data-compatibility
/// decisions.
///
/// # Example
///
/// ```rust,ignore
/// use lily_queue::{Json, QueueHandlerError, queue, queue_service};
///
/// #[queue_service]
/// impl UserService {
///     #[queue("user.created", version = 1, content = "json")]
///     async fn handle_user_created(
///         &self,
///         Json(msg): Json<UserCreated>,
///     ) -> Result<(), QueueHandlerError> {
///         // Handler implementation
///         Ok(())
///     }
/// }
/// ```
///
/// # Note
///
/// This marker is valid only inside a [`queue_service`] implementation block.
/// Standalone use is rejected so a handler cannot appear registered while
/// silently producing no metadata.
#[proc_macro_attribute]
pub fn queue(_args: TokenStream, _input: TokenStream) -> TokenStream {
    syn::Error::new(
        proc_macro2::Span::call_site(),
        "#[queue] must be used on a method inside a #[queue_service] impl block",
    )
    .to_compile_error()
    .into()
}

/// Marker consumed by the enclosing [`queue_service`] macro.
///
/// The queue AsyncAPI feature must be enabled and `#[queue_service]` must be
/// the outermost attribute on the implementation block. Calling this marker
/// directly is always an error because doing so would create metadata outside
/// the canonical `QueueHandlerMetadata`
/// registration.
#[proc_macro_attribute]
pub fn asyncapi(_args: TokenStream, _input: TokenStream) -> TokenStream {
    #[cfg(feature = "asyncapi")]
    let message = "#[asyncapi] must be nested beneath an outer #[queue_service] attribute";
    #[cfg(not(feature = "asyncapi"))]
    let message =
        "queue AsyncAPI metadata requires enabling the `asyncapi` feature on `lily_queue`";

    syn::Error::new(proc_macro2::Span::call_site(), message)
        .to_compile_error()
        .into()
}

/// Marker attribute for one queue middleware type.
///
/// The marker is repeatable and its source order is retained. It is valid only
/// beneath an outer `#[queue_service]`: place it on that implementation block
/// for service-wide middleware or beside `#[queue(...)]` on one handler.
/// Lily constructs each referenced concrete type once while building the
/// consumer and compiles the effective `global -> service -> handler` plan.
///
/// ```rust,ignore
/// #[queue_service]
/// #[middleware(ServiceAudit)]
/// impl OrderWorker {
///     #[queue("orders.created", version = 1, content = "json")]
///     #[middleware(HandlerMetrics)]
///     async fn created(&self) -> Result<(), QueueHandlerError> {
///         Ok(())
///     }
/// }
/// ```
#[proc_macro_attribute]
pub fn middleware(_args: TokenStream, _input: TokenStream) -> TokenStream {
    syn::Error::new(
        proc_macro2::Span::call_site(),
        "#[middleware] must be placed beneath an outer #[queue_service] attribute",
    )
    .to_compile_error()
    .into()
}

/// Marker attribute for one queue delivery guard type.
///
/// The marker is repeatable and its source order is retained. It is valid only
/// beneath an outer `#[queue_service]`: place it on that implementation block
/// for service-wide guards or beside `#[queue(...)]` on one handler. Guards
/// return Lily's typed retryable/permanent rejection rather than a boolean or
/// a silent drop decision.
///
/// ```rust,ignore
/// #[queue_service]
/// #[guard(ServiceAdmission)]
/// impl OrderWorker {
///     #[queue("orders.created", version = 1, content = "json")]
///     #[guard(CanCreateOrder)]
///     async fn created(&self) -> Result<(), QueueHandlerError> {
///         Ok(())
///     }
/// }
/// ```
#[proc_macro_attribute]
pub fn guard(_args: TokenStream, _input: TokenStream) -> TokenStream {
    syn::Error::new(
        proc_macro2::Span::call_site(),
        "#[guard] must be placed beneath an outer #[queue_service] attribute",
    )
    .to_compile_error()
    .into()
}
