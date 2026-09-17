#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

//! Derives for Lily's MongoDB collection and repository adapters.
//!
//! Application data layers may use the collection derive by itself or compose
//! it with the optional repository derive:
//!
//! - [`MongoCollection`] owns a typed driver collection handle, generates
//!   public typed CRUD operations, produces explicit migration steps and
//!   implements `lily_injection::ServiceTrait`. A domain service may inject
//!   this collection adapter directly.
//! - [`Repository`] implements
//!   `lily_mongo_repository::MongoRepository<Entity>` by delegating to that
//!   collection. This is an optional convenience layer; it does not register
//!   the repository and does not implement its lifecycle.
//!
//! A container-managed collection therefore derives `Injectable`,
//! `MongoCollection` and `Default`. Only applications choosing the repository
//! convenience also define a repository deriving `Injectable`, `Repository`
//! and `Default`, then declare its `ServiceTrait` implementation.
//!
//! # Collection attributes
//!
//! `#[collection_type(Entity)]` is required. `#[collection("name")]` is
//! optional and otherwise defaults to the lower-cased collection struct name.
//! The derive validates collection, cell and index names at compile time.
//!
//! - `#[index(field = "created_at")]` adds a non-unique ascending index step.
//! - `#[unique_field("email")]` adds a unique single-field index step.
//! - `#[unique_combination("tenant_id", "slug")]` accepts 2 to 8 fields.
//! - `#[cell_name("orders")]` selects a named `MongoFactory` cell in factory
//!   mode.
//!
//! Index attributes only generate values returned by `migration_steps()`.
//! They never create a collection or index during DI startup.
//!
//! # Required collection fields
//!
//! Single-database mode uses this shape:
//!
//! ```ignore
//! #[derive(Injectable, MongoCollection, Default)]
//! #[collection("applications")]
//! #[collection_type(Application)]
//! #[service(lifetime = "Singleton")]
//! struct ApplicationCollection {
//!     #[inject]
//!     db: Arc<DatabaseService>,
//!     collection: Option<Collection<Application>>,
//! }
//! ```
//!
//! Factory mode additionally injects `mongo_factory: Arc<MongoFactory>`, adds
//! `#[cell_name("...")]`, and leaves `db: Arc<DatabaseService>` unmarked. The
//! generated lifecycle selects the configured cell and assigns `db` before it
//! creates the lightweight collection handle.
//!
//! Do not manually implement `ServiceTrait` for the collection because
//! `MongoCollection` already does so.
//!
//! # Optional repository composition
//!
//! ```ignore
//! #[derive(Injectable, Repository, Default)]
//! #[entity_type(Application)]
//! #[collection_type(ApplicationCollection)]
//! #[service(lifetime = "Singleton")]
//! struct ApplicationRepository {
//!     #[inject]
//!     collection: Arc<ApplicationCollection>,
//! }
//!
//! impl ServiceTrait for ApplicationRepository {}
//! ```
//!
//! `Entity` must be cloneable and support BSON serialization and
//! deserialization. Generated creates write MongoDB's returned `_id` back into
//! the returned entity. Generated updates require a non-null `_id`; an
//! operation context with an expected revision also uses `_revision` for
//! optimistic concurrency.
//!
//! The collection's generated CRUD functions are intentional application API.
//! A service may inject the collection and call them directly. The repository
//! derive delegates to the same functions and adds the common
//! `MongoRepository` convenience contract; it is not a mandatory access layer.

extern crate proc_macro;
mod base_repository;
mod collection;
use proc_macro::TokenStream;

/// Generates a lifecycle-aware typed MongoDB collection adapter.
///
/// The input must be a named-field struct containing `db` and `collection`
/// state fields and must declare `#[collection_type(Entity)]`. See the crate
/// documentation for single/factory shapes and every supported index
/// attribute.
///
/// Generated inherent API includes `collection_name()`, `migration_steps()` and
/// the public typed CRUD functions `insert_one`, `insert_many`, `find_one`,
/// `find_page`, `replace_one`, `delete_one`, `delete_many` and
/// `count_documents`. A service may inject the generated collection adapter
/// and use these functions without defining a repository. The derive also
/// implements `lily_injection::ServiceTrait`; do not add a competing manual
/// implementation.
///
/// CRUD calls require an explicit
/// `lily_mongo_repository::MongoOperationContext`. Reads use validated
/// `MongoFilter` and bounded `MongoPageRequest` values; batch inserts use
/// `MongoWriteBatch`. The lower-level replace and delete functions deliberately
/// accept BSON filter documents and return MongoDB driver result types. The
/// optional [`Repository`] layer adds the common entity/typed-ID conveniences.
#[proc_macro_derive(
    MongoCollection,
    attributes(
        collection,
        collection_type,
        cell_name,
        index,
        unique_field,
        unique_combination
    )
)]
pub fn derive_collection(input: TokenStream) -> TokenStream {
    collection::derive_impl(input)
}

/// Implements `lily_mongo_repository::MongoRepository<Entity>` for a struct.
///
/// The struct must contain a named `Arc<Collection>` field whose name contains
/// `collection`. `Entity` must be cloneable and serializable/deserializable by
/// BSON. The referenced collection type is generated with
/// `#[derive(MongoCollection)]`.
///
/// This derive is an optional convenience and does not register the repository
/// with Lily's DI container. An injectable repository additionally derives
/// `Injectable`, marks its collection field with `#[inject]`, declares its
/// lifetime and implements `ServiceTrait`. Registration metadata is then
/// discovered automatically by the application container.
///
/// ```ignore
/// use std::sync::Arc;
/// use lily_injectable_derive::Injectable;
/// use lily_injection::ServiceTrait;
/// use lily_mongodb::Repository;
///
/// #[derive(Injectable, Repository, Default)]
/// #[entity_type(Application)]
/// #[collection_type(ApplicationCollection)]
/// #[service(lifetime = "Singleton")]
/// struct ApplicationRepository {
///     #[inject]
///     collection: Arc<ApplicationCollection>,
/// }
///
/// impl ServiceTrait for ApplicationRepository {}
/// ```
///
/// `#[entity_type(Entity)]` and `#[collection_type(CollectionAdapter)]` are
/// required. The struct must have a named field whose identifier contains
/// `collection`; the canonical field is
/// `#[inject] collection: Arc<CollectionAdapter>`.
#[proc_macro_derive(Repository, attributes(entity_type, collection_type))]
pub fn derive_base_repository(input: TokenStream) -> TokenStream {
    base_repository::derive_impl(input)
}
