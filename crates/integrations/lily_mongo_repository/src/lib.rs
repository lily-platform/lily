#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

//! MongoDB repository contract used by Lily's generated data layer.
//!
//! # Choose the right layer
//!
//! Application controllers normally call a DTO-facing service, not this crate.
//! A domain service may inject its `MongoCollection` adapter and use the
//! generated CRUD methods directly. Use [`MongoRepository`] when the optional
//! common repository contract and its typed-ID/entity conveniences are useful.
//!
//! This crate deliberately does not expose a database-neutral
//! `BaseRepository`. Every public type names MongoDB and preserves BSON
//! semantics. Do not use these types as controller request or response models.
//!
//! # Canonical operation inputs
//!
//! Empty filters and unbounded reads are rejected. Callers must explicitly
//! choose [`MongoFilter::all`] and every multi-record read must use a bounded
//! [`MongoPageRequest`].
//!
//! ```
//! use std::time::Duration;
//! use lily_mongo_repository::{MongoFilter, MongoOperationContext, MongoPageRequest};
//! use mongodb::bson::doc;
//!
//! let filter = MongoFilter::new(doc! { "tenant_id": "tenant-42" })?;
//! let page = MongoPageRequest::new(0, 25)?;
//! let operation = MongoOperationContext::detached()
//!     .with_deadline(Duration::from_secs(2))?;
//!
//! assert!(!filter.matches_all());
//! assert_eq!(page.limit(), 25);
//! assert!(!operation.cancellation_token().is_cancelled());
//! # Ok::<(), lily_mongo_repository::MongoRepositoryError>(())
//! ```
//!
//! # Optional repository composition
//!
//! A collection adapter does not require a repository: it may be injected
//! directly into a domain service. When an application opts into the repository
//! convenience layer, its container-managed data layer composes three separate
//! derives:
//!
//! 1. `MongoCollection` generates the driver operations and the collection's
//!    `ServiceTrait` lifecycle implementation.
//! 2. `Repository` implements [`MongoRepository`] by delegating to that
//!    collection.
//! 3. `Injectable` registers each concrete component and resolves fields
//!    marked with `#[inject]` through the application's `Extensions` provider.
//!
//! `Repository` does **not** perform DI registration and does not implement
//! `ServiceTrait`. A repository that is injected into another component must
//! therefore also derive `Injectable` and explicitly implement
//! `ServiceTrait`. `Injectable` publishes its metadata to
//! `lily_injection_registry`; application code does not manually register the
//! repository.
//!
//! The following is the repository-backed single-database shape. Remove the
//! repository struct and inject `ApplicationCollection` into the domain service
//! when direct collection access is preferred. The example is ignored as a
//! doctest only because this low-level contract crate intentionally does not
//! depend back on the MongoDB and injection adapter crates.
//!
//! ```ignore
//! use std::sync::Arc;
//!
//! use lily_injectable_derive::Injectable;
//! use lily_injection::ServiceTrait;
//! use lily_mongodb::{
//!     Collection, DatabaseService, MongoCollection, Repository,
//! };
//! use mongodb::bson::oid::ObjectId;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Clone, Serialize, Deserialize)]
//! pub struct Application {
//!     #[serde(skip_serializing_if = "Option::is_none")]
//!     pub _id: Option<ObjectId>,
//!     pub name: String,
//! }
//!
//! #[derive(Injectable, MongoCollection, Default)]
//! #[collection("applications")]
//! #[collection_type(Application)]
//! #[service(lifetime = "Singleton")]
//! pub struct ApplicationCollection {
//!     #[inject]
//!     db: Arc<DatabaseService>,
//!     collection: Option<Collection<Application>>,
//! }
//!
//! // MongoCollection already generated ServiceTrait and public CRUD methods.
//! // A domain service may inject ApplicationCollection directly.
//!
//! #[derive(Injectable, Repository, Default)]
//! #[collection_type(ApplicationCollection)]
//! #[entity_type(Application)]
//! #[service(lifetime = "Singleton")]
//! pub struct ApplicationRepository {
//!     #[inject]
//!     collection: Arc<ApplicationCollection>,
//! }
//!
//! // Repository generates MongoRepository<Application>, not ServiceTrait.
//! impl ServiceTrait for ApplicationRepository {}
//! ```
//!
//! In `lily_mongodb` factory mode, the collection instead injects
//! `Arc<MongoFactory>`, declares `#[cell_name("...")]`, and keeps its `db`
//! field as initialized runtime state. The repository shape is unchanged.
//!
//! Keep directly injected lifetimes compatible. The conventional chain uses
//! singleton database, collection and repository components; the registry
//! validates missing dependencies, cycles and captive scoped dependencies
//! before the application container starts them in dependency-first order.
//!
//! Because procedural-macro output is compiled in the application crate, that
//! crate must directly depend on the crates named by the generated code. For
//! this composition that normally includes `async-trait`, `futures`,
//! `lily_mongo_repository`, `lily_error`, `lily_injectable_derive`,
//! `lily_injection`, `lily_injection_registry`, `lily_mongodb`, `linkme`,
//! `mongodb` and `serde`; transitive dependencies are not sufficient in Rust.
//!
//! A few internal contract and downstream-macro hooks must remain Rust-public
//! because procedural macro output is compiled in the user's crate. Those
//! specific helpers are marked `#[doc(hidden)]`; this does not include the CRUD
//! methods generated by `MongoCollection`, which are application API.

use std::{
    fmt,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use mongodb::{
    ClientSession,
    bson::{Bson, Document, oid::ObjectId},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::{sync::Mutex, time::Instant};
use tokio_util::sync::CancellationToken;

/// Stable MongoDB error vocabulary used by repository operations.
pub use lily_error::application::mongodb::MongoDbError as MongoRepositoryError;

/// Hard V1 limit for a single repository page.
pub const MAX_MONGO_PAGE_SIZE: u32 = 100;
/// Hard V1 limit for one multi-document write.
pub const MAX_MONGO_WRITE_BATCH_SIZE: usize = 1_000;
/// Hard V1 limit for one ID lookup batch.
pub const MAX_MONGO_ID_BATCH_SIZE: usize = 100;
/// Hard V1 limit for a serialized BSON filter.
pub const MAX_MONGO_FILTER_BYTES: usize = 64 * 1024;

/// A parsed MongoDB object identifier.
///
/// Parsing is performed at the transport/service boundary so repository
/// implementations never receive an unchecked string identifier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MongoDocumentId(ObjectId);

impl MongoDocumentId {
    /// Parses a 24-character hexadecimal MongoDB ObjectId.
    ///
    /// The invalid input itself is not retained in the returned error.
    pub fn parse(value: &str) -> Result<Self, MongoRepositoryError> {
        ObjectId::parse_str(value).map(Self).map_err(|_| {
            MongoRepositoryError::InvalidDocumentId(
                "document id must be a 24-character hexadecimal ObjectId".to_string(),
            )
        })
    }

    /// Borrows the validated driver ObjectId.
    pub const fn as_object_id(&self) -> &ObjectId {
        &self.0
    }

    /// Consumes this wrapper and returns the driver ObjectId.
    pub fn into_object_id(self) -> ObjectId {
        self.0
    }

    /// Downstream-macro hook for extracting the conventional `_id` field.
    #[doc(hidden)]
    pub fn from_entity<T>(entity: &T) -> Result<Self, MongoRepositoryError>
    where
        T: Serialize,
    {
        let document = mongodb::bson::to_document(entity).map_err(MongoRepositoryError::from)?;
        document.get_object_id("_id").map(Self).map_err(|_| {
            MongoRepositoryError::InvalidDocumentId(
                "entity must contain a BSON ObjectId in `_id`".to_string(),
            )
        })
    }
}

impl From<ObjectId> for MongoDocumentId {
    fn from(value: ObjectId) -> Self {
        Self(value)
    }
}

impl fmt::Display for MongoDocumentId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// A validated, bounded batch of MongoDB identifiers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MongoIdBatch(Vec<MongoDocumentId>);

impl MongoIdBatch {
    /// Validates a non-empty ID batch against [`MAX_MONGO_ID_BATCH_SIZE`].
    pub fn new(ids: Vec<MongoDocumentId>) -> Result<Self, MongoRepositoryError> {
        if ids.is_empty() || ids.len() > MAX_MONGO_ID_BATCH_SIZE {
            return Err(MongoRepositoryError::InvalidBatch(format!(
                "id batch size must be between 1 and {MAX_MONGO_ID_BATCH_SIZE}"
            )));
        }
        Ok(Self(ids))
    }

    /// Borrows the validated identifiers in caller order.
    pub fn as_slice(&self) -> &[MongoDocumentId] {
        &self.0
    }

    /// Returns the number of identifiers in the batch.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns whether the batch is empty.
    ///
    /// A successfully constructed batch is never empty; this method exists for
    /// normal collection ergonomics.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// A validated, bounded multi-document write payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MongoWriteBatch<T>(Vec<T>);

impl<T> MongoWriteBatch<T> {
    /// Validates a non-empty write batch against
    /// [`MAX_MONGO_WRITE_BATCH_SIZE`].
    pub fn new(documents: Vec<T>) -> Result<Self, MongoRepositoryError> {
        if documents.is_empty() || documents.len() > MAX_MONGO_WRITE_BATCH_SIZE {
            return Err(MongoRepositoryError::InvalidBatch(format!(
                "write batch size must be between 1 and {MAX_MONGO_WRITE_BATCH_SIZE}"
            )));
        }
        Ok(Self(documents))
    }

    /// Borrows the validated documents in caller order.
    pub fn as_slice(&self) -> &[T] {
        &self.0
    }

    /// Consumes the validated wrapper and returns the document vector.
    pub fn into_inner(self) -> Vec<T> {
        self.0
    }

    /// Returns the number of documents in the batch.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns whether the batch is empty.
    ///
    /// A successfully constructed batch is never empty; this method exists for
    /// normal collection ergonomics.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// A BSON query filter whose size and all-document intent are explicit.
#[derive(Clone, Debug, PartialEq)]
pub struct MongoFilter {
    document: Document,
    all_documents: bool,
}

impl MongoFilter {
    /// Validates a non-empty BSON filter.
    pub fn new(document: Document) -> Result<Self, MongoRepositoryError> {
        if document.is_empty() {
            return Err(MongoRepositoryError::InvalidFilter(
                "an empty filter is ambiguous; use MongoFilter::all() explicitly".to_string(),
            ));
        }
        Self::validated(document, false)
    }

    /// Explicitly opts in to matching every document.
    pub fn all() -> Self {
        Self {
            document: Document::new(),
            all_documents: true,
        }
    }

    /// Downstream-macro hook for building an ID-only filter.
    #[doc(hidden)]
    pub fn by_ids(ids: &MongoIdBatch) -> Result<Self, MongoRepositoryError> {
        let object_ids = ids
            .as_slice()
            .iter()
            .map(|id| *id.as_object_id())
            .collect::<Vec<_>>();
        Self::new(mongodb::bson::doc! { "_id": { "$in": object_ids } })
    }

    fn validated(document: Document, all_documents: bool) -> Result<Self, MongoRepositoryError> {
        let encoded = mongodb::bson::to_vec(&document).map_err(MongoRepositoryError::from)?;
        if encoded.len() > MAX_MONGO_FILTER_BYTES {
            return Err(MongoRepositoryError::InvalidFilter(format!(
                "serialized filter exceeds the {MAX_MONGO_FILTER_BYTES}-byte limit"
            )));
        }
        Ok(Self {
            document,
            all_documents,
        })
    }

    /// Borrows the validated BSON document for a repository implementation.
    pub fn as_document(&self) -> &Document {
        &self.document
    }

    /// Consumes the wrapper and returns the validated BSON document.
    pub fn into_document(self) -> Document {
        self.document
    }

    /// Returns `true` only for an explicit [`MongoFilter::all`] value.
    pub const fn matches_all(&self) -> bool {
        self.all_documents
    }
}

/// A bounded page request with deterministic ordering.
#[derive(Clone, Debug, PartialEq)]
pub struct MongoPageRequest {
    offset: u64,
    limit: u32,
    sort: Document,
}

impl MongoPageRequest {
    /// Creates a page ordered by `_id` ascending.
    pub fn new(offset: u64, limit: u32) -> Result<Self, MongoRepositoryError> {
        Self::with_sort(offset, limit, mongodb::bson::doc! { "_id": 1 })
    }

    /// Creates a page with an explicit, deterministic Mongo sort document.
    /// At most four field paths are accepted and directions are limited to
    /// `1` (ascending) or `-1` (descending).
    pub fn with_sort(
        offset: u64,
        limit: u32,
        sort: Document,
    ) -> Result<Self, MongoRepositoryError> {
        if limit == 0 || limit > MAX_MONGO_PAGE_SIZE {
            return Err(MongoRepositoryError::InvalidPage(format!(
                "page limit must be between 1 and {MAX_MONGO_PAGE_SIZE}"
            )));
        }
        if sort.is_empty() || sort.len() > 4 {
            return Err(MongoRepositoryError::InvalidPage(
                "sort must contain between one and four fields".to_string(),
            ));
        }
        for (field, direction) in &sort {
            if !valid_sort_field(field) || !matches!(direction, Bson::Int32(1 | -1)) {
                return Err(MongoRepositoryError::InvalidPage(
                    "sort fields must be safe dotted identifiers with direction 1 or -1"
                        .to_string(),
                ));
            }
        }
        Ok(Self {
            offset,
            limit,
            sort,
        })
    }

    /// Returns the zero-based number of matching documents to skip.
    pub const fn offset(&self) -> u64 {
        self.offset
    }

    /// Returns the maximum number of items exposed to the caller.
    pub const fn limit(&self) -> u32 {
        self.limit
    }

    /// Borrows the validated deterministic MongoDB sort document.
    pub fn sort(&self) -> &Document {
        &self.sort
    }

    /// Downstream-macro hook for a limit-plus-one driver query.
    #[doc(hidden)]
    pub const fn driver_limit(&self) -> i64 {
        self.limit as i64 + 1
    }
}

fn valid_sort_field(field: &str) -> bool {
    !field.is_empty()
        && !field.starts_with('.')
        && !field.ends_with('.')
        && field.split('.').all(|component| {
            !component.is_empty()
                && !component.starts_with('$')
                && component
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        })
}

/// A bounded page response. `has_more` is computed from a limit-plus-one
/// query; no unbounded `find_all` operation exists on the V1 contract.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MongoPage<T> {
    /// Items in deterministic repository order, capped by [`Self::limit`].
    pub items: Vec<T>,
    /// Zero-based offset used for this page.
    pub offset: u64,
    /// Requested maximum number of exposed items.
    pub limit: u32,
    /// Whether another item existed in the limit-plus-one driver window.
    pub has_more: bool,
}

impl<T> MongoPage<T> {
    /// Downstream-macro hook for truncating a limit-plus-one driver window.
    #[doc(hidden)]
    pub fn from_driver_window(mut items: Vec<T>, request: &MongoPageRequest) -> Self {
        let has_more = items.len() > request.limit as usize;
        items.truncate(request.limit as usize);
        Self {
            items,
            offset: request.offset,
            limit: request.limit,
            has_more,
        }
    }
}

/// A caller-owned MongoDB session that has already entered a transaction.
///
/// Repository operations sharing this value are serialized because MongoDB
/// sessions cannot execute concurrent operations. The transaction owner is
/// responsible for commit/abort; repositories never commit implicitly.
pub struct MongoTransaction {
    session: Mutex<ClientSession>,
    observation: MongoTransactionObservation,
}

const OBSERVED_TRANSIENT_TRANSACTION: u8 = 1 << 0;
const OBSERVED_UNKNOWN_COMMIT_RESULT: u8 = 1 << 1;

#[doc(hidden)]
#[derive(Clone, Default)]
pub struct MongoTransactionObservation {
    labels: Arc<AtomicU8>,
}

/// Secret-safe MongoDB transaction labels observed during one attempt.
///
/// This is hidden framework ABI for Lily's queue transaction owner. It never
/// contains a driver diagnostic, command, endpoint or application document.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MongoTransactionErrorObservation {
    labels: u8,
}

impl MongoTransactionErrorObservation {
    /// Whether any operation observed `TransientTransactionError`.
    #[must_use]
    pub const fn transient_transaction(self) -> bool {
        self.labels & OBSERVED_TRANSIENT_TRANSACTION != 0
    }

    /// Whether any operation observed `UnknownTransactionCommitResult`.
    #[must_use]
    pub const fn unknown_commit_result(self) -> bool {
        self.labels & OBSERVED_UNKNOWN_COMMIT_RESULT != 0
    }
}

impl MongoTransactionObservation {
    /// Creates fresh attempt-local observation state.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn observe_driver_error(&self, error: &mongodb::error::Error) {
        use mongodb::error::{TRANSIENT_TRANSACTION_ERROR, UNKNOWN_TRANSACTION_COMMIT_RESULT};

        let mut labels = 0;
        if error.contains_label(TRANSIENT_TRANSACTION_ERROR) {
            labels |= OBSERVED_TRANSIENT_TRANSACTION;
        }
        if error.contains_label(UNKNOWN_TRANSACTION_COMMIT_RESULT) {
            labels |= OBSERVED_UNKNOWN_COMMIT_RESULT;
        }
        self.labels.fetch_or(labels, Ordering::AcqRel);
    }

    /// Returns the secret-safe labels observed during this attempt.
    #[must_use]
    pub fn snapshot(&self) -> MongoTransactionErrorObservation {
        MongoTransactionErrorObservation {
            labels: self.labels.load(Ordering::Acquire),
        }
    }

    /// Whether any operation in this attempt observed a transaction-body
    /// retry label.
    #[must_use]
    pub fn transient_seen(&self) -> bool {
        self.snapshot().transient_transaction()
    }

    /// Whether any commit in this attempt observed an indeterminate result.
    #[must_use]
    pub fn unknown_commit_result_seen(&self) -> bool {
        self.snapshot().unknown_commit_result()
    }

    /// Clears all observations before the owner deliberately retries an
    /// attempt. It must not be called while the prior attempt is still live.
    pub fn clear(&self) {
        self.labels.store(0, Ordering::Release);
    }
}

impl fmt::Debug for MongoTransactionObservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MongoTransactionObservation")
            .field("snapshot", &self.snapshot())
            .finish()
    }
}

impl MongoTransaction {
    /// Runtime-adapter hook for a session that has already started a
    /// transaction.
    #[doc(hidden)]
    pub fn from_started_session(session: ClientSession) -> Self {
        Self {
            session: Mutex::new(session),
            observation: MongoTransactionObservation::new(),
        }
    }

    /// Runtime-adapter hook that binds a started session to observation state
    /// created by the attempt owner before the transaction began.
    #[doc(hidden)]
    pub fn from_started_session_with_observation(
        session: ClientSession,
        observation: MongoTransactionObservation,
    ) -> Self {
        Self {
            session: Mutex::new(session),
            observation,
        }
    }

    /// Downstream-macro hook for serial MongoDB session access.
    #[doc(hidden)]
    pub async fn lock_session(&self) -> tokio::sync::MutexGuard<'_, ClientSession> {
        self.session.lock().await
    }

    fn observe_driver_error(&self, error: &mongodb::error::Error) {
        self.observation.observe_driver_error(error);
    }

    /// Returns labels observed by operations in the current transaction
    /// attempt without clearing them.
    #[doc(hidden)]
    #[must_use]
    pub fn error_observation(&self) -> MongoTransactionErrorObservation {
        self.observation.snapshot()
    }

    /// Clears the attempt-local observation before a deliberate retry.
    #[doc(hidden)]
    pub fn clear_error_observation(&self) {
        self.observation.clear();
    }

    /// Clones the attempt observation token without exposing the driver
    /// session. Queue-owned operation contexts use this to preserve evidence
    /// across independently constructed repository calls.
    #[doc(hidden)]
    #[must_use]
    pub fn observation_token(&self) -> MongoTransactionObservation {
        self.observation.clone()
    }

    /// Commits this transaction under the supplied cancellation/deadline
    /// policy.
    ///
    /// A transaction is not committed automatically when dropped. The caller
    /// remains responsible for choosing exactly one terminal commit/abort
    /// path.
    pub async fn commit(
        &self,
        operation: &MongoOperationContext<'_>,
    ) -> Result<(), MongoRepositoryError> {
        operation
            .execute(async {
                self.session
                    .lock()
                    .await
                    .commit_transaction()
                    .await
                    .map_err(|error| {
                        self.observe_driver_error(&error);
                        MongoRepositoryError::from(error)
                    })
            })
            .await
    }

    /// Aborts this transaction under the supplied cancellation/deadline
    /// policy.
    ///
    /// Call this explicitly on business-operation failure when the transaction
    /// has not already reached a terminal state.
    pub async fn abort(
        &self,
        operation: &MongoOperationContext<'_>,
    ) -> Result<(), MongoRepositoryError> {
        operation
            .execute(async {
                self.session
                    .lock()
                    .await
                    .abort_transaction()
                    .await
                    .map_err(|error| {
                        self.observe_driver_error(&error);
                        MongoRepositoryError::from(error)
                    })
            })
            .await
    }
}

impl fmt::Debug for MongoTransaction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MongoTransaction")
            .field("session", &"<redacted>")
            .finish()
    }
}

/// Per-operation cancellation, deadline, transaction and optimistic
/// concurrency policy.
///
/// Dropping a repository future drops the in-flight driver future. No helper
/// task is detached, so handler cancellation cannot leave a background query.
#[derive(Clone)]
pub struct MongoOperationContext<'transaction> {
    cancellation: CancellationToken,
    deadline: Option<Instant>,
    transaction: Option<&'transaction MongoTransaction>,
    transaction_observation: Option<MongoTransactionObservation>,
    expected_revision: Option<i64>,
}

impl MongoOperationContext<'static> {
    /// Creates an independent operation context. The caller can clone and
    /// cancel the returned token through [`Self::cancellation_token`].
    pub fn detached() -> Self {
        Self::new(CancellationToken::new())
    }
}

impl<'transaction> MongoOperationContext<'transaction> {
    /// Creates an operation context controlled by the supplied cancellation
    /// token and with no deadline, transaction or expected revision.
    pub fn new(cancellation: CancellationToken) -> Self {
        Self {
            cancellation,
            deadline: None,
            transaction: None,
            transaction_observation: None,
            expected_revision: None,
        }
    }

    /// Adds a relative deadline beginning at this call.
    ///
    /// A zero duration is rejected. Calling this method again replaces the
    /// previous deadline.
    pub fn with_deadline(mut self, timeout: Duration) -> Result<Self, MongoRepositoryError> {
        if timeout.is_zero() {
            return Err(MongoRepositoryError::InvalidOperationContext(
                "operation deadline must be greater than zero".to_string(),
            ));
        }
        self.deadline = Some(Instant::now() + timeout);
        Ok(self)
    }

    /// Associates the operation with a caller-owned MongoDB transaction.
    ///
    /// This consumes the context because the returned value borrows the
    /// transaction. Commit or abort remains the transaction owner's
    /// responsibility.
    pub fn with_transaction<'next>(
        self,
        transaction: &'next MongoTransaction,
    ) -> MongoOperationContext<'next> {
        MongoOperationContext {
            cancellation: self.cancellation,
            deadline: self.deadline,
            transaction: Some(transaction),
            transaction_observation: Some(transaction.observation_token()),
            expected_revision: self.expected_revision,
        }
    }

    /// Attaches shared attempt observation state without exposing a MongoDB
    /// session. This is framework ABI for transaction owners that construct a
    /// fresh repository context for every call.
    #[doc(hidden)]
    pub fn with_transaction_observation(
        mut self,
        observation: MongoTransactionObservation,
    ) -> Self {
        self.transaction_observation = Some(observation);
        self
    }

    /// Returns the shared attempt observation token, when one is attached.
    #[doc(hidden)]
    #[must_use]
    pub fn transaction_observation(&self) -> Option<&MongoTransactionObservation> {
        self.transaction_observation.as_ref()
    }

    /// Requires `_revision` to match for generated update/delete operations.
    ///
    /// A zero matched count becomes `ConcurrencyConflict` instead of silent
    /// success. Revisions must be non-negative.
    pub fn with_expected_revision(mut self, revision: i64) -> Result<Self, MongoRepositoryError> {
        if revision < 0 {
            return Err(MongoRepositoryError::InvalidOperationContext(
                "expected revision cannot be negative".to_string(),
            ));
        }
        self.expected_revision = Some(revision);
        Ok(self)
    }

    /// Clones the cancellation token so the caller can cancel in-flight work.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    /// Downstream repository-implementation hook.
    #[doc(hidden)]
    pub const fn transaction(&self) -> Option<&MongoTransaction> {
        self.transaction
    }

    /// Records transaction labels before converting a raw MongoDB driver
    /// error into Lily's secret-safe public vocabulary.
    ///
    /// Generated collection implementations use this hook so an application
    /// error conversion cannot erase the transaction owner's retry evidence.
    #[doc(hidden)]
    pub fn map_driver_error(&self, error: mongodb::error::Error) -> MongoRepositoryError {
        if let Some(observation) = &self.transaction_observation {
            observation.observe_driver_error(&error);
        } else if let Some(transaction) = self.transaction {
            transaction.observe_driver_error(&error);
        }
        MongoRepositoryError::from(error)
    }

    /// Downstream repository-implementation hook.
    #[doc(hidden)]
    pub const fn expected_revision(&self) -> Option<i64> {
        self.expected_revision
    }

    /// Downstream repository-implementation hook.
    #[doc(hidden)]
    pub fn apply_concurrency_filter(&self, mut filter: Document) -> Document {
        if let Some(revision) = self.expected_revision {
            filter.insert("_revision", revision);
        }
        filter
    }

    /// Downstream repository-implementation hook.
    #[doc(hidden)]
    pub async fn execute<T, F>(&self, operation: F) -> Result<T, MongoRepositoryError>
    where
        F: Future<Output = Result<T, MongoRepositoryError>> + Send,
        T: Send,
    {
        let deadline = async {
            match self.deadline {
                Some(deadline) => tokio::time::sleep_until(deadline).await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::pin!(deadline);
        tokio::pin!(operation);

        tokio::select! {
            biased;
            _ = self.cancellation.cancelled() => Err(MongoRepositoryError::OperationCancelled),
            _ = &mut deadline => Err(MongoRepositoryError::OperationTimedOut),
            result = &mut operation => result,
        }
    }
}

impl fmt::Debug for MongoOperationContext<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MongoOperationContext")
            .field("cancelled", &self.cancellation.is_cancelled())
            .field("has_deadline", &self.deadline.is_some())
            .field("transactional", &self.transaction.is_some())
            .field("expected_revision", &self.expected_revision)
            .finish()
    }
}

/// MongoDB-specific convenience contract implemented by generated repositories.
///
/// A domain service may instead inject its generated collection adapter and
/// call that adapter's public CRUD methods directly. Use this trait when the
/// shared repository vocabulary, typed IDs and entity-level conveniences fit
/// the application. The supported implementation path is
/// `#[derive(lily_mongodb::Repository)]` on a repository that injects the
/// matching generated collection. Container-managed
/// repositories additionally use `#[derive(Injectable)]`, `#[inject]` on the
/// collection field and an explicit `ServiceTrait` implementation. The trait
/// is Rust-public because its implementation is emitted into the user's crate;
/// it is not a general persistence-provider extension point. Define a separate
/// application trait when custom storage semantics are required.
///
/// Generated methods receive a [`MongoOperationContext`] and honor its
/// cancellation, deadline, transaction and optimistic-concurrency policy
/// without detaching driver work after the returned future is dropped.
#[async_trait]
pub trait MongoRepository<T>: Send + Sync
where
    T: Send + Sync + Serialize + DeserializeOwned + 'static,
{
    /// Inserts one document and returns the canonical persisted entity,
    /// including its generated `_id`.
    async fn create(
        &self,
        document: T,
        operation: &MongoOperationContext<'_>,
    ) -> Result<T, MongoRepositoryError>;

    /// Inserts a validated, non-empty bounded batch.
    ///
    /// The returned vector corresponds to input order. This method does not
    /// promise transaction-level atomicity unless the supplied operation
    /// context carries a transaction.
    async fn create_many(
        &self,
        documents: MongoWriteBatch<T>,
        operation: &MongoOperationContext<'_>,
    ) -> Result<Vec<T>, MongoRepositoryError>;

    /// Fully replaces the entity selected by the document's `_id`.
    ///
    /// With an expected revision, generated implementations include the
    /// revision in the write predicate, increment it and return
    /// [`MongoRepositoryError::ConcurrencyConflict`] when no document matches.
    async fn update(
        &self,
        document: T,
        operation: &MongoOperationContext<'_>,
    ) -> Result<T, MongoRepositoryError>;

    /// Deletes the document with the supplied validated ObjectId.
    ///
    /// Returns `false` when no document exists, except that an expected
    /// revision turns a zero match into a concurrency conflict.
    async fn delete_by_id(
        &self,
        id: MongoDocumentId,
        operation: &MongoOperationContext<'_>,
    ) -> Result<bool, MongoRepositoryError>;

    /// Deletes at most one document matching the validated filter.
    ///
    /// If the filter can match multiple documents, which matching document is
    /// selected is a MongoDB driver concern; use an ID filter when identity
    /// matters.
    async fn delete_one(
        &self,
        filter: MongoFilter,
        operation: &MongoOperationContext<'_>,
    ) -> Result<bool, MongoRepositoryError>;

    /// Deletes all documents matching an explicit validated filter and returns
    /// the number removed.
    ///
    /// Matching every document requires [`MongoFilter::all`] at the call site.
    async fn delete_many(
        &self,
        filter: MongoFilter,
        operation: &MongoOperationContext<'_>,
    ) -> Result<u64, MongoRepositoryError>;

    /// Finds a document by validated ObjectId.
    ///
    /// Absence is represented by `Ok(None)` at the repository boundary.
    async fn find_by_id(
        &self,
        id: MongoDocumentId,
        operation: &MongoOperationContext<'_>,
    ) -> Result<Option<T>, MongoRepositoryError>;

    /// Finds documents from a validated, non-empty bounded ID batch.
    ///
    /// Missing IDs are omitted. Output order follows the repository's
    /// deterministic query order and is not required to match input ID order.
    async fn find_by_ids(
        &self,
        ids: MongoIdBatch,
        operation: &MongoOperationContext<'_>,
    ) -> Result<Vec<T>, MongoRepositoryError>;

    /// Returns at most one document matching the validated filter.
    ///
    /// Use a uniquely identifying filter when deterministic identity matters.
    async fn find_one(
        &self,
        filter: MongoFilter,
        operation: &MongoOperationContext<'_>,
    ) -> Result<Option<T>, MongoRepositoryError>;

    /// Returns one bounded, deterministically ordered page.
    ///
    /// Implementations fetch at most `limit + 1` records to compute
    /// [`MongoPage::has_more`] and expose at most `limit` items.
    async fn find_page(
        &self,
        filter: MongoFilter,
        page: MongoPageRequest,
        operation: &MongoOperationContext<'_>,
    ) -> Result<MongoPage<T>, MongoRepositoryError>;

    /// Counts documents matching the explicit validated filter.
    async fn count(
        &self,
        filter: MongoFilter,
        operation: &MongoOperationContext<'_>,
    ) -> Result<u64, MongoRepositoryError>;

    /// Returns whether any document matches the explicit validated filter.
    async fn exists(
        &self,
        filter: MongoFilter,
        operation: &MongoOperationContext<'_>,
    ) -> Result<bool, MongoRepositoryError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Serialize)]
    struct EntityWithId {
        _id: Option<ObjectId>,
        ignored: &'static str,
    }

    #[test]
    fn page_and_filter_contracts_are_bounded() {
        assert!(MongoPageRequest::new(0, 0).is_err());
        assert!(MongoPageRequest::new(0, MAX_MONGO_PAGE_SIZE + 1).is_err());
        assert!(MongoFilter::new(Document::new()).is_err());
        assert!(MongoFilter::all().matches_all());

        let request = MongoPageRequest::new(20, 2).unwrap();
        let page = MongoPage::from_driver_window(vec![1, 2, 3], &request);
        assert_eq!(page.items, vec![1, 2]);
        assert!(page.has_more);
    }

    #[test]
    fn entity_ids_and_id_filters_are_strict_and_bounded() {
        let first = ObjectId::new();
        let second = ObjectId::new();
        let entity = EntityWithId {
            _id: Some(first),
            ignored: "must not become a delete predicate",
        };

        assert_eq!(
            MongoDocumentId::from_entity(&entity)
                .unwrap()
                .into_object_id(),
            first
        );
        assert!(matches!(
            MongoDocumentId::from_entity(&EntityWithId {
                _id: None,
                ignored: "missing",
            }),
            Err(MongoRepositoryError::InvalidDocumentId(_))
        ));

        let ids = MongoIdBatch::new(vec![first.into(), second.into()]).unwrap();
        let filter = MongoFilter::by_ids(&ids).unwrap().into_document();
        assert_eq!(filter.len(), 1);
        let in_values = filter
            .get_document("_id")
            .unwrap()
            .get_array("$in")
            .unwrap();
        assert_eq!(in_values.len(), 2);
        assert!(MongoIdBatch::new(Vec::new()).is_err());
    }

    #[tokio::test]
    async fn cancellation_wins_without_detaching_the_operation() {
        let token = CancellationToken::new();
        token.cancel();
        let context = MongoOperationContext::new(token);

        let result = context
            .execute(async {
                std::future::pending::<()>().await;
                Ok::<_, MongoRepositoryError>(())
            })
            .await;

        assert_eq!(result, Err(MongoRepositoryError::OperationCancelled));
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_is_a_typed_failure() {
        let context = MongoOperationContext::detached()
            .with_deadline(Duration::from_secs(1))
            .unwrap();
        let task = tokio::spawn(async move {
            context
                .execute(async {
                    std::future::pending::<()>().await;
                    Ok::<_, MongoRepositoryError>(())
                })
                .await
        });
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(
            task.await.unwrap(),
            Err(MongoRepositoryError::OperationTimedOut)
        );
    }

    #[test]
    fn transaction_observation_is_shared_and_attempt_reset_is_explicit() {
        let observation = MongoTransactionObservation::new();
        let shared = observation.clone();
        shared
            .labels
            .fetch_or(OBSERVED_TRANSIENT_TRANSACTION, Ordering::AcqRel);

        assert!(observation.transient_seen());
        assert!(!observation.unknown_commit_result_seen());

        let context =
            MongoOperationContext::detached().with_transaction_observation(observation.clone());
        assert!(
            context
                .transaction_observation()
                .unwrap()
                .snapshot()
                .transient_transaction()
        );

        observation.clear();
        assert_eq!(
            shared.snapshot(),
            MongoTransactionErrorObservation::default()
        );
    }
}
