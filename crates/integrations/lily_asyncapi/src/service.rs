//! Immutable, attach-once access to a generated AsyncAPI document.
//!
//! Document construction belongs to Lily's application build pipeline. This
//! module deliberately exposes no document mutation or transport response
//! helpers: application code can only read the completed typed document and
//! its canonical JSON representation.

use std::{
    fmt,
    marker::PhantomData,
    sync::{Arc, OnceLock},
};

use crate::AsyncApiDocument;

struct AsyncApiDocumentState {
    document: AsyncApiDocument,
    canonical_json: Arc<[u8]>,
}

/// An immutable snapshot of one application's generated AsyncAPI document.
///
/// Cloning a snapshot is inexpensive: the typed document and its canonical
/// JSON bytes are shared through a single [`Arc`]. The marker `K` separates
/// independently owned AsyncAPI surfaces at the type level without changing
/// the generated wire document.
pub struct AsyncApiSnapshot<K> {
    state: Arc<AsyncApiDocumentState>,
    marker: PhantomData<fn() -> K>,
}

impl<K> Clone for AsyncApiSnapshot<K> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
            marker: PhantomData,
        }
    }
}

impl<K> fmt::Debug for AsyncApiSnapshot<K> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AsyncApiSnapshot")
            .field("canonical_json_bytes", &self.state.canonical_json.len())
            .finish_non_exhaustive()
    }
}

impl<K> AsyncApiSnapshot<K> {
    /// Borrow the immutable AsyncAPI 3.1 document model.
    #[must_use]
    pub fn document(&self) -> &AsyncApiDocument {
        &self.state.document
    }

    /// Borrow the byte-stable JSON generated once during application build.
    #[must_use]
    pub fn canonical_json(&self) -> &[u8] {
        &self.state.canonical_json
    }
}

/// Read-only, attach-once AsyncAPI document service.
///
/// Lily creates one service for each application-owned marker `K`, builds the
/// complete document off-service, and attaches it exactly once. Readers never
/// observe a partially built document. Different marker types produce
/// different service types, allowing multiple application surfaces to coexist
/// without a runtime name or downcast convention.
///
/// This service intentionally has no dependency-injection, HTTP, WebSocket, or
/// queue dependency. The relevant composition root owns registration and
/// lifecycle integration.
pub struct AsyncApiService<K> {
    state: OnceLock<Arc<AsyncApiDocumentState>>,
    marker: PhantomData<fn() -> K>,
}

impl<K> fmt::Debug for AsyncApiService<K> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AsyncApiService")
            .field("initialized", &self.state.get().is_some())
            .finish()
    }
}

impl<K> AsyncApiService<K> {
    /// Create an unattached service for Lily's composition root.
    pub(crate) const fn new() -> Self {
        Self {
            state: OnceLock::new(),
            marker: PhantomData,
        }
    }

    /// Bind a typed document to the canonical bytes prepared by the builder.
    ///
    /// Keeping preparation separate from attachment guarantees that all
    /// validation and serialization failures happen before the service becomes
    /// observable to application code.
    pub(crate) fn prepare(
        document: AsyncApiDocument,
        canonical_json: Arc<[u8]>,
    ) -> PreparedAsyncApi<K> {
        PreparedAsyncApi {
            state: Arc::new(AsyncApiDocumentState {
                document,
                canonical_json,
            }),
            marker: PhantomData,
        }
    }

    /// Atomically attach a completely prepared document.
    pub(crate) fn attach(&self, prepared: PreparedAsyncApi<K>) -> Result<(), AsyncApiServiceError> {
        self.state
            .set(prepared.state)
            .map_err(|_| AsyncApiServiceError::AlreadyInitialized)
    }

    /// Return an immutable snapshot after the application build has completed.
    pub fn snapshot(&self) -> Result<AsyncApiSnapshot<K>, AsyncApiServiceError> {
        self.state
            .get()
            .cloned()
            .map(|state| AsyncApiSnapshot {
                state,
                marker: PhantomData,
            })
            .ok_or(AsyncApiServiceError::NotInitialized)
    }

    /// Borrow the immutable typed document after initialization.
    pub fn document(&self) -> Result<&AsyncApiDocument, AsyncApiServiceError> {
        self.state
            .get()
            .map(|state| &state.document)
            .ok_or(AsyncApiServiceError::NotInitialized)
    }

    /// Borrow the canonical JSON bytes after initialization.
    pub fn canonical_json(&self) -> Result<&[u8], AsyncApiServiceError> {
        self.state
            .get()
            .map(|state| state.canonical_json.as_ref())
            .ok_or(AsyncApiServiceError::NotInitialized)
    }
}

/// A validated document waiting for its single atomic service attachment.
///
/// This token is crate-private so application code cannot bypass the document
/// builder or replace an initialized service. Its marker also prevents a
/// document prepared for one AsyncAPI surface from being attached to another.
#[doc(hidden)]
pub struct PreparedAsyncApi<K> {
    state: Arc<AsyncApiDocumentState>,
    marker: PhantomData<fn() -> K>,
}

impl<K> fmt::Debug for PreparedAsyncApi<K> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedAsyncApi")
            .field("canonical_json_bytes", &self.state.canonical_json.len())
            .finish_non_exhaustive()
    }
}

/// AsyncAPI service lifecycle error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AsyncApiServiceError {
    /// The application build has not attached a generated document yet.
    NotInitialized,
    /// A second document was attached to the immutable service.
    AlreadyInitialized,
}

impl fmt::Display for AsyncApiServiceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotInitialized => formatter.write_str("AsyncAPI document is not initialized"),
            Self::AlreadyInitialized => {
                formatter.write_str("AsyncAPI document is already initialized")
            }
        }
    }
}

impl std::error::Error for AsyncApiServiceError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AsyncApiConfig, build::build_document};

    struct ConsumerDocument;
    struct WebSocketDocument;

    fn prepared<K>() -> PreparedAsyncApi<K> {
        let config = AsyncApiConfig::new("Test", "1.0.0").expect("config");
        let (document, bytes) = build_document(&config, Vec::new()).expect("document");
        AsyncApiService::<K>::prepare(document, bytes)
    }

    #[test]
    fn service_is_read_only_before_attach_and_rejects_second_attach() {
        let service = AsyncApiService::<ConsumerDocument>::new();
        assert_eq!(
            service.snapshot().expect_err("not initialized"),
            AsyncApiServiceError::NotInitialized
        );

        service.attach(prepared()).expect("first attach");
        let expected = service.canonical_json().expect("bytes").to_vec();
        let snapshot = service.snapshot().expect("snapshot");
        assert_eq!(snapshot.canonical_json(), expected);
        assert_eq!(snapshot.document().specification_version(), "3.1.0");

        assert_eq!(
            service.attach(prepared()).expect_err("duplicate attach"),
            AsyncApiServiceError::AlreadyInitialized
        );
        assert_eq!(service.canonical_json().expect("stable bytes"), expected);
    }

    #[test]
    fn marker_types_own_independent_once_locks() {
        let consumer = AsyncApiService::<ConsumerDocument>::new();
        let websocket = AsyncApiService::<WebSocketDocument>::new();
        consumer.attach(prepared()).expect("consumer");
        assert_eq!(
            websocket.snapshot().expect_err("separate marker"),
            AsyncApiServiceError::NotInitialized
        );
        websocket.attach(prepared()).expect("websocket");
        assert_eq!(
            consumer.canonical_json().expect("consumer"),
            websocket.canonical_json().expect("websocket")
        );
    }

    #[test]
    fn concurrent_attachment_has_exactly_one_winner() {
        let service = Arc::new(AsyncApiService::<ConsumerDocument>::new());
        let mut workers = Vec::new();
        for _ in 0..16 {
            let service = Arc::clone(&service);
            workers.push(std::thread::spawn(move || service.attach(prepared())));
        }
        let successes = workers
            .into_iter()
            .map(|worker| worker.join().expect("attachment worker"))
            .filter(Result::is_ok)
            .count();
        assert_eq!(successes, 1);
        assert!(service.snapshot().is_ok());
    }
}
