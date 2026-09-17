//! HTTP invocation metadata and abnormal cleanup context.

use lily_injection::Extensions;
use lily_web_core::{CleanupCancellation, RequestExtensions};
use std::sync::Arc;

/// Last framework-observed boundary of an interrupted around callback.
/// It does not describe the callback's individual side effects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum HttpMiddlewareStage {
    /// The callback was first polled, before calling `next`.
    Before,
    /// `next` was polled and has not returned to the middleware.
    DelegatingToNext,
    /// `next` returned; the middleware itself has not yet returned.
    After,
}

/// Bounded interruption evidence, distinct from successful cleanup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum HttpRequestInterruption {
    /// The application's graceful cutoff elapsed.
    GracefulDeadline,
    /// The application requested forced shutdown.
    ForcedShutdown,
    /// The request execution deadline elapsed.
    RequestTimeout,
    /// The bounded attempt to finalize an error/rejection response expired.
    ResponseFinalizationTimeout,
    /// The peer disconnected.
    PeerDisconnect,
    /// The owning transport failed.
    TransportFailure,
    /// The transport abandoned its service waiter.
    ServiceWaiterDropped,
    /// The execution owner contained a callback poll panic.
    ExecutionPanicked,
    /// An inner invocation was dropped without a more specific stop cause,
    /// including application code abandoning `next` while returning normally.
    ExecutionInterrupted,
}

/// Bounded metadata retained independently of a request's body and local map.
/// Paths are truncated to 2048 UTF-8 bytes; methods to 32. Neither is logged by
/// the termination executor. The identity is request-local, not a trace label.
pub struct HttpRequestTerminationMetadata {
    id: u64,
    method: String,
    path: String,
}

impl HttpRequestTerminationMetadata {
    /// Framework snapshot construction seam.
    #[doc(hidden)]
    pub fn new(id: u64, method: &str, path: &str) -> Self {
        fn bounded(value: &str, limit: usize) -> String {
            let mut end = value.len().min(limit);
            while !value.is_char_boundary(end) {
                end -= 1;
            }
            value[..end].to_owned()
        }
        Self {
            id,
            method: bounded(method, 32),
            path: bounded(path, 2048),
        }
    }
    /// The admitted request's process identity.
    pub fn request_id(&self) -> u64 {
        self.id
    }
    /// Bounded request method snapshot.
    pub fn method(&self) -> &str {
        &self.method
    }
    /// Bounded path snapshot. It must not be used as a metric label.
    pub fn path(&self) -> &str {
        &self.path
    }
}

/// Cleanup-only context for one interrupted middleware invocation.
///
/// The original DI generation is active during this callback. There is no
/// request body, response writer or `next` capability. Explicitly retained
/// state survives the execution stack; arbitrary async locals do not. State
/// must not keep execution input readers or body producers alive: those must
/// release before cleanup can start. A normal-returned invocation is never
/// reopened because of a later response failure.
pub struct HttpRequestTerminationContext<'a> {
    metadata: &'a HttpRequestTerminationMetadata,
    invocation_id: usize,
    stage: HttpMiddlewareStage,
    interruption: HttpRequestInterruption,
    state: &'a mut RequestExtensions,
    extensions: &'a Arc<Extensions>,
    cancellation: CleanupCancellation,
}

impl<'a> HttpRequestTerminationContext<'a> {
    /// Framework invocation construction seam.
    #[doc(hidden)]
    pub fn new(
        metadata: &'a HttpRequestTerminationMetadata,
        invocation_id: usize,
        stage: HttpMiddlewareStage,
        interruption: HttpRequestInterruption,
        state: &'a mut RequestExtensions,
        extensions: &'a Arc<Extensions>,
        cancellation: CleanupCancellation,
    ) -> Self {
        Self {
            metadata,
            invocation_id,
            stage,
            interruption,
            state,
            extensions,
            cancellation,
        }
    }
    /// Bounded metadata captured before execution.
    pub fn metadata(&self) -> &HttpRequestTerminationMetadata {
        self.metadata
    }
    /// Invocation identity within this request, independent of middleware type.
    pub fn invocation_id(&self) -> usize {
        self.invocation_id
    }
    /// Last observed normal execution boundary.
    pub fn stage(&self) -> HttpMiddlewareStage {
        self.stage
    }
    /// Observed interruption, not a guarantee of rollback.
    pub fn interruption(&self) -> HttpRequestInterruption {
        self.interruption
    }
    /// State explicitly retained by this invocation during normal execution.
    pub fn state(&self) -> &RequestExtensions {
        self.state
    }
    /// Mutates or takes this invocation's retained state.
    pub fn state_mut(&mut self) -> &mut RequestExtensions {
        self.state
    }
    /// The original provider; scoped resolution uses the still-live generation.
    pub fn extensions(&self) -> &Arc<Extensions> {
        self.extensions
    }
    /// The same read-only authority passed as the callback parameter.
    pub fn cancellation(&self) -> CleanupCancellation {
        self.cancellation.clone()
    }
}
