//! Shared HTTP value types used by Lily's server, middleware, and client
//! adapters.
//!
//! Application code normally receives this API through `lily_http_api`; a
//! direct dependency on `lily_web_core` is not part of the canonical server
//! setup. The crate nevertheless owns the public request, response, cookie,
//! streaming-body, SSE, static-file, and TLS contracts re-exported by that
//! facade. Internal parsing and transport modules are deliberately not part of
//! the public surface.
//!
//! Request bodies have one ownership mode. Buffered helpers such as
//! [`Request::buffer_body`] may be used together, while selecting the terminal
//! streaming path transfers body ownership and prevents later buffered
//! consumption. Response values implement [`IntoResponse`] and are written
//! into the request-scoped [`Response`] selected by the HTTP runtime.

#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

mod body_budget;
mod buffer;
mod cancellation;
mod cookie;
mod http_resources;
mod request;
mod response;
mod string_interner;
mod tls;

// Public contracts are exposed from one stable root. Their implementation
// modules remain private so source-reading tools do not mistake transport
// internals for alternate APIs.
pub use body_budget::*;
pub use buffer::HttpBuffer;
pub use cancellation::CleanupCancellation;
pub use cookie::*;
/// Shared execution-cancellation view, also exported by `lily_cancellation`.
pub use lily_cancellation::ExecutionCancellation;

/// Cross-crate framework construction seams, not application callback APIs.
#[doc(hidden)]
pub mod __private {
    pub use crate::cancellation::CleanupCancellationSource;
    pub use crate::http_resources::{HttpResourceSnapshot, HttpResources};
    pub use lily_cancellation::__private::ExecutionCancellationSource;

    /// Binds the accepted execution view outside mutable request-local storage.
    pub fn bind_request_execution(
        request: &mut crate::Request,
        view: crate::ExecutionCancellation,
    ) {
        request.set_execution_cancellation(view);
    }
}
/// Compatibility namespace for the HTTP protocol selector.
///
/// Prefer the root-level [`HttpProtocol`] export in new code.
pub mod enums {
    pub use lily_core::HttpProtocol;
}

// Preserve the two HTTP contracts shared with the facade without leaking
// unrelated cache, queue, upload, health, environment, or diagnostic APIs
// from `lily_core`.
#[doc(hidden)]
pub use lily_core::debug_log;
pub use lily_core::{HttpProtocol, RawHeader};
pub use request::*;
pub use response::*;
pub use string_interner::InternedString;
pub use tls::{RustlsConfig, TlsConfigError, TlsFileRole};
