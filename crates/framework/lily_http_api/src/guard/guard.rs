//! Application-defined request admission guards.
//!
//! Lily owns construction, route ordering, and fail-closed execution. The
//! application owns authentication, authorization, rate-limit state, and the
//! identity or client key used by those policies. A denial returns the shared
//! [`GuardRejection`] contract, for example:
//!
//! ```no_run
//! use lily_http_api::{
//!     Extensions, GuardInitError, GuardRejection, GuardTrait, HttpErrorCode, Request,
//! };
//! use std::{sync::Arc, time::Duration};
//!
//! struct ApplicationRateLimitGuard;
//!
//! #[lily_http_api::async_trait::async_trait]
//! impl GuardTrait for ApplicationRateLimitGuard {
//!     async fn new(_extensions: Arc<Extensions>) -> Result<Self, GuardInitError> {
//!         Ok(Self)
//!     }
//!
//!     async fn can_activate(&self, request: &mut Request, _cancellation: lily_http_api::ExecutionCancellation) -> Result<(), GuardRejection> {
//!         let application_policy_allows_request = request.client_ip().is_some();
//!         if application_policy_allows_request {
//!             return Ok(());
//!         }
//!
//!         Err(GuardRejection::too_many_requests(
//!             HttpErrorCode::new("RATE_LIMITED").expect("static code is valid"),
//!             Duration::from_secs(30),
//!         )
//!         .expect("static rejection policy is valid"))
//!     }
//! }
//! ```

use lily_injection::Extensions;
pub use lily_web_core::HttpRejection as GuardRejection;
use lily_web_core::Request;
use std::sync::Arc;

/// A typed guard-construction failure raised before the HTTP application is
/// published. Messages must describe configuration keys, never secret values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GuardInitError {
    /// A guard-required application setting was not supplied.
    MissingConfiguration {
        /// Stable application-facing guard name.
        guard: &'static str,
        /// Name of the missing setting; never its secret value.
        setting: &'static str,
    },
    /// Guard configuration was present but invalid.
    InvalidConfiguration {
        /// Stable application-facing guard name.
        guard: &'static str,
        /// Secret-safe validation reason.
        reason: String,
    },
    /// A guard dependency could not be resolved or initialized.
    Dependency {
        /// Stable application-facing guard name.
        guard: &'static str,
        /// Secret-safe dependency failure reason.
        reason: String,
    },
}

impl std::fmt::Display for GuardInitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingConfiguration { guard, setting } => {
                write!(
                    formatter,
                    "guard '{guard}' requires configuration '{setting}'"
                )
            }
            Self::InvalidConfiguration { guard, reason } => {
                write!(
                    formatter,
                    "guard '{guard}' has invalid configuration: {reason}"
                )
            }
            Self::Dependency { guard, reason } => {
                write!(formatter, "guard '{guard}' dependency failed: {reason}")
            }
        }
    }
}

impl std::error::Error for GuardInitError {}

/// Contract for application-defined request admission checks.
///
/// Lily owns guard construction, ordering and fail-closed execution. Identity
/// verification, token/session parsing and role or scope policy remain owned by
/// the application and can publish request-local state through [`Request`].
#[async_trait::async_trait]
pub trait GuardTrait: Send + Sync {
    /// Constructs a guard while the application is building. Missing or
    /// invalid configuration must be returned instead of replaced by a
    /// permissive default.
    async fn new(extensions: Arc<Extensions>) -> Result<Self, GuardInitError>
    where
        Self: Sized;

    /// Allows a request or returns a typed, fail-closed rejection.
    ///
    /// The read-only signal matches [`Request::execution_cancellation`]. A
    /// signalled guard may finish cooperatively within the owner's remaining
    /// execution window. It does not own or cancel scope cleanup.
    async fn can_activate(
        &self,
        request: &mut Request,
        cancellation: crate::ExecutionCancellation,
    ) -> Result<(), GuardRejection>;

    /// Returns the stable name used to identify this guard in diagnostics.
    ///
    /// The default is the fully qualified Rust type name. Implement this when
    /// an application needs a shorter, release-stable diagnostic name.
    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }
}
