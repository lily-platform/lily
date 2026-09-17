//! Application-defined admission guards for routed WebSocket messages.
//!
//! Lily owns guard construction, deterministic global/controller/action
//! ordering and fail-closed execution. Authentication, authorization,
//! rate-limiting and business policy remain application responsibilities.
//! Guards can publish message-local state through
//! [`WsMessageExchange`](crate::middleware::WsMessageExchange) for
//! later guards and typed action extractors.

use std::{fmt, sync::Arc};

use async_trait::async_trait;
use lily_injection::Extensions;

use crate::{
    codec::DecodedWebSocketPayload,
    middleware::WsMessageExchange,
    outcome::{
        CloseConnection, PendingWebSocketActionOutcome, WebSocketActionError,
        WebSocketActionErrorDisposition, WebSocketErrorCode, WebSocketOutcomeConfigError,
    },
};

/// WebSocket message admission contract.
///
/// Lily invokes [`Self::new`] once for each concrete guard type while building
/// an application and shares that app-owned instance across every effective
/// route plan that references it. Singleton DI services may be retained by the
/// guard. Directly constructed resources and raw spawned tasks are
/// application-owned. Scoped and transient dependencies belong in
/// [`Self::can_activate`], where [`WsMessageExchange::service`] resolves them
/// from the active message scope.
///
/// Guards run in `global -> controller -> action` order after all effective
/// message middleware has entered and before typed extraction consumes payload
/// authority. A rejection prevents every remaining guard, extractor and action
/// from running; entered middleware still unwinds in reverse order.
#[async_trait]
pub trait WsGuard: Send + Sync + 'static {
    /// Constructs this application's single guard instance.
    ///
    /// Missing dependencies and invalid policy must abort `WsApp::build`
    /// instead of producing a permissive fallback.
    async fn new(extensions: Arc<Extensions>) -> Result<Self, GuardInitializationError>
    where
        Self: Sized;

    /// Allows the current routed message or returns one typed rejection.
    /// The read-only signal matches [`WsMessageExchange::cancellation`].
    async fn can_activate(
        &self,
        exchange: &mut WsMessageExchange,
        _cancellation: crate::ExecutionCancellation,
    ) -> Result<(), WebSocketGuardRejection>;

    /// Stable guard identity used only for bounded diagnostics.
    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }
}

/// Safe startup failure returned by a guard constructor.
///
/// The category is intentionally closed and source-free. Constructor code may
/// log an underlying application error at its own trusted boundary, but must
/// not attach provider text, credentials or request data to this value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum GuardInitializationError {
    /// A required application-lifetime service could not be resolved.
    #[error("required guard dependency is unavailable")]
    MissingDependency,
    /// Immutable guard policy is invalid.
    #[error("guard policy is invalid")]
    InvalidPolicy,
    /// Guard construction failed for another internal reason.
    #[error("guard initialization failed")]
    Internal,
}

impl GuardInitializationError {
    /// Stable low-cardinality diagnostic code for logs and metrics.
    #[must_use]
    pub const fn diagnostic_code(self) -> &'static str {
        match self {
            Self::MissingDependency => "WS_GUARD_DEPENDENCY_MISSING",
            Self::InvalidPolicy => "WS_GUARD_POLICY_INVALID",
            Self::Internal => "WS_GUARD_INITIALIZATION_FAILED",
        }
    }
}

/// Wire behavior selected by an intentional guard rejection.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum WebSocketGuardRejectionDisposition {
    /// Emit one safe error envelope and keep the connection open.
    Error,
    /// Emit one safe acknowledgement payload correlated to the inbound
    /// message's `ack_id`.
    Ack,
    /// Close the connection with the supplied bounded RFC 6455 decision.
    Close(CloseConnection),
}

/// Bounded, source-free rejection returned by a WebSocket guard.
///
/// The public code/message pair is validated by the same contract as
/// [`WebSocketActionError`]. `Ack` deliberately carries only the safe
/// `{code, message}` rejection object; guards cannot attach arbitrary payload
/// data at this boundary.
#[derive(Clone)]
pub struct WebSocketGuardRejection {
    error: WebSocketActionError,
    disposition: WebSocketGuardRejectionDisposition,
}

impl WebSocketGuardRejection {
    /// Creates a safe error-envelope rejection that keeps the connection open.
    pub fn error(
        code: WebSocketErrorCode,
        public_message: impl Into<String>,
    ) -> Result<Self, WebSocketOutcomeConfigError> {
        Ok(Self {
            error: WebSocketActionError::rejected(code, public_message)?,
            disposition: WebSocketGuardRejectionDisposition::Error,
        })
    }

    /// Creates a safe acknowledgement rejection.
    ///
    /// Runtime materialization requires the inbound message to carry an
    /// acknowledgement authority. If it does not, the canonical action-output
    /// contract converts this decision into the bounded missing-authority
    /// error instead of inventing an identifier.
    pub fn ack(
        code: WebSocketErrorCode,
        public_message: impl Into<String>,
    ) -> Result<Self, WebSocketOutcomeConfigError> {
        Ok(Self {
            error: WebSocketActionError::rejected(code, public_message)?,
            disposition: WebSocketGuardRejectionDisposition::Ack,
        })
    }

    /// Creates a safe rejection that closes the connection.
    pub fn close(
        code: WebSocketErrorCode,
        public_message: impl Into<String>,
        close: CloseConnection,
    ) -> Result<Self, WebSocketOutcomeConfigError> {
        Ok(Self {
            error: WebSocketActionError::closing(code, public_message, close.clone())?,
            disposition: WebSocketGuardRejectionDisposition::Close(close),
        })
    }

    /// Stable wire and telemetry code.
    #[must_use]
    pub const fn code(&self) -> WebSocketErrorCode {
        self.error.code()
    }

    /// Bounded application-safe message.
    #[must_use]
    pub fn public_message(&self) -> &str {
        self.error.public_message()
    }

    /// Terminal behavior selected by this rejection.
    #[must_use]
    pub const fn disposition(&self) -> &WebSocketGuardRejectionDisposition {
        &self.disposition
    }

    /// Converts guard policy into the existing typed action terminal boundary.
    pub(crate) fn into_action_result(
        self,
    ) -> Result<PendingWebSocketActionOutcome, WebSocketActionError> {
        let Self { error, disposition } = self;
        match disposition {
            WebSocketGuardRejectionDisposition::Error => Err(error),
            WebSocketGuardRejectionDisposition::Ack => {
                let payload = serde_json::json!({
                    "code": error.code().as_str(),
                    "message": error.public_message(),
                });
                Ok(PendingWebSocketActionOutcome::Ack(
                    DecodedWebSocketPayload::Json(payload),
                ))
            }
            WebSocketGuardRejectionDisposition::Close(close) => {
                debug_assert!(matches!(
                    error.disposition(),
                    WebSocketActionErrorDisposition::Close(_)
                ));
                Ok(PendingWebSocketActionOutcome::Close(close))
            }
        }
    }
}

impl fmt::Debug for WebSocketGuardRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WebSocketGuardRejection")
            .field("code", &self.code())
            .field("public_message_bytes", &self.public_message().len())
            .field("disposition", &self.disposition)
            .finish()
    }
}

impl fmt::Display for WebSocketGuardRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "WebSocket guard rejected the message with code {}",
            self.code()
        )
    }
}

impl std::error::Error for WebSocketGuardRejection {}

/// Runtime-owned guard chain compiled from one immutable effective route plan.
pub(crate) struct GuardChain<'plan> {
    guards: &'plan [Arc<dyn WsGuard>],
}

impl<'plan> GuardChain<'plan> {
    /// Borrows the global -> controller -> action guard plan without cloning it.
    pub(crate) const fn new(guards: &'plan [Arc<dyn WsGuard>]) -> Self {
        Self { guards }
    }

    /// Executes guards in plan order and stops at the first rejection.
    pub(crate) async fn execute(
        &self,
        exchange: &mut WsMessageExchange,
    ) -> Result<(), WebSocketGuardRejection> {
        for guard in self.guards {
            let cancellation = exchange.cancellation().clone();
            guard.can_activate(exchange, cancellation).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn denied_code() -> WebSocketErrorCode {
        WebSocketErrorCode::new("TEST_GUARD_DENIED").expect("static code is valid")
    }

    #[test]
    fn error_rejection_reuses_bounded_action_error_contract() {
        let rejection = WebSocketGuardRejection::error(denied_code(), "Access denied.")
            .expect("safe rejection is valid");

        assert_eq!(rejection.code(), denied_code());
        assert_eq!(rejection.public_message(), "Access denied.");
        assert_eq!(
            rejection.disposition(),
            &WebSocketGuardRejectionDisposition::Error
        );

        let error = rejection
            .into_action_result()
            .expect_err("error disposition becomes the existing action error boundary");
        assert_eq!(error.code(), denied_code());
        assert!(matches!(
            error.disposition(),
            WebSocketActionErrorDisposition::KeepOpen
        ));
    }

    #[test]
    fn ack_rejection_has_only_the_safe_code_and_message_payload() {
        let rejection = WebSocketGuardRejection::ack(denied_code(), "Quota exceeded.")
            .expect("safe acknowledgement rejection is valid");

        let PendingWebSocketActionOutcome::Ack(DecodedWebSocketPayload::Json(payload)) = rejection
            .into_action_result()
            .expect("ack disposition becomes a pending typed outcome")
        else {
            panic!("guard acknowledgement must use one JSON rejection payload");
        };
        assert_eq!(
            payload,
            serde_json::json!({
                "code": "TEST_GUARD_DENIED",
                "message": "Quota exceeded.",
            })
        );
    }

    #[test]
    fn close_rejection_preserves_the_validated_close_decision() {
        let close = CloseConnection::policy("Policy rejected the message.")
            .expect("static close reason is valid");
        let rejection =
            WebSocketGuardRejection::close(denied_code(), "Access denied.", close.clone())
                .expect("safe close rejection is valid");

        assert_eq!(
            rejection.disposition(),
            &WebSocketGuardRejectionDisposition::Close(close.clone())
        );
        let PendingWebSocketActionOutcome::Close(actual) = rejection
            .into_action_result()
            .expect("close disposition becomes a pending typed outcome")
        else {
            panic!("guard close must remain a close outcome");
        };
        assert_eq!(actual, close);
    }

    #[test]
    fn invalid_public_message_is_rejected_and_debug_is_payload_safe() {
        assert!(WebSocketGuardRejection::error(denied_code(), "unsafe\nmessage").is_err());

        let public_message = "A public but sensitive-looking diagnostic token";
        let rejection = WebSocketGuardRejection::ack(denied_code(), public_message)
            .expect("bounded public text is accepted");
        let diagnostics = format!("{rejection:?} {rejection}");
        assert!(!diagnostics.contains(public_message));
        assert!(diagnostics.contains("TEST_GUARD_DENIED"));
    }
}
