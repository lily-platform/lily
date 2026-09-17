use std::{error::Error, fmt, sync::Arc};

const INVALID_RETRYABLE_CODE: &str = "QUEUE_HANDLER_RETRYABLE_CODE_INVALID";
const INVALID_PERMANENT_CODE: &str = "QUEUE_HANDLER_PERMANENT_CODE_INVALID";
const MAX_ERROR_CODE_BYTES: usize = 64;

/// Explicit settlement class selected by a queue handler or extractor.
///
/// Lily never infers this distinction from an application's business error.
/// A retryable failure may use the queue's bounded retry policy before it is
/// dead-lettered; a permanent failure is dead-lettered immediately.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum QueueHandlerFailureClass {
    /// The delivery may be attempted again under the configured retry budget.
    Retryable,
    /// Re-running the same delivery is not expected to succeed.
    Permanent,
}

/// Typed, secret-safe failure returned by a queue handler or extractor.
///
/// Application handlers may return this type directly or return an
/// application error implementing `Into<QueueHandlerError>`. The stable code
/// is suitable for bounded telemetry and retry/DLQ headers. Invalid codes are
/// replaced with a class-specific framework code rather than being emitted.
/// Sources are retained for in-process diagnostics but are deliberately
/// omitted from `Debug` and `Display`.
#[derive(Clone)]
pub struct QueueHandlerError {
    class: QueueHandlerFailureClass,
    code: &'static str,
    source: Option<Arc<dyn Error + Send + Sync>>,
}

impl QueueHandlerError {
    /// Creates a retryable failure with a stable application code.
    #[must_use]
    pub fn retryable(code: &'static str) -> Self {
        Self::new(QueueHandlerFailureClass::Retryable, code)
    }

    /// Creates a permanent failure with a stable application code.
    #[must_use]
    pub fn permanent(code: &'static str) -> Self {
        Self::new(QueueHandlerFailureClass::Permanent, code)
    }

    /// Creates a retryable failure while retaining an internal source.
    #[must_use]
    pub fn retryable_with_source<E>(code: &'static str, source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self::with_source(QueueHandlerFailureClass::Retryable, code, source)
    }

    /// Creates a permanent failure while retaining an internal source.
    #[must_use]
    pub fn permanent_with_source<E>(code: &'static str, source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self::with_source(QueueHandlerFailureClass::Permanent, code, source)
    }

    /// Returns the explicit retry/permanent classification.
    #[must_use]
    pub const fn class(&self) -> QueueHandlerFailureClass {
        self.class
    }

    /// Returns a bounded, telemetry-safe failure code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        self.code
    }

    fn new(class: QueueHandlerFailureClass, code: &'static str) -> Self {
        Self {
            class,
            code: validated_code(class, code),
            source: None,
        }
    }

    fn with_source<E>(class: QueueHandlerFailureClass, code: &'static str, source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self {
            class,
            code: validated_code(class, code),
            source: Some(Arc::new(source)),
        }
    }
}

impl fmt::Debug for QueueHandlerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QueueHandlerError")
            .field("class", &self.class)
            .field("code", &self.code)
            .field("has_source", &self.source.is_some())
            .finish()
    }
}

impl fmt::Display for QueueHandlerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "queue handler failed ({})", self.code)
    }
}

impl Error for QueueHandlerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn Error + 'static))
    }
}

fn validated_code(class: QueueHandlerFailureClass, code: &'static str) -> &'static str {
    if is_valid_code(code) {
        code
    } else {
        match class {
            QueueHandlerFailureClass::Retryable => INVALID_RETRYABLE_CODE,
            QueueHandlerFailureClass::Permanent => INVALID_PERMANENT_CODE,
        }
    }
}

fn is_valid_code(code: &str) -> bool {
    let bytes = code.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= MAX_ERROR_CODE_BYTES
        && bytes[0].is_ascii_alphanumeric()
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SENSITIVE_SENTINEL: &str = "QUEUE-HANDLER-SENSITIVE-SOURCE";

    #[derive(Debug)]
    struct SensitiveSource;

    impl fmt::Display for SensitiveSource {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str(SENSITIVE_SENTINEL)
        }
    }

    impl Error for SensitiveSource {}

    #[test]
    fn failure_class_and_safe_code_are_explicit() {
        let retryable = QueueHandlerError::retryable("ORDER_STORE_UNAVAILABLE");
        let permanent = QueueHandlerError::permanent("ORDER_PAYLOAD_INVALID");

        assert_eq!(retryable.class(), QueueHandlerFailureClass::Retryable);
        assert_eq!(retryable.code(), "ORDER_STORE_UNAVAILABLE");
        assert_eq!(permanent.class(), QueueHandlerFailureClass::Permanent);
        assert_eq!(permanent.code(), "ORDER_PAYLOAD_INVALID");
    }

    #[test]
    fn invalid_codes_fail_closed_to_bounded_static_codes() {
        assert_eq!(
            QueueHandlerError::retryable("contains secret value").code(),
            INVALID_RETRYABLE_CODE
        );
        assert_eq!(
            QueueHandlerError::permanent("").code(),
            INVALID_PERMANENT_CODE
        );
        assert_eq!(
            QueueHandlerError::permanent("X".repeat(65).leak()).code(),
            INVALID_PERMANENT_CODE
        );
    }

    #[test]
    fn formatting_does_not_expose_retained_source() {
        let error =
            QueueHandlerError::retryable_with_source("ORDER_STORE_UNAVAILABLE", SensitiveSource);

        assert!(!format!("{error}").contains(SENSITIVE_SENTINEL));
        assert!(!format!("{error:?}").contains(SENSITIVE_SENTINEL));
        assert!(error.source().is_some());
    }
}
