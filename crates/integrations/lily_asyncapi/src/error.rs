use thiserror::Error;

/// A fail-closed error produced while configuring or composing an AsyncAPI document.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum AsyncApiBuildError {
    /// A bounded or semantic value did not satisfy the frozen AsyncAPI contract.
    #[error("invalid AsyncAPI field `{field}`: {detail}")]
    Validation {
        /// Stable field path or category.
        field: String,
        /// Secret-safe diagnostic detail.
        detail: String,
    },

    /// Two accepted runtime records attempted to own the same document identity.
    #[error("duplicate AsyncAPI {kind} identifier `{identifier}`")]
    Duplicate {
        /// Kind of document object.
        kind: &'static str,
        /// Conflicting identifier.
        identifier: String,
    },

    /// Two different sources normalized to a conflicting document component.
    #[error("AsyncAPI {kind} collision for `{identifier}`: {detail}")]
    Collision {
        /// Kind of component.
        kind: &'static str,
        /// Conflicting document key.
        identifier: String,
        /// Secret-safe diagnostic detail.
        detail: String,
    },

    /// A document object references an object outside the accepted contribution.
    #[error("invalid AsyncAPI reference from `{owner}` to `{target}`: {detail}")]
    Reference {
        /// Referencing operation or channel.
        owner: String,
        /// Missing or incompatible target.
        target: String,
        /// Secret-safe diagnostic detail.
        detail: String,
    },

    /// A typed schema could not be registered safely.
    #[error("AsyncAPI schema `{schema}` is invalid: {detail}")]
    Schema {
        /// Human-readable schema component name.
        schema: String,
        /// Secret-safe diagnostic detail.
        detail: String,
    },

    /// Canonical JSON serialization failed.
    #[error("failed to serialize the AsyncAPI document: {detail}")]
    Serialization {
        /// Serializer diagnostic.
        detail: String,
    },

    /// Canonical JSON exceeded the document byte budget.
    #[error("canonical AsyncAPI document is {actual} bytes; maximum is {maximum} bytes")]
    DocumentTooLarge {
        /// Serialized byte length.
        actual: usize,
        /// Configured framework maximum.
        maximum: usize,
    },
}

impl AsyncApiBuildError {
    pub(crate) fn validation(field: impl Into<String>, detail: impl Into<String>) -> Self {
        Self::Validation {
            field: field.into(),
            detail: detail.into(),
        }
    }

    pub(crate) fn duplicate(kind: &'static str, identifier: impl Into<String>) -> Self {
        Self::Duplicate {
            kind,
            identifier: identifier.into(),
        }
    }
}
