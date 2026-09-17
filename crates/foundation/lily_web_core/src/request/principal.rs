use serde_json::{Map, Value};
use std::collections::BTreeSet;
use std::sync::Arc;

/// An authenticated identity attached to one HTTP request or WebSocket connection.
///
/// Lily never builds this value from an unverified token. An application
/// authentication boundary attaches it only after its verifier has validated
/// the credential's signature and mandatory claims.
#[derive(Clone, PartialEq)]
pub struct Principal {
    inner: Arc<PrincipalInner>,
}

#[derive(PartialEq)]
struct PrincipalInner {
    subject: String,
    roles: BTreeSet<String>,
    scopes: BTreeSet<String>,
    claims: Map<String, Value>,
}

impl std::fmt::Debug for Principal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Principal")
            .field("subject", &"<redacted>")
            .field("role_count", &self.inner.roles.len())
            .field("scope_count", &self.inner.scopes.len())
            .field("claim_count", &self.inner.claims.len())
            .finish()
    }
}

impl Principal {
    /// Builds a principal returned by a trusted authentication provider.
    pub fn new(
        subject: impl Into<String>,
        roles: impl IntoIterator<Item = String>,
        scopes: impl IntoIterator<Item = String>,
        claims: Map<String, Value>,
    ) -> Self {
        Self {
            inner: Arc::new(PrincipalInner {
                subject: subject.into(),
                roles: roles.into_iter().collect(),
                scopes: scopes.into_iter().collect(),
                claims,
            }),
        }
    }

    /// Returns the verified subject identifier.
    pub fn subject(&self) -> &str {
        &self.inner.subject
    }

    /// Returns the verified role set.
    pub fn roles(&self) -> &BTreeSet<String> {
        &self.inner.roles
    }

    /// Returns the verified authorization scope set.
    pub fn scopes(&self) -> &BTreeSet<String> {
        &self.inner.scopes
    }

    /// Returns the verified claim set. Callers must treat claim values as
    /// sensitive and must not log the complete map.
    pub fn claims(&self) -> &Map<String, Value> {
        &self.inner.claims
    }

    /// Returns whether the principal has `role`.
    pub fn has_role(&self, role: &str) -> bool {
        self.inner.roles.contains(role)
    }

    /// Returns whether the principal has `scope`.
    pub fn has_scope(&self, scope: &str) -> bool {
        self.inner.scopes.contains(scope)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn debug_output_redacts_identity_and_claim_values() {
        let mut claims = Map::new();
        claims.insert("tenant".to_string(), json!("sensitive-tenant"));
        let principal = Principal::new(
            "sensitive-subject",
            ["sensitive-role".to_string()],
            ["sensitive-scope".to_string()],
            claims,
        );

        let output = format!("{principal:?}");
        for sensitive in [
            "sensitive-subject",
            "sensitive-role",
            "sensitive-scope",
            "sensitive-tenant",
        ] {
            assert!(!output.contains(sensitive));
        }
        assert!(output.contains("role_count: 1"));
        assert!(output.contains("scope_count: 1"));
        assert!(output.contains("claim_count: 1"));
    }

    #[test]
    fn clone_shares_the_immutable_identity_payload() {
        let principal = Principal::new("subject", [], [], Map::new());
        let cloned = principal.clone();

        assert!(Arc::ptr_eq(&principal.inner, &cloned.inner));
        assert_eq!(principal, cloned);
    }
}
