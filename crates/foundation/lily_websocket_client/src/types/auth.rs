use crate::WebSocketError;
use async_trait::async_trait;
use std::path::PathBuf;

/// Refreshes authentication headers immediately before every initial or
/// reconnect handshake. Implementations may fetch or rotate credentials.
#[async_trait]
pub trait AuthHeaderProvider: Send + Sync {
    /// Produces the complete set of dynamic authentication headers for the
    /// next handshake.
    ///
    /// The client validates header names and values and applies a bounded
    /// deadline. Implementations must not log or retain short-lived secrets.
    async fn headers(&self) -> Result<Vec<(String, String)>, WebSocketError>;
}

/// Static provider useful for API keys that are already managed by a secret
/// store outside the client.
#[derive(Clone, Default)]
pub struct StaticAuthHeaders(
    /// Header name/value pairs copied into every handshake request.
    pub Vec<(String, String)>,
);

impl std::fmt::Debug for StaticAuthHeaders {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names = self
            .0
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>();
        formatter
            .debug_struct("StaticAuthHeaders")
            .field("header_names", &names)
            .finish()
    }
}

#[async_trait]
impl AuthHeaderProvider for StaticAuthHeaders {
    async fn headers(&self) -> Result<Vec<(String, String)>, WebSocketError> {
        Ok(self.0.clone())
    }
}

/// Bearer credential provider backed by a bounded file.
///
/// The file is re-read immediately before every initial and reconnect
/// handshake. This supports atomic secret rotation without retaining the
/// credential in configuration snapshots or debug output.
#[derive(Clone)]
pub struct BearerTokenFile {
    path: PathBuf,
}

impl BearerTokenFile {
    /// Creates a provider that re-reads `path` before every handshake.
    ///
    /// The path must be non-empty. File type, size, and credential content are
    /// validated when [`AuthHeaderProvider::headers`] is called.
    pub fn new(path: impl Into<PathBuf>) -> Result<Self, WebSocketError> {
        let path = path.into();
        if path.as_os_str().is_empty() {
            return Err(WebSocketError::InvalidConfiguration(
                "authorization_bearer_file cannot be empty".into(),
            ));
        }
        Ok(Self { path })
    }
}

impl std::fmt::Debug for BearerTokenFile {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BearerTokenFile")
            .field("configured", &true)
            .finish()
    }
}

#[async_trait]
impl AuthHeaderProvider for BearerTokenFile {
    async fn headers(&self) -> Result<Vec<(String, String)>, WebSocketError> {
        const MAX_TOKEN_BYTES: u64 = 16 * 1024;
        let metadata = tokio::fs::metadata(&self.path)
            .await
            .map_err(|_| WebSocketError::Authentication("credential file unavailable".into()))?;
        if !metadata.is_file() || metadata.len() == 0 || metadata.len() > MAX_TOKEN_BYTES {
            return Err(WebSocketError::Authentication(
                "credential file size or type is invalid".into(),
            ));
        }
        let value = tokio::fs::read_to_string(&self.path)
            .await
            .map_err(|_| WebSocketError::Authentication("credential file unavailable".into()))?;
        let token = value.trim();
        if token.is_empty()
            || token.len() > MAX_TOKEN_BYTES as usize
            || token.chars().any(char::is_whitespace)
            || token.chars().any(char::is_control)
        {
            return Err(WebSocketError::Authentication(
                "credential file content is invalid".into(),
            ));
        }
        Ok(vec![("authorization".into(), format!("Bearer {token}"))])
    }
}

#[cfg(test)]
mod tests {
    use super::{AuthHeaderProvider, BearerTokenFile};

    #[tokio::test]
    async fn file_provider_refreshes_and_debug_never_exposes_the_token() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("token");
        tokio::fs::write(&path, "first-token\n").await.unwrap();
        let provider = BearerTokenFile::new(&path).unwrap();
        assert_eq!(
            provider.headers().await.unwrap(),
            [("authorization".into(), "Bearer first-token".into())]
        );
        tokio::fs::write(&path, "second-token\n").await.unwrap();
        assert_eq!(
            provider.headers().await.unwrap(),
            [("authorization".into(), "Bearer second-token".into())]
        );
        let debug = format!("{provider:?}");
        assert!(!debug.contains("first-token"));
        assert!(!debug.contains("second-token"));
        assert!(!debug.contains(path.to_string_lossy().as_ref()));
    }

    #[tokio::test]
    async fn file_provider_rejects_whitespace_and_unbounded_content() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("token");
        tokio::fs::write(&path, "two tokens").await.unwrap();
        let provider = BearerTokenFile::new(&path).unwrap();
        assert!(provider.headers().await.is_err());
        tokio::fs::write(&path, "x".repeat(16 * 1024 + 1))
            .await
            .unwrap();
        assert!(provider.headers().await.is_err());
    }
}
