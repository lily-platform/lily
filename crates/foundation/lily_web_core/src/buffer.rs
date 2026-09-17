//! HTTP transport buffer abstraction backed by [`bytes::BytesMut`].

use bytes::{Bytes, BytesMut};
use lily_error::application::http_api::HttpApiError;

/// Buffer used by Lily's HTTP/1 transport and request/response bodies.
pub struct HttpBuffer {
    inner: BytesMut,
}

impl HttpBuffer {
    /// Allocates an empty buffer with at least `capacity` bytes of storage.
    pub(crate) async fn new(capacity: usize) -> Result<Self, HttpApiError> {
        Ok(Self {
            inner: BytesMut::with_capacity(capacity),
        })
    }

    /// Creates a buffer containing `data`.
    pub(crate) async fn with_data(data: &[u8]) -> Result<Self, HttpApiError> {
        let mut buffer = Self::new(data.len()).await?;
        buffer.extend_from_slice(data).await;
        Ok(buffer)
    }

    #[inline]
    /// Returns the currently visible bytes.
    pub fn as_slice(&self) -> &[u8] {
        self.inner.as_ref()
    }

    #[inline]
    /// Returns the number of visible bytes.
    pub fn len(&self) -> usize {
        self.as_slice().len()
    }

    #[inline]
    /// Returns `true` when no visible bytes remain.
    pub fn is_empty(&self) -> bool {
        self.as_slice().is_empty()
    }

    #[inline]
    /// Returns the allocation capacity of the backing buffer.
    pub(crate) fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    /// Appends bytes to the current contents.
    pub(crate) async fn extend_from_slice(&mut self, bytes: &[u8]) {
        self.inner.extend_from_slice(bytes);
    }

    /// Transfers the visible buffer into an immutable, zero-copy byte view.
    ///
    /// This is used when a request body selects terminal streaming ownership;
    /// the compatibility buffer is no longer retained by the request.
    pub(crate) fn into_bytes(self) -> Bytes {
        self.inner.freeze()
    }

    /// Name of the selected storage backend, intended for diagnostics.
    pub(crate) const fn backend_name() -> &'static str {
        "bytes::BytesMut"
    }
}

impl AsRef<[u8]> for HttpBuffer {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl std::fmt::Debug for HttpBuffer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HttpBuffer")
            .field("backend", &Self::backend_name())
            .field("length", &self.len())
            .field("capacity", &self.capacity())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::HttpBuffer;

    #[test]
    fn reports_the_compiled_backend() {
        assert_eq!(HttpBuffer::backend_name(), "bytes::BytesMut");
    }

    #[tokio::test]
    async fn owned_data_is_exposed_without_transformation() {
        let buffer = HttpBuffer::with_data(b"lily").await.unwrap();

        assert_eq!(buffer.as_slice(), b"lily");
        assert_eq!(buffer.len(), 4);
        assert!(!buffer.is_empty());
    }
}
