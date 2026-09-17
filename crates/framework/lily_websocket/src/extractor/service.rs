use std::fmt;
use std::future::Future;
use std::ops::Deref;
use std::sync::Arc;

use crate::outcome::WebSocketActionError;

use super::{
    FromWebSocketMessageParts, WebSocketExtractionError, WebSocketExtractionFailureKind,
    WebSocketMessageInvocation,
};

/// Action-time dependency resolved inside the active message DI scope.
///
/// Concrete services and derive-declared `dyn Trait` routes preserve their
/// registered singleton, scoped or transient lifetime.
pub struct Service<T: ?Sized + 'static>(pub Arc<T>);

impl<T: ?Sized + 'static> Service<T> {
    /// Returns shared ownership of the resolved dependency.
    #[must_use]
    pub fn into_inner(self) -> Arc<T> {
        self.0
    }
}

impl<T: ?Sized + 'static> Clone for Service<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<T: ?Sized + 'static> Deref for Service<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl<T: ?Sized + 'static> AsRef<T> for Service<T> {
    fn as_ref(&self) -> &T {
        self.0.as_ref()
    }
}

impl<T: ?Sized + 'static> fmt::Debug for Service<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Service")
            .field("service_type", &std::any::type_name::<T>())
            .finish_non_exhaustive()
    }
}

impl<T> FromWebSocketMessageParts for Service<T>
where
    T: ?Sized + Send + Sync + 'static,
{
    type Rejection = WebSocketActionError;

    #[allow(clippy::manual_async_fn)]
    fn from_message_parts(
        invocation: &mut WebSocketMessageInvocation,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        let extensions = invocation.extensions();
        async move {
            extensions
                .get_service::<T>(None)
                .await
                .map(Self)
                .map_err(|error| {
                    WebSocketExtractionError::with_source(
                        WebSocketExtractionFailureKind::Service,
                        error,
                    )
                    .into()
                })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    trait Probe: Send + Sync {
        fn value(&self) -> usize;
    }

    struct ProbeService(usize);

    impl Probe for ProbeService {
        fn value(&self) -> usize {
            self.0
        }
    }

    #[test]
    fn wrapper_preserves_concrete_and_trait_arc_identity() {
        let concrete = Arc::new(ProbeService(42));
        let service = Service(Arc::clone(&concrete));
        assert_eq!(service.value(), 42);
        assert!(Arc::ptr_eq(&concrete, &service.into_inner()));

        let interface: Arc<dyn Probe> = concrete;
        let service = Service(Arc::clone(&interface));
        assert_eq!(service.value(), 42);
        assert!(Arc::ptr_eq(&interface, &service.into_inner()));
    }
}
