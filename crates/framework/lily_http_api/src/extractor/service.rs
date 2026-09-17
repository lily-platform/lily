use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

use lily_error::application::http_api::HttpApiError;
use lily_injection::{Extensions, InjectionError};
use lily_web_core::Request;

use super::FromRequestParts;

/// An action-time dependency resolved inside the current request scope.
///
/// `Service<T>` uses the application's existing DI authority, so concrete
/// services and derive-declared `dyn Trait` interfaces preserve their
/// registered singleton, scoped or transient lifetime. Frequently used
/// singleton dependencies should normally remain controller fields; this
/// extractor is the canonical way to request scoped and transient services.
///
/// Concrete and registered trait-interface dependencies use the same action
/// syntax:
///
/// ```
/// use lily_http_api::Service;
///
/// trait Clock: Send + Sync {
///     fn unix_seconds(&self) -> u64;
/// }
///
/// async fn concrete(_clock: Service<SystemClock>) {}
/// async fn interface(_clock: Service<dyn Clock>) {}
///
/// struct SystemClock;
/// ```
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

impl<T> FromRequestParts for Service<T>
where
    T: ?Sized + Send + Sync + 'static,
{
    type Rejection = InjectionError;

    async fn from_request_parts(
        _request: &mut Request,
        extensions: &Extensions,
    ) -> Result<Self, Self::Rejection> {
        extensions.get_service::<T>(None).await.map(Self)
    }
}

/// Borrowed service extraction seam used by generated action adapters.
#[doc(hidden)]
pub trait ServiceRequestExtractor: Sized {
    /// Resolves one service while borrowing the adapter-owned provider only
    /// until this extraction future completes.
    fn extract_service<'a>(
        extensions: &'a Extensions,
    ) -> impl std::future::Future<Output = Result<Self, HttpApiError>> + Send + 'a + use<'a, Self>;
}

impl<T> ServiceRequestExtractor for Service<T>
where
    T: ?Sized + Send + Sync + 'static,
{
    async fn extract_service(extensions: &Extensions) -> Result<Self, HttpApiError> {
        extensions
            .get_service::<T>(None)
            .await
            .map(Self)
            .map_err(HttpApiError::from)
    }
}

/// Resolves the action's fixed `Service<T>` request-parts parameter.
#[doc(hidden)]
pub fn extract_service<'a, S>(
    extensions: &'a Extensions,
) -> impl std::future::Future<Output = Result<S, HttpApiError>> + Send + 'a + use<'a, S>
where
    S: ServiceRequestExtractor,
{
    S::extract_service(extensions)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::Service;

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
    fn wrapper_preserves_arc_identity_for_concrete_and_trait_objects() {
        let concrete = Arc::new(ProbeService(42));
        let concrete_service = Service(Arc::clone(&concrete));
        assert_eq!(concrete_service.value(), 42);
        assert!(Arc::ptr_eq(
            &concrete,
            &concrete_service.clone().into_inner()
        ));

        let interface: Arc<dyn Probe> = concrete;
        let interface_service = Service(Arc::clone(&interface));
        assert_eq!(interface_service.as_ref().value(), 42);
        assert!(Arc::ptr_eq(&interface, &interface_service.into_inner()));
    }
}
