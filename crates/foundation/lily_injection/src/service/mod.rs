//! Lifecycle and lifetime contracts for injectable services.

use async_trait::async_trait;

/// Lifecycle implemented by every service that derives `Injectable`.
///
/// A no-op service can use an empty implementation:
///
/// ```
/// use lily_injection::ServiceTrait;
///
/// struct UserService;
/// impl ServiceTrait for UserService {}
/// ```
///
/// Override [`initialize`](Self::initialize) to acquire owned runtime resources
/// and [`dispose`](Self::dispose) to release them. Application code must not
/// call either method directly; the owning container invokes them exactly at
/// the service lifetime boundaries.
///
/// The `Injectable` derive additionally verifies `Send + Sync + 'static` for
/// the concrete service even though this object-safe lifecycle trait itself
/// only declares `Send`.
#[async_trait]
pub trait ServiceTrait: Send {
    /// Optional lifecycle hook invoked by the owning `ApplicationContainer`.
    ///
    /// Singleton services start during container build. Scoped and transient
    /// services start when that container creates the instance. Application
    /// code must not invoke this hook directly.
    async fn initialize(&mut self) -> Result<(), lily_error::injection::InjectionError> {
        Ok(())
    }

    /// Optional lifecycle hook for idempotent shared cleanup.
    ///
    /// Cleanup deliberately receives `&self`: request code may still retain an
    /// `Arc` when its scope closes, but the owning container must be able to
    /// close shared resources without requiring unique ownership. Services
    /// should keep mutable shutdown state behind synchronization primitives and
    /// make repeated calls harmless. Cleanup may run in a container-owned task,
    /// so implementations must carry required state explicitly instead of
    /// relying on the identity or arbitrary task-local values of the caller.
    async fn dispose(&self) -> Result<(), lily_error::injection::InjectionError> {
        Ok(())
    }
}

mod service_lifetime;
// service_registry module deprecated - moved to lily_injection_registry crate

/// Lifetime selected by `#[service(lifetime = "...")]`.
///
/// Application code normally chooses this through the derive attribute rather
/// than constructing this enum directly.
pub use lily_injection_registry::ServiceLifetime;
pub(crate) use service_lifetime::ServiceDescriptor;
// service_registry exports removed - use lily_injection_registry instead
