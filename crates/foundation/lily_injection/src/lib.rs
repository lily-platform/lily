#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

//! Application-owned dependency injection for Lily.
//!
//! Most Lily HTTP, WebSocket and consumer applications do not build this
//! container directly: their application builder owns one. Application code
//! normally interacts with DI in two places:
//!
//! 1. declare services with `#[derive(lily_injection::Injectable)]`,
//!    `#[service(...)]`, `#[inject]` and an implementation of [`ServiceTrait`];
//! 2. resolve a service from [`Extensions::get_service`] when constructor
//!    injection or a framework-provided typed service extractor is not the
//!    appropriate boundary.
//!
//! `lily_injection_registry` is not an application API. It is the link-time ABI
//! shared by the derive and this runtime. Do not construct `ServiceMetadata`,
//! register factories or inspect distributed slices in application code.
//!
//! # Service definition
//!
//! ```
//! use std::sync::Arc;
//! use lily_injection::{Injectable, ServiceTrait};
//!
//! #[derive(Default, Injectable)]
//! #[service(lifetime = "Singleton")]
//! struct Clock;
//! impl ServiceTrait for Clock {}
//!
//! #[derive(Injectable)]
//! #[service(lifetime = "Singleton")]
//! struct AuditService {
//!     #[inject]
//!     clock: Arc<Clock>,
//! }
//! impl ServiceTrait for AuditService {}
//! ```
//!
//! `#[inject]` fields use `Arc<T>` or `Arc<dyn Trait>`. Constructor injection is
//! preferred: a service stores the dependencies it needs instead of resolving
//! them repeatedly from a provider.
//!
//! # Resolution
//!
//! [`Extensions`] is a read-only provider. It has no public registration or
//! cache-mutation API:
//!
//! ```ignore
//! let service = extensions.get_service::<AuditService>(None).await?;
//! let interface = extensions.get_service::<dyn AuditApi>(None).await?;
//! ```
//!
//! `None` uses the current task-local [`ProcessContext`] when one exists.
//! Singleton services and root-owned transients can resolve without a scope.
//! Scoped services require an active scope created by the application adapter
//! or [`ApplicationContainer::run_scoped`]. A transient resolved inside a scope
//! is disposed with that scope; a transient resolved from the root is disposed
//! when its container closes.
//!
//! Each scope generation captures the trace identity and subscriber active at
//! creation. Its owned `di.scope.dispose` task restores that subscriber on
//! both polling and cancellation/drop, with the captured identity as parent.
//! This also applies to manager-forced cleanup and process ID reuse; a later
//! closing task's ambient span cannot replace the owner. The original
//! [`ProcessContext`] is retained for forced disposal as well.
//! Only the parent's identity is retained, so keeping a scope open does not
//! keep an already-finished request span open. Without an active traced owner,
//! cleanup starts a new SDK root. Standalone callers should create the scope
//! inside the span that owns the request/job.
//!
//! `get_service` uses a DEBUG `di.service.resolve` span and one lifecycle pair
//! per polled invocation. It records `di.requested_type`, plus
//! `di.implementation_type` and `di.lifetime` when registration metadata is
//! available. Framework-attached handles are identified as singletons. These
//! diagnostic names come from Rust type metadata, never instance values or
//! request data, and can change when code is refactored.
//!
//! Resolution errors, including early not-found/closed-container returns, emit
//! an ERROR event with a static `lily.error_code` (for example,
//! `di.service_not_found`). It remains attached to the caller's span when DEBUG
//! is filtered out. Error messages and context metadata are not formatted.
//! Enabling DEBUG reveals resolution details; ordinary INFO logging remains
//! quiet on successful resolution. Existing metric instruments, labels, and
//! admission accounting do not depend on this log filter. Build and shutdown
//! diagnostics keep their existing levels.
//!
//! # Standalone composition
//!
//! Code that is not hosted by a Lily application adapter may own the container
//! explicitly:
//!
//! ```ignore
//! let container = ApplicationContainer::build().await?;
//! let service = container.resolve::<AuditService>(None).await?;
//! container.close().await?;
//! ```
//!
//! Do not build a second container inside an HTTP/WebSocket/consumer
//! application. Resolve from that application's provider so singleton, scope
//! and shutdown ownership remain unified.
//!
//! # Lifetime and lifecycle
//!
//! - `Singleton`: one eagerly initialized instance per application container;
//! - `Scoped`: one lazy instance per request/job scope;
//! - `Transient`: a new lazy instance per resolution.
//!
//! [`ServiceTrait::initialize`] runs exactly once for every created instance.
//! [`ServiceTrait::dispose`] runs from the owner that recorded that instance.
//! Container build validates missing dependencies, duplicate interface routes,
//! lifetime capture and cycles before starting singleton services.
//! Dropping a pending build rejects further provider work, publishes any
//! partially initialized singleton to the lifecycle ledger, and hands reverse
//! disposal to a detached rollback task. That asynchronous fallback is
//! guaranteed while the Tokio runtime that polled the build remains alive.
//! Build rollback shares one aggregate budget for resolution drain and root
//! disposal. Set [`BUILD_ROLLBACK_TIMEOUT_ENV`] to a whole number of seconds
//! to override it without constructing `ConfigService`; an absent variable
//! uses [`DEFAULT_SHUTDOWN_TIMEOUT`] (30 seconds). Invalid, zero, and excessive
//! values fail the container build before a service starts. The value is read
//! once when the build transaction is created, so later environment mutation
//! cannot change an in-flight rollback deadline. This setting does not alter
//! the normal runtime shutdown timeout selected by an application adapter.
//!
//! ```text
//! LILY_INJECTION_BUILD_ROLLBACK_TIMEOUT_SECS=45
//! ```
//!
//! A still-running factory prevents root disposal rather than racing it; typed
//! outcomes and retained work counters describe incomplete cleanup. Pending
//! asynchronous lifecycle work requires a Tokio time driver. An entirely
//! synchronous rollback can still complete on a runtime without one, while
//! missing timer support for pending work is contained as lifecycle failure
//! evidence.
//!
//! [`ApplicationScopeFactory`] adds a single-use job scope API without changing
//! [`ApplicationScope::run`]. Share the factory as an `Arc`, create a fresh
//! [`ProcessContext`] per job, then call [`ServiceScope::run`]. Its callback
//! receives `&Extensions` inside the correct context. It awaits disposal on both
//! success and error; disposal failure takes precedence over an application
//! failure. Dropped runs retain asynchronous cleanup through the container.
//!
//! `lily_injection` is the only DI dependency an application needs. It exposes
//! [`Injectable`], [`ServiceTrait`], [`InjectionError`] and the scope APIs;
//! generated code accesses its registration support through this runtime.
//! No direct dependency on the derive crate, registry or `linkme` is needed.
//! HTTP, WebSocket and consumer applications can instead use their framework's
//! root re-exports, for example `lily_http_api::{Injectable, ServiceTrait}`.
//! Cargo-renamed dependencies are supported in both forms. Existing direct
//! imports from `lily_injectable_derive` remain compatible.

extern crate self as lily_injection;

mod application_container;
mod application_scope_factory;
pub use application_scope_factory::{ApplicationScopeFactory, ServiceScope};
#[doc(hidden)]
pub mod __private {
    pub use crate::application_scope_factory::ServiceScopeSnapshot;
    pub use lily_injection_registry as registry;
    pub use lily_injection_registry::linkme;
    use std::any::TypeId;
    use std::sync::Arc;

    use crate::{ApplicationContainer, ApplicationContainerBuilder, Extensions, ServiceLifetime};

    pub use crate::application_container::{
        ApplicationContainerBuild, ApplicationContainerBuildOutcome,
    };
    pub use crate::private::{InitializationGuard, attach_framework_singleton};
    pub use crate::storage::FrameworkSingletonAttachment;

    /// Begin a caller-polled, cancellation-safe application-container build.
    ///
    /// Lily application adapters use this seam when a larger composition
    /// transaction must explicitly cancel DI startup and wait for its rollback.
    /// Ordinary applications should continue to call
    /// [`ApplicationContainerBuilder::build`].
    #[doc(hidden)]
    pub fn begin_application_container_build(
        builder: ApplicationContainerBuilder,
    ) -> ApplicationContainerBuild {
        ApplicationContainerBuild::new(builder)
    }

    /// Read and validate the process bootstrap rollback budget.
    ///
    /// Framework adapter qualification tests use this seam to advance virtual
    /// time by the same value a newly-created DI build will capture.
    #[doc(hidden)]
    pub fn configured_build_rollback_timeout()
    -> Result<std::time::Duration, lily_error::injection::InjectionError> {
        crate::application_container::configured_build_rollback_timeout()
    }

    /// True only after the retained container shutdown task has actually joined
    /// and its scope/resolution workers have stopped. This is termination proof,
    /// not a claim that every disposer completed successfully.
    #[doc(hidden)]
    pub fn container_shutdown_quiescent(container: &ApplicationContainer) -> bool {
        container.shutdown_quiescent()
    }

    /// Whether a retained container close receipt has already been installed.
    #[doc(hidden)]
    pub fn container_shutdown_started(container: &ApplicationContainer) -> bool {
        container.shutdown_started()
    }

    /// Observed container close result, without starting or retrying cleanup.
    /// `None` is not proof of termination; `Some(false)` is a joined failure.
    #[doc(hidden)]
    pub fn container_shutdown_succeeded(container: &ApplicationContainer) -> Option<bool> {
        container.shutdown_succeeded()
    }

    /// Opaque identity for one concrete application-scope generation.
    ///
    /// Framework adapters capture this before task cancellation so a cleanup
    /// observer can never attach to a later scope that reuses the same ID.
    #[doc(hidden)]
    #[derive(Clone)]
    pub struct ScopeCleanupObservation {
        pub(crate) manager: Arc<crate::storage::ScopeManager>,
        pub(crate) observation: crate::storage::ScopeCleanupObservation,
    }

    /// Capture the generation returned by scope creation itself, without a
    /// second lookup by its reusable process ID. This does not begin cleanup.
    #[doc(hidden)]
    pub fn application_scope_cleanup_observation(
        scope: &crate::ApplicationScope,
    ) -> ScopeCleanupObservation {
        scope.cleanup_observation()
    }

    /// Close with the original cleanup ticket's result. `Ok(false)` means no
    /// ticket was obtained (already closed/claimed by another DI owner), not
    /// successful disposal. Adapters retain a separate generation observation
    /// to prove actual termination in either case.
    #[doc(hidden)]
    pub async fn close_application_scope_before(
        scope: &mut crate::ApplicationScope,
        deadline: tokio::time::Instant,
    ) -> Result<bool, crate::InjectionError> {
        scope.close_before_observed(deadline).await
    }

    /// Capture one currently live or closing application-scope generation.
    #[doc(hidden)]
    pub fn observe_scope_cleanup(
        container: &ApplicationContainer,
        scope_id: &str,
    ) -> Option<ScopeCleanupObservation> {
        let manager = container.services().scope_manager_handle();
        let observation = manager.observe_scope_cleanup(scope_id)?;
        Some(ScopeCleanupObservation {
            manager,
            observation,
        })
    }

    /// Wait through an adapter-owned deadline for the captured generation.
    #[doc(hidden)]
    pub async fn wait_for_observed_scope_cleanup_before(
        observation: ScopeCleanupObservation,
        deadline: tokio::time::Instant,
    ) -> Result<(), crate::InjectionError> {
        observation
            .manager
            .wait_for_observed_scope_cleanup_before(observation.observation, deadline)
            .await
    }

    /// Observe termination of the captured generation without creating another
    /// timeout authority. This proves that disposal is no longer running;
    /// disposer failures remain in the container's canonical shutdown ledger.
    #[doc(hidden)]
    pub async fn wait_for_observed_scope_cleanup(observation: ScopeCleanupObservation) {
        observation
            .manager
            .wait_for_observed_scope_cleanup(observation.observation)
            .await;
    }

    /// Return the canonical lifetime for one registered concrete or interface
    /// service route without resolving or initializing that service.
    #[doc(hidden)]
    pub fn service_registration_lifetime(
        extensions: &Extensions,
        type_id: TypeId,
    ) -> Option<ServiceLifetime> {
        extensions.service_registration_lifetime(type_id)
    }

    /// Wait until one exact application scope is no longer live and none of
    /// its asynchronous cleanup work remains active.
    #[doc(hidden)]
    pub async fn wait_for_scope_cleanup(container: &ApplicationContainer, scope_id: &str) {
        container
            .services()
            .scope_manager()
            .wait_for_scope_cleanup(scope_id)
            .await;
    }

    /// Wait for one exact application scope through an adapter-owned absolute
    /// deadline, aborting and joining only that scope's disposer task when the
    /// deadline expires. The result reports ownership reconciliation; errors
    /// from normally completed disposers remain in the container's canonical
    /// aggregate shutdown ledger.
    #[doc(hidden)]
    pub async fn wait_for_scope_cleanup_before(
        container: &ApplicationContainer,
        scope_id: &str,
        deadline: tokio::time::Instant,
    ) -> Result<(), crate::InjectionError> {
        container
            .services()
            .scope_manager()
            .wait_for_scope_cleanup_before(scope_id, deadline)
            .await
    }
}
mod private;
/// Service lifecycle and lifetime contracts.
pub mod service;
mod storage;

pub use application_container::{
    ApplicationContainer, ApplicationContainerBuilder, ApplicationScope,
    BUILD_ROLLBACK_TIMEOUT_ENV, ContainerShutdownReport, DEFAULT_SHUTDOWN_TIMEOUT,
    MAX_BUILD_ROLLBACK_TIMEOUT_SECS,
}; 
pub use service::*;
pub use storage::Extensions;

/// Declare an injectable service and its constructor dependencies.
pub use lily_injectable_derive::Injectable;

/// Re-export used when implementing asynchronous [`ServiceTrait`] lifecycle
/// hooks without adding a separate `async-trait` import path.
pub use async_trait;
// Proc-macro generated code uses this path so downstream applications do not
// need an undocumented direct `futures` dependency for panic-safe lifecycle
// boundaries.
#[doc(hidden)]
pub use futures as __private_futures;
/// Typed DI construction, resolution, scope and shutdown errors.
pub use lily_error::injection::{
    InjectionError, ShutdownOutcome, ShutdownOutcomeStatus, ShutdownRemainingWork,
};

/// Request/job context used to own scoped services.
pub use lily_process::ProcessContext;
