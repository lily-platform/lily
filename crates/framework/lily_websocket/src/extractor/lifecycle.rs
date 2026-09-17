use std::future::{Future, ready};
use std::sync::Arc;

use lily_injection::Extensions;
use lily_web_core::Principal;
use tokio_util::sync::CancellationToken;

use crate::controller::{WebSocketContext, WebSocketLifecycleError};

use super::{
    CleanupCancellation, ConnectionId, ConnectionLocal, ExecutionCancellation, MessageDeadline,
    Namespace, Service, WebSocketExtractionError,
};

/// Type-level selector for the connection-open lifecycle hook.
#[derive(Debug, Clone, Copy)]
#[doc(hidden)]
pub enum Connected {}

/// Type-level selector for the connection-close lifecycle hook.
#[derive(Debug, Clone, Copy)]
#[doc(hidden)]
pub enum Disconnected {}

/// Bounded reason supplied exactly once to a disconnected lifecycle hook.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum DisconnectReason {
    /// Peer completed a normal close or ended the transport.
    Peer,
    /// Application requested a normal close.
    Application,
    /// Peer violated the selected wire protocol.
    Protocol,
    /// Authentication, authorization or another policy closed the connection.
    Policy,
    /// The application-provided connection identity deadline elapsed.
    IdentityExpired,
    /// Outbound application admission capacity did not recover before its deadline.
    SlowConsumer,
    /// Connection reached its application-idle deadline.
    IdleTimeout,
    /// Application shutdown cancelled the connection.
    ServerShutdown,
    /// Runtime could not safely continue processing.
    Internal,
}

/// Complete owned state supplied to one lifecycle-hook adapter.
#[doc(hidden)]
pub struct WebSocketLifecycleInvocation {
    extensions: Arc<Extensions>,
    context: Arc<WebSocketContext>,
    principal: Option<Principal>,
    cancellation: LifecycleCancellation,
    deadline: tokio::time::Instant,
    disconnect_reason: Option<DisconnectReason>,
}

enum LifecycleCancellation {
    Execution(ExecutionCancellation),
    Cleanup(CleanupCancellation),
}

impl WebSocketLifecycleInvocation {
    /// Creates a connection-open invocation.
    pub(crate) fn connected(
        extensions: Arc<Extensions>,
        context: Arc<WebSocketContext>,
        cancellation: CancellationToken,
        deadline: tokio::time::Instant,
    ) -> Self {
        let principal = context.principal();
        let budget = context.shutdown_budget().clone();
        Self {
            extensions,
            context,
            principal,
            cancellation: LifecycleCancellation::Execution(ExecutionCancellation::with_budget(
                cancellation,
                budget,
            )),
            deadline,
            disconnect_reason: None,
        }
    }

    /// Creates a connection-close invocation with one bounded reason.
    pub(crate) fn disconnected(
        extensions: Arc<Extensions>,
        context: Arc<WebSocketContext>,
        cancellation: CancellationToken,
        deadline: tokio::time::Instant,
        reason: DisconnectReason,
    ) -> Self {
        let principal = context.principal();
        Self {
            extensions,
            context,
            principal,
            cancellation: LifecycleCancellation::Cleanup(CleanupCancellation::new(cancellation)),
            deadline,
            disconnect_reason: Some(reason),
        }
    }

    /// Read-only application DI provider.
    #[must_use]
    pub fn extensions(&self) -> Arc<Extensions> {
        Arc::clone(&self.extensions)
    }

    /// Shared connection context.
    #[must_use]
    pub fn context(&self) -> &Arc<WebSocketContext> {
        &self.context
    }

    /// Application-owned principal frozen for this lifecycle invocation.
    #[must_use]
    pub fn principal(&self) -> Option<&Principal> {
        self.principal.as_ref()
    }

    /// Read-only execution signal, present only for a connected invocation.
    #[must_use]
    pub const fn execution_cancellation(&self) -> Option<&ExecutionCancellation> {
        match &self.cancellation {
            LifecycleCancellation::Execution(signal) => Some(signal),
            LifecycleCancellation::Cleanup(_) => None,
        }
    }

    /// Read-only cleanup signal, present only for a disconnected invocation.
    #[must_use]
    pub const fn cleanup_cancellation(&self) -> Option<&CleanupCancellation> {
        match &self.cancellation {
            LifecycleCancellation::Cleanup(signal) => Some(signal),
            LifecycleCancellation::Execution(_) => None,
        }
    }

    /// Absolute lifecycle deadline.
    #[must_use]
    pub const fn deadline(&self) -> tokio::time::Instant {
        self.deadline
    }

    /// Disconnect reason, present only for a close hook.
    #[must_use]
    pub const fn disconnect_reason(&self) -> Option<DisconnectReason> {
        self.disconnect_reason
    }
}

/// Extracts one body-free argument for a typed lifecycle phase.
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be used as a WebSocket lifecycle extractor",
    note = "lifecycle operations accept only lifecycle-part extractors and do not receive message payloads"
)]
pub trait FromWebSocketLifecycleParts<Phase>: Sized {
    /// Typed extraction failure converted at the lifecycle boundary.
    type Rejection: Into<WebSocketLifecycleError> + Send;

    /// Produces one owned lifecycle argument.
    fn from_lifecycle_parts(
        invocation: &mut WebSocketLifecycleInvocation,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send;
}

/// Optional lifecycle extraction preserving malformed state as an error.
pub trait OptionalFromWebSocketLifecycleParts<Phase>: Sized {
    /// Typed extraction failure converted at the lifecycle boundary.
    type Rejection: Into<WebSocketLifecycleError> + Send;

    /// `Ok(None)` represents semantic absence only.
    fn from_lifecycle_parts(
        invocation: &mut WebSocketLifecycleInvocation,
    ) -> impl Future<Output = Result<Option<Self>, Self::Rejection>> + Send;
}

impl<E, Phase> FromWebSocketLifecycleParts<Phase> for Option<E>
where
    E: OptionalFromWebSocketLifecycleParts<Phase>,
{
    type Rejection = E::Rejection;

    fn from_lifecycle_parts(
        invocation: &mut WebSocketLifecycleInvocation,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        E::from_lifecycle_parts(invocation)
    }
}

impl From<WebSocketExtractionError> for WebSocketLifecycleError {
    fn from(_error: WebSocketExtractionError) -> Self {
        Self::rejected("WS_LIFECYCLE_EXTRACTION_FAILED")
    }
}

impl<Phase> FromWebSocketLifecycleParts<Phase> for Arc<WebSocketContext> {
    type Rejection = WebSocketExtractionError;

    fn from_lifecycle_parts(
        invocation: &mut WebSocketLifecycleInvocation,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(Ok(Arc::clone(invocation.context())))
    }
}

impl<Phase> FromWebSocketLifecycleParts<Phase> for WebSocketContext {
    type Rejection = WebSocketExtractionError;

    fn from_lifecycle_parts(
        invocation: &mut WebSocketLifecycleInvocation,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(Ok(invocation.context().as_ref().clone()))
    }
}

impl<Phase> FromWebSocketLifecycleParts<Phase> for ConnectionId {
    type Rejection = WebSocketExtractionError;

    fn from_lifecycle_parts(
        invocation: &mut WebSocketLifecycleInvocation,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(Ok(Self(invocation.context().connection_id())))
    }
}

impl<Phase> FromWebSocketLifecycleParts<Phase> for Namespace {
    type Rejection = WebSocketExtractionError;

    fn from_lifecycle_parts(
        invocation: &mut WebSocketLifecycleInvocation,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(Ok(Self(invocation.context().namespace().to_owned())))
    }
}

impl<Phase> FromWebSocketLifecycleParts<Phase> for Principal {
    type Rejection = WebSocketExtractionError;

    fn from_lifecycle_parts(
        invocation: &mut WebSocketLifecycleInvocation,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(
            invocation
                .principal()
                .cloned()
                .ok_or_else(WebSocketExtractionError::missing_principal),
        )
    }
}

impl<Phase> OptionalFromWebSocketLifecycleParts<Phase> for Principal {
    type Rejection = WebSocketExtractionError;

    fn from_lifecycle_parts(
        invocation: &mut WebSocketLifecycleInvocation,
    ) -> impl Future<Output = Result<Option<Self>, Self::Rejection>> + Send {
        ready(Ok(invocation.principal().cloned()))
    }
}

impl<T, Phase> FromWebSocketLifecycleParts<Phase> for ConnectionLocal<T>
where
    T: Clone + Send + Sync + 'static,
{
    type Rejection = WebSocketExtractionError;

    fn from_lifecycle_parts(
        invocation: &mut WebSocketLifecycleInvocation,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(
            invocation
                .context()
                .connection_locals()
                .get::<T>()
                .cloned()
                .map(Self)
                .ok_or_else(WebSocketExtractionError::missing_connection_local),
        )
    }
}

impl<T, Phase> OptionalFromWebSocketLifecycleParts<Phase> for ConnectionLocal<T>
where
    T: Clone + Send + Sync + 'static,
{
    type Rejection = WebSocketExtractionError;

    fn from_lifecycle_parts(
        invocation: &mut WebSocketLifecycleInvocation,
    ) -> impl Future<Output = Result<Option<Self>, Self::Rejection>> + Send {
        ready(Ok(invocation
            .context()
            .connection_locals()
            .get::<T>()
            .cloned()
            .map(Self)))
    }
}

impl FromWebSocketLifecycleParts<Connected> for ExecutionCancellation {
    type Rejection = WebSocketLifecycleError;

    fn from_lifecycle_parts(
        invocation: &mut WebSocketLifecycleInvocation,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(invocation.execution_cancellation().cloned().ok_or_else(|| {
            WebSocketLifecycleError::internal("execution signal requires a connected invocation")
        }))
    }
}

impl FromWebSocketLifecycleParts<Disconnected> for CleanupCancellation {
    type Rejection = WebSocketLifecycleError;

    fn from_lifecycle_parts(
        invocation: &mut WebSocketLifecycleInvocation,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(invocation.cleanup_cancellation().cloned().ok_or_else(|| {
            WebSocketLifecycleError::internal("cleanup signal requires a disconnected invocation")
        }))
    }
}

impl<Phase> FromWebSocketLifecycleParts<Phase> for MessageDeadline {
    type Rejection = WebSocketExtractionError;

    fn from_lifecycle_parts(
        invocation: &mut WebSocketLifecycleInvocation,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(Ok(Self(invocation.deadline())))
    }
}

impl<T, Phase> FromWebSocketLifecycleParts<Phase> for Service<T>
where
    T: ?Sized + Send + Sync + 'static,
{
    type Rejection = WebSocketLifecycleError;

    #[allow(clippy::manual_async_fn)]
    fn from_lifecycle_parts(
        invocation: &mut WebSocketLifecycleInvocation,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        let extensions = invocation.extensions();
        async move {
            extensions
                .get_service::<T>(None)
                .await
                .map(Self)
                .map_err(WebSocketLifecycleError::internal)
        }
    }
}

impl FromWebSocketLifecycleParts<Disconnected> for DisconnectReason {
    type Rejection = WebSocketLifecycleError;

    fn from_lifecycle_parts(
        invocation: &mut WebSocketLifecycleInvocation,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(
            invocation
                .disconnect_reason()
                .ok_or_else(|| WebSocketLifecycleError::internal("missing disconnect reason")),
        )
    }
}

/// Hidden tuple extraction ABI for connected/disconnected adapters.
#[doc(hidden)]
pub trait ExtractWebSocketLifecycleArguments<Phase>: Sized {
    /// Extracts all lifecycle arguments in declaration order.
    fn extract(
        invocation: &mut WebSocketLifecycleInvocation,
    ) -> impl Future<Output = Result<Self, WebSocketLifecycleError>> + Send;
}

impl<Phase> ExtractWebSocketLifecycleArguments<Phase> for () {
    async fn extract(
        _invocation: &mut WebSocketLifecycleInvocation,
    ) -> Result<Self, WebSocketLifecycleError> {
        Ok(())
    }
}

macro_rules! impl_lifecycle_tuple {
    ($($type:ident),+ $(,)?) => {
        impl<Phase, $($type),+> ExtractWebSocketLifecycleArguments<Phase> for ($($type,)+)
        where
            $($type: FromWebSocketLifecycleParts<Phase> + Send,)+
        {
            #[allow(non_snake_case)]
            async fn extract(
                invocation: &mut WebSocketLifecycleInvocation,
            ) -> Result<Self, WebSocketLifecycleError> {
                $(let $type = $type::from_lifecycle_parts(invocation).await.map_err(Into::into)?;)+
                Ok(($($type,)+))
            }
        }
    };
}

impl_lifecycle_tuple!(A);
impl_lifecycle_tuple!(A, B);
impl_lifecycle_tuple!(A, B, C);
impl_lifecycle_tuple!(A, B, C, D);
impl_lifecycle_tuple!(A, B, C, D, E);
impl_lifecycle_tuple!(A, B, C, D, E, F);
impl_lifecycle_tuple!(A, B, C, D, E, F, G);
impl_lifecycle_tuple!(A, B, C, D, E, F, G, H);
impl_lifecycle_tuple!(A, B, C, D, E, F, G, H, I);
impl_lifecycle_tuple!(A, B, C, D, E, F, G, H, I, J);
impl_lifecycle_tuple!(A, B, C, D, E, F, G, H, I, J, K);
impl_lifecycle_tuple!(A, B, C, D, E, F, G, H, I, J, K, L);
impl_lifecycle_tuple!(A, B, C, D, E, F, G, H, I, J, K, L, M);
impl_lifecycle_tuple!(A, B, C, D, E, F, G, H, I, J, K, L, M, N);
impl_lifecycle_tuple!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O);
impl_lifecycle_tuple!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P);

/// Extracts a generated lifecycle hook's complete typed tuple.
#[doc(hidden)]
pub fn extract_lifecycle_arguments<'a, Arguments, Phase>(
    invocation: &'a mut WebSocketLifecycleInvocation,
) -> impl Future<Output = Result<Arguments, WebSocketLifecycleError>> + Send + 'a
where
    Arguments: ExtractWebSocketLifecycleArguments<Phase> + 'a,
    Phase: 'a,
{
    Arguments::extract(invocation)
}

/// Extracts a generated connection-open hook's complete typed tuple.
///
/// A disconnect reason has no connection-open extraction contract:
///
/// ```compile_fail
/// use lily_websocket::{
///     DisconnectReason, WebSocketLifecycleInvocation, extract_connected_arguments,
/// };
///
/// fn invalid(invocation: &mut WebSocketLifecycleInvocation) {
///     let _ = extract_connected_arguments::<(DisconnectReason,)>(invocation);
/// }
/// ```
#[doc(hidden)]
pub fn extract_connected_arguments<'a, Arguments>(
    invocation: &'a mut WebSocketLifecycleInvocation,
) -> impl Future<Output = Result<Arguments, WebSocketLifecycleError>> + Send + 'a
where
    Arguments: ExtractWebSocketLifecycleArguments<Connected> + 'a,
{
    Arguments::extract(invocation)
}

/// Extracts a generated connection-close hook's complete typed tuple.
#[doc(hidden)]
pub fn extract_disconnected_arguments<'a, Arguments>(
    invocation: &'a mut WebSocketLifecycleInvocation,
) -> impl Future<Output = Result<Arguments, WebSocketLifecycleError>> + Send + 'a
where
    Arguments: ExtractWebSocketLifecycleArguments<Disconnected> + 'a,
{
    Arguments::extract(invocation)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::ConnectionManager;
    use lily_injection::ApplicationContainer;
    use uuid::Uuid;

    fn assert_disconnected_extractor<T>()
    where
        T: FromWebSocketLifecycleParts<Disconnected>,
    {
    }

    #[test]
    fn disconnect_reason_has_only_disconnected_extraction_contract() {
        assert_disconnected_extractor::<DisconnectReason>();
    }

    #[tokio::test]
    async fn lifecycle_extractors_preserve_the_phase_and_cancellation_authority() {
        let container = ApplicationContainer::build().await.unwrap();
        let context = Arc::new(WebSocketContext::new(
            Uuid::new_v4(),
            Arc::new(ConnectionManager::new()),
            "test".to_owned(),
        ));
        let execution_source = CancellationToken::new();
        let cleanup_source = CancellationToken::new();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
        let mut connected = WebSocketLifecycleInvocation::connected(
            container.services(),
            Arc::clone(&context),
            execution_source.clone(),
            deadline,
        );
        let mut disconnected = WebSocketLifecycleInvocation::disconnected(
            container.services(),
            context,
            cleanup_source.clone(),
            deadline,
            DisconnectReason::Peer,
        );
        assert!(connected.cleanup_cancellation().is_none());
        assert!(disconnected.execution_cancellation().is_none());
        let (execution,) = extract_connected_arguments::<(ExecutionCancellation,)>(&mut connected)
            .await
            .unwrap();
        let (cleanup,) =
            extract_disconnected_arguments::<(CleanupCancellation,)>(&mut disconnected)
                .await
                .unwrap();
        execution_source.cancel();
        execution.cancelled().await;
        assert!(connected.execution_cancellation().unwrap().is_cancelled());
        assert!(!cleanup.is_cancelled());
        assert!(!disconnected.cleanup_cancellation().unwrap().is_cancelled());
        cleanup_source.cancel();
        cleanup.cancelled().await;
        assert!(disconnected.cleanup_cancellation().unwrap().is_cancelled());
        // An incorrectly routed internal invocation must also fail closed.
        assert!(
            extract_connected_arguments::<(ExecutionCancellation,)>(&mut disconnected)
                .await
                .is_err()
        );
        assert!(
            extract_disconnected_arguments::<(CleanupCancellation,)>(&mut connected)
                .await
                .is_err()
        );
        container.close().await.unwrap();
    }

    #[tokio::test]
    async fn slow_consumer_reason_is_preserved_by_disconnected_extraction() {
        let container = ApplicationContainer::build()
            .await
            .expect("build lifecycle extractor test container");
        let context = Arc::new(WebSocketContext::new(
            Uuid::new_v4(),
            Arc::new(ConnectionManager::new()),
            "test".to_owned(),
        ));
        let mut invocation = WebSocketLifecycleInvocation::disconnected(
            container.services(),
            context,
            CancellationToken::new(),
            tokio::time::Instant::now() + std::time::Duration::from_secs(1),
            DisconnectReason::SlowConsumer,
        );

        let reason = DisconnectReason::from_lifecycle_parts(&mut invocation)
            .await
            .expect("disconnected invocation contains its close reason");
        assert_eq!(reason, DisconnectReason::SlowConsumer);

        container
            .close()
            .await
            .expect("close lifecycle extractor test container");
    }
}
