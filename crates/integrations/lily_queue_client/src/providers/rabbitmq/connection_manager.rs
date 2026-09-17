// =============================================================================
// RabbitMQ Connection Manager - Connection Pooling & Lifecycle
// =============================================================================

use async_trait::async_trait;
use lapin::Connection;
use lily_error::application::{message_broker::RabbitMQError, MessageBrokerError};
use lily_trace::tracing;
use lily_trace::tracing::Instrument;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::{telemetry::RabbitMqClientMetrics, ConnectionManager, RabbitMqOptions};

/// RabbitMQ connection manager with automatic reconnection
///
/// Performance optimizations:
/// - Single connection reuse across all channels
/// - Lazy connection establishment
/// - Automatic reconnection on failure
/// - Thread-safe with Mutex (minimal contention)
pub struct RabbitMQConnectionManager {
    options: RabbitMqOptions,
    connections: Mutex<ConnectionOwnership<Arc<Connection>>>,
    next_connection: AtomicUsize,
    metrics: RabbitMqClientMetrics,
}

#[derive(Debug)]
struct ConnectionOwnership<T> {
    starting: Vec<T>,
    active: Vec<T>,
    retired: Vec<T>,
}

impl<T> Default for ConnectionOwnership<T> {
    fn default() -> Self {
        Self {
            starting: Vec::new(),
            active: Vec::new(),
            retired: Vec::new(),
        }
    }
}

impl<T> ConnectionOwnership<T> {
    fn commit_starting(&mut self) {
        let next = std::mem::take(&mut self.starting);
        let previous = std::mem::replace(&mut self.active, next);
        self.retired.extend(previous);
    }

    fn retained_count(&self) -> usize {
        self.starting.len() + self.active.len() + self.retired.len()
    }
}

impl RabbitMQConnectionManager {
    /// Creates a stopped connection pool from a validated RabbitMQ plan.
    pub fn new(options: RabbitMqOptions) -> Self {
        Self {
            options,
            connections: Mutex::new(ConnectionOwnership::default()),
            next_connection: AtomicUsize::new(0),
            metrics: RabbitMqClientMetrics::default(),
        }
    }

    /// Establishes the full configured pool before publishing readiness.
    pub async fn start(&self, ct: CancellationToken) -> Result<(), MessageBrokerError> {
        // Every connection remains in a manager-owned ledger across all await
        // points. If the caller drops this future, a later `close()` can still
        // perform and prove asynchronous cleanup of the partial pool.
        let mut connections = self.connections.lock().await;
        let close_timeout = self.options.connection_timeout();
        let residue_cleanup = close_startup_residue(&mut connections, close_timeout).await;
        self.metrics
            .active_connections(connections.retained_count());
        residue_cleanup?;
        connections.starting.reserve(self.options.pool_size());
        for _ in 0..self.options.pool_size() {
            match self.connect_with_retry(&ct).await {
                Ok(connection) => connections.starting.push(Arc::new(connection)),
                Err(error) => {
                    let cleanup =
                        close_retained_connections(&mut connections.starting, close_timeout).await;
                    self.metrics
                        .active_connections(connections.retained_count());
                    if let Err(cleanup_error) = cleanup {
                        tracing::warn!(%cleanup_error, "RabbitMQ startup rollback failed");
                    }
                    return Err(error);
                }
            }
        }

        // Ownership transfer contains no await: cancellation can observe
        // either the complete staged pool or the complete committed pool,
        // never an unowned local Vec.
        connections.commit_starting();
        let retirement = close_retained_connections(&mut connections.retired, close_timeout).await;
        self.metrics
            .active_connections(connections.retained_count());
        retirement?;
        Ok(())
    }

    async fn connect_with_retry(
        &self,
        ct: &CancellationToken,
    ) -> Result<Connection, MessageBrokerError> {
        let mut last_error = None;
        for attempt in 0..self.options.max_reconnect_attempts() {
            let span = tracing::info_span!(
                "messaging.connection.recovery",
                lily.delivery_attempt = u64::from(attempt) + 1,
                lily.outcome = tracing::field::Empty,
                lily.error_code = tracing::field::Empty,
                otel.status_code = tracing::field::Empty,
            );
            self.metrics.connection_attempt();
            match self.options.connect(ct).instrument(span.clone()).await {
                Ok(connection) => {
                    span.record("lily.outcome", "success");
                    self.metrics.connection_recovery_outcome("success");
                    return Ok(connection);
                }
                Err(error) => {
                    span.record("lily.outcome", "error");
                    span.record("lily.error_code", error.error_code());
                    span.record("otel.status_code", "ERROR");
                    self.metrics.connection_recovery_outcome("failure");
                    if is_permanent_connection_failure(&error) {
                        return Err(error);
                    }
                    last_error = Some(error);
                }
            }

            if attempt + 1 < self.options.max_reconnect_attempts() {
                tokio::select! {
                    biased;
                    _ = ct.cancelled() => {
                        return Err(MessageBrokerError::RabbitMQError(RabbitMQError::Cancelled));
                    }
                    _ = tokio::time::sleep(self.options.reconnect_backoff()) => {}
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            MessageBrokerError::RabbitMQError(RabbitMQError::General(
                "RabbitMQ connection failed without an error".into(),
            ))
        }))
    }

    /// Closes and removes every connection owned by the pool.
    pub async fn close(&self) -> Result<(), MessageBrokerError> {
        // The guard itself owns all ledgers across awaits. If this future is
        // cancelled, dropping the guard leaves every handle in manager state
        // for the next bounded close attempt.
        let mut connections = self.connections.lock().await;
        let close_timeout = self.options.connection_timeout();
        let mut first_error = None;

        if let Err(error) =
            close_retained_connections(&mut connections.starting, close_timeout).await
        {
            first_error = Some(error);
        }

        if let Err(error) =
            close_retained_connections(&mut connections.retired, close_timeout).await
        {
            if first_error.is_some() {
                tracing::warn!(
                    lily.error_code = error.error_code(),
                    "Additional RabbitMQ retired-pool close failure"
                );
            } else {
                first_error = Some(error);
            }
        }

        if let Err(error) = close_retained_connections(&mut connections.active, close_timeout).await
        {
            if first_error.is_some() {
                tracing::warn!(
                    lily.error_code = error.error_code(),
                    "Additional RabbitMQ active-pool close failure"
                );
            } else {
                first_error = Some(error);
            }
        }
        self.metrics
            .active_connections(connections.retained_count());

        first_error.map_or(Ok(()), Err)
    }
}

fn is_permanent_connection_failure(error: &MessageBrokerError) -> bool {
    matches!(
        error,
        MessageBrokerError::RabbitMQError(RabbitMQError::Configuration(_) | RabbitMQError::Tls(_))
    )
}

async fn close_retained_connections(
    connections: &mut Vec<Arc<Connection>>,
    timeout: Duration,
) -> Result<(), MessageBrokerError> {
    let result = close_connection_refs(connections, timeout).await;
    reconcile_connection_ledger(connections, |connection| {
        connection_lifecycle_state(connection).is_terminal()
    });
    result
}

fn reconcile_connection_ledger<T>(
    connections: &mut Vec<T>,
    mut is_terminal: impl FnMut(&T) -> bool,
) {
    connections.retain(|connection| !is_terminal(connection));
}

async fn close_startup_residue(
    connections: &mut ConnectionOwnership<Arc<Connection>>,
    timeout: Duration,
) -> Result<(), MessageBrokerError> {
    let mut first_error = None;
    if let Err(error) = close_retained_connections(&mut connections.starting, timeout).await {
        first_error = Some(error);
    }
    if let Err(error) = close_retained_connections(&mut connections.retired, timeout).await {
        if first_error.is_some() {
            tracing::warn!(
                lily.error_code = error.error_code(),
                "Additional RabbitMQ retired-pool cleanup failure"
            );
        } else {
            first_error = Some(error);
        }
    }
    first_error.map_or(Ok(()), Err)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ConnectionLifecycleState {
    Initial,
    Connecting,
    Connected,
    Closing,
    Closed,
    Reconnecting,
    Error,
}

impl ConnectionLifecycleState {
    const fn is_terminal(self) -> bool {
        matches!(self, Self::Closed | Self::Error)
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Initial => "initial",
            Self::Connecting => "connecting",
            Self::Connected => "connected",
            Self::Closing => "closing",
            Self::Closed => "closed",
            Self::Reconnecting => "reconnecting",
            Self::Error => "error",
        }
    }
}

fn connection_lifecycle_state(connection: &Connection) -> ConnectionLifecycleState {
    let status = connection.status();
    if status.connected() {
        ConnectionLifecycleState::Connected
    } else if status.connecting() {
        ConnectionLifecycleState::Connecting
    } else if status.closing() {
        ConnectionLifecycleState::Closing
    } else if status.closed() {
        ConnectionLifecycleState::Closed
    } else if status.reconnecting() {
        ConnectionLifecycleState::Reconnecting
    } else if status.errored() {
        ConnectionLifecycleState::Error
    } else {
        ConnectionLifecycleState::Initial
    }
}

async fn observe_closing_state(
    mut current_state: impl FnMut() -> ConnectionLifecycleState,
    poll_interval: Duration,
) -> ConnectionLifecycleState {
    loop {
        let state = current_state();
        if state != ConnectionLifecycleState::Closing {
            return state;
        }
        tokio::time::sleep(poll_interval).await;
    }
}

async fn close_connection_refs(
    connections: &[Arc<Connection>],
    timeout: Duration,
) -> Result<(), MessageBrokerError> {
    let close = async {
        let mut failures = Vec::new();
        for connection in connections {
            let mut state = connection_lifecycle_state(connection);
            if state == ConnectionLifecycleState::Connected {
                if let Err(error) = connection
                    .close(200, "Lily queue client shutdown".into())
                    .await
                {
                    failures.push(error.to_string());
                    continue;
                }
                state = connection_lifecycle_state(connection);
            }

            if state == ConnectionLifecycleState::Closing {
                state = observe_closing_state(
                    || connection_lifecycle_state(connection),
                    Duration::from_millis(10),
                )
                .await;
            }

            if !state.is_terminal() {
                failures.push(format!(
                    "connection remained in non-terminal {} state",
                    state.as_str()
                ));
            }
        }

        if failures.is_empty() {
            Ok(())
        } else {
            Err(MessageBrokerError::RabbitMQError(RabbitMQError::Lapin(
                format!(
                    "failed to close {} RabbitMQ connection(s): {}",
                    failures.len(),
                    failures.join("; ")
                ),
            )))
        }
    };

    tokio::time::timeout(timeout, close).await.map_err(|_| {
        MessageBrokerError::RabbitMQError(RabbitMQError::Timeout(
            "RabbitMQ connection pool close".into(),
        ))
    })?
}

fn select_connection_index<T>(
    connections: &[T],
    offset: usize,
    mut is_connected: impl FnMut(&T) -> bool,
) -> Option<usize> {
    let connected_count = connections
        .iter()
        .filter(|connection| is_connected(connection))
        .count();
    if connected_count == 0 {
        return None;
    }

    let selected = offset % connected_count;
    connections
        .iter()
        .enumerate()
        .filter(|(_, connection)| is_connected(connection))
        .nth(selected)
        .map(|(index, _)| index)
}

async fn await_usable_or_terminal_active_generation(
    connections: &mut Vec<Arc<Connection>>,
    timeout: Duration,
    cancellation: &CancellationToken,
) -> Result<(), MessageBrokerError> {
    let observation = async {
        loop {
            if cancellation.is_cancelled() {
                return Err(MessageBrokerError::RabbitMQError(RabbitMQError::Cancelled));
            }
            reconcile_connection_ledger(connections, |connection| {
                connection_lifecycle_state(connection).is_terminal()
            });
            if connections.is_empty()
                || connections
                    .iter()
                    .any(|connection| connection.status().connected())
            {
                return Ok(());
            }
            wait_for_connection_recovery_poll(cancellation, Duration::from_millis(10)).await?;
        }
    };

    tokio::time::timeout(timeout, observation)
        .await
        .map_err(|_| {
            MessageBrokerError::RabbitMQError(RabbitMQError::Timeout(
                "RabbitMQ connection recovery".into(),
            ))
        })?
}

async fn wait_for_connection_recovery_poll(
    cancellation: &CancellationToken,
    poll_interval: Duration,
) -> Result<(), MessageBrokerError> {
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => {
            Err(MessageBrokerError::RabbitMQError(RabbitMQError::Cancelled))
        }
        _ = tokio::time::sleep(poll_interval) => Ok(()),
    }
}

#[async_trait]
impl ConnectionManager<Connection> for RabbitMQConnectionManager {
    async fn get_connection(
        &self,
        ct: CancellationToken,
    ) -> Result<Arc<Connection>, MessageBrokerError> {
        let mut connections = self.connections.lock().await;
        reconcile_connection_ledger(&mut connections.active, |connection| {
            connection_lifecycle_state(connection).is_terminal()
        });
        if !connections.active.is_empty()
            && !connections
                .active
                .iter()
                .any(|connection| connection.status().connected())
        {
            let recovery = await_usable_or_terminal_active_generation(
                &mut connections.active,
                self.options.connection_timeout(),
                &ct,
            )
            .await;
            self.metrics
                .active_connections(connections.retained_count());
            recovery?;
        }
        if connections.active.is_empty() {
            self.metrics.connection_recovery();
            connections
                .active
                .push(Arc::new(self.connect_with_retry(&ct).await?));
        }

        self.metrics
            .active_connections(connections.retained_count());

        let index = select_connection_index(
            &connections.active,
            self.next_connection.fetch_add(1, Ordering::Relaxed),
            |connection| connection.status().connected(),
        )
        .ok_or_else(|| {
            MessageBrokerError::RabbitMQError(RabbitMQError::General(
                "RabbitMQ active connection generation has no usable connection".into(),
            ))
        })?;
        Ok(connections.active[index].clone())
    }

    fn is_connected(&self) -> bool {
        self.connections
            .try_lock()
            .map(|connections| {
                !connections.active.is_empty()
                    && connections
                        .active
                        .iter()
                        .all(|connection| connection.status().connected())
            })
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use lily_error::application::message_broker::RabbitMqTlsErrorKind;
    use tokio::sync::Mutex;
    use tokio_util::sync::CancellationToken;

    use super::*;

    #[test]
    fn local_configuration_and_tls_material_errors_are_not_retried() {
        for error in [
            MessageBrokerError::RabbitMQError(RabbitMQError::Configuration("invalid".into())),
            MessageBrokerError::RabbitMQError(RabbitMQError::Tls(
                RabbitMqTlsErrorKind::ClientPrivateKeyInvalid,
            )),
        ] {
            assert!(is_permanent_connection_failure(&error));
        }
        assert!(!is_permanent_connection_failure(
            &MessageBrokerError::RabbitMQError(RabbitMQError::Lapin("transient".into()))
        ));
    }

    #[test]
    fn startup_commit_atomically_moves_new_and_previous_generations() {
        let mut ownership = ConnectionOwnership {
            starting: vec![1_u8, 2],
            active: vec![3, 4],
            retired: Vec::new(),
        };

        ownership.commit_starting();

        assert!(ownership.starting.is_empty());
        assert_eq!(ownership.active, [1, 2]);
        assert_eq!(ownership.retired, [3, 4]);
        assert_eq!(ownership.retained_count(), 4);
    }

    #[tokio::test]
    async fn cancelled_start_future_leaves_staged_generation_manager_owned() {
        let ownership = Arc::new(Mutex::new(ConnectionOwnership::<u8>::default()));
        let entered = CancellationToken::new();
        let task_ownership = Arc::clone(&ownership);
        let task_entered = entered.clone();
        let task = tokio::spawn(async move {
            let mut ownership = task_ownership.lock().await;
            ownership.starting.push(7);
            task_entered.cancel();
            std::future::pending::<()>().await;
        });

        entered.cancelled().await;
        task.abort();
        assert!(task
            .await
            .expect_err("startup task must abort")
            .is_cancelled());

        let ownership = ownership.lock().await;
        assert_eq!(ownership.starting, [7]);
        assert!(ownership.active.is_empty());
        assert!(ownership.retired.is_empty());
    }

    #[test]
    fn close_evidence_releases_only_terminal_connections() {
        let mut states = vec![
            ConnectionLifecycleState::Closed,
            ConnectionLifecycleState::Error,
            ConnectionLifecycleState::Closing,
            ConnectionLifecycleState::Reconnecting,
            ConnectionLifecycleState::Connected,
        ];

        reconcile_connection_ledger(&mut states, |state| state.is_terminal());

        assert_eq!(
            states,
            [
                ConnectionLifecycleState::Closing,
                ConnectionLifecycleState::Reconnecting,
                ConnectionLifecycleState::Connected,
            ]
        );
    }

    #[tokio::test]
    async fn cancelled_closing_observation_retains_owner_until_terminal_evidence() {
        let state = Arc::new(std::sync::Mutex::new(ConnectionLifecycleState::Closing));
        let observed = CancellationToken::new();
        let observed_by_reader = observed.clone();
        let state_by_reader = Arc::clone(&state);
        let mut observation = Box::pin(observe_closing_state(
            move || {
                observed_by_reader.cancel();
                *state_by_reader
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
            },
            Duration::from_secs(60),
        ));

        tokio::select! {
            biased;
            outcome = &mut observation => panic!("closing unexpectedly became terminal: {outcome:?}"),
            () = observed.cancelled() => {}
        }
        drop(observation);

        let mut retained = vec![ConnectionLifecycleState::Closing];
        reconcile_connection_ledger(&mut retained, |state| state.is_terminal());
        assert_eq!(retained, [ConnectionLifecycleState::Closing]);

        *state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = ConnectionLifecycleState::Closed;
        assert_eq!(
            observe_closing_state(
                || {
                    *state
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                },
                Duration::from_millis(1),
            )
            .await,
            ConnectionLifecycleState::Closed
        );

        retained[0] = ConnectionLifecycleState::Closed;
        reconcile_connection_ledger(&mut retained, |state| state.is_terminal());
        assert!(retained.is_empty());
    }

    #[test]
    fn reconnecting_connection_is_non_terminal_and_remains_owned() {
        let mut retained = vec![ConnectionLifecycleState::Reconnecting];
        reconcile_connection_ledger(&mut retained, |state| state.is_terminal());
        assert_eq!(retained, [ConnectionLifecycleState::Reconnecting]);
    }

    #[test]
    fn connection_selection_keeps_non_terminal_owners_and_uses_only_connected_entries() {
        let mut active = vec![
            ConnectionLifecycleState::Closing,
            ConnectionLifecycleState::Closed,
            ConnectionLifecycleState::Reconnecting,
            ConnectionLifecycleState::Connected,
            ConnectionLifecycleState::Error,
        ];
        reconcile_connection_ledger(&mut active, |state| state.is_terminal());

        assert_eq!(
            active,
            [
                ConnectionLifecycleState::Closing,
                ConnectionLifecycleState::Reconnecting,
                ConnectionLifecycleState::Connected,
            ]
        );
        assert_eq!(
            select_connection_index(&active, 0, |state| {
                *state == ConnectionLifecycleState::Connected
            }),
            Some(2)
        );
        assert_eq!(
            select_connection_index(&active, 9, |state| {
                *state == ConnectionLifecycleState::Connected
            }),
            Some(2)
        );
    }

    #[tokio::test]
    async fn recovery_observation_honors_pre_and_midflight_cancellation() {
        let pre_cancelled = CancellationToken::new();
        pre_cancelled.cancel();
        let error = wait_for_connection_recovery_poll(&pre_cancelled, Duration::from_secs(60))
            .await
            .expect_err("pre-cancelled recovery observation must not sleep");
        assert!(matches!(
            error,
            MessageBrokerError::RabbitMQError(RabbitMQError::Cancelled)
        ));

        let midflight = CancellationToken::new();
        let waiter_token = midflight.clone();
        let waiter = tokio::spawn(async move {
            wait_for_connection_recovery_poll(&waiter_token, Duration::from_secs(60)).await
        });
        tokio::task::yield_now().await;
        midflight.cancel();
        let error = waiter
            .await
            .expect("recovery observer task must join")
            .expect_err("mid-flight cancellation must preempt the poll interval");
        assert!(matches!(
            error,
            MessageBrokerError::RabbitMQError(RabbitMQError::Cancelled)
        ));
    }
}
