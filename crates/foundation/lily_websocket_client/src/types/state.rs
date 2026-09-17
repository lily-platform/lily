// =============================================================================
// WebSocket Connection State Types
// =============================================================================

/// WebSocket connection state
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionState {
    /// Not connected
    Disconnected,

    /// Attempting to connect
    Connecting,

    /// Successfully connected
    Connected,

    /// Attempting to reconnect after connection loss
    Reconnecting {
        /// Current reconnection attempt number
        attempt: usize,
    },

    /// Connection failed and will not retry
    Failed,
}

impl ConnectionState {
    /// Check if currently connected
    pub fn is_connected(&self) -> bool {
        matches!(self, ConnectionState::Connected)
    }

    /// Check if in a transitional state
    pub fn is_transitioning(&self) -> bool {
        matches!(
            self,
            ConnectionState::Connecting | ConnectionState::Reconnecting { .. }
        )
    }

    /// Check if in a failed state
    pub fn is_failed(&self) -> bool {
        matches!(self, ConnectionState::Failed)
    }
}

impl std::fmt::Display for ConnectionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionState::Disconnected => write!(f, "Disconnected"),
            ConnectionState::Connecting => write!(f, "Connecting"),
            ConnectionState::Connected => write!(f, "Connected"),
            ConnectionState::Reconnecting { attempt } => {
                write!(f, "Reconnecting (attempt {})", attempt)
            }
            ConnectionState::Failed => write!(f, "Failed"),
        }
    }
}
