//! Shared outbound admission contract, independent of backplane configuration.

use super::{
    BroadcastMessage, BroadcastTarget, ConnectionError, ConnectionManager, ConnectionOperationError,
};
use crate::request::{
    MAX_CANONICAL_NAMESPACE_BYTES, is_canonical_namespace, is_canonical_room,
    is_canonical_route_token,
};
use tokio_tungstenite::tungstenite::Message;

pub(crate) const MAX_BROADCAST_CONNECTION_TARGETS: usize = 256;
pub(crate) const MAX_BROADCAST_EXCLUSIONS: usize = 256;
pub(crate) const MAX_BROADCAST_ROOMS: usize = 256;

/// Only the shared validator can construct this command and its encoded frame.
#[derive(Debug)]
pub(crate) struct PreparedBroadcast {
    pub(super) command: BroadcastMessage,
    pub(super) frame: Message,
}

impl PreparedBroadcast {
    pub(crate) fn command(&self) -> &BroadcastMessage {
        &self.command
    }

    /// A dynamic scope with no local members may still have remote recipients.
    pub(crate) fn has_no_targets(&self) -> bool {
        match &self.command.target {
            BroadcastTarget::NamespaceConnections {
                connection_ids: ids,
                ..
            } => ids
                .iter()
                .all(|id| self.command.exclude.binary_search(id).is_ok()),
            BroadcastTarget::Rooms { rooms, .. } => rooms.is_empty(),
            _ => false,
        }
    }
}

pub(crate) fn validate_broadcast_namespace(
    namespace: &str,
) -> Result<(), ConnectionOperationError> {
    if is_canonical_namespace(namespace)
        && is_canonical_route_token(namespace, MAX_CANONICAL_NAMESPACE_BYTES)
    {
        Ok(())
    } else {
        Err(ConnectionOperationError::InvalidBackplaneTarget)
    }
}

impl ConnectionManager {
    pub(crate) fn prepare_broadcast(
        &self,
        mut command: BroadcastMessage,
    ) -> Result<PreparedBroadcast, ConnectionError> {
        // Validate/encode once before target lookup, queue admission or publish.
        let frame =
            self.encode_outbound_application_frame(&command.message, command.wire_format)?;
        validate_broadcast_namespace(command.target.namespace())?;
        let validate_room = |room: &str| {
            if is_canonical_room(room, self.max_room_name_length()) {
                Ok(())
            } else {
                Err(ConnectionOperationError::InvalidBackplaneTarget)
            }
        };
        match &mut command.target {
            BroadcastTarget::Namespace(_) => {}
            BroadcastTarget::Room { room, .. } => {
                validate_room(room)?;
            }
            BroadcastTarget::Rooms { rooms, .. } => {
                if rooms.len() > MAX_BROADCAST_ROOMS {
                    return Err(ConnectionOperationError::BackplaneTargetLimitExceeded.into());
                }
                rooms.sort_unstable();
                rooms.dedup();
                for room in rooms {
                    validate_room(room)?;
                }
            }
            BroadcastTarget::Principal { .. } => {
                // PrincipalId is validated at construction/deserialization.
            }
            BroadcastTarget::NamespaceConnections { connection_ids, .. } => {
                validate_connection_ids(connection_ids)?;
            }
        }
        if command.exclude.len() > MAX_BROADCAST_EXCLUSIONS {
            return Err(ConnectionOperationError::BackplaneExclusionLimitExceeded.into());
        }
        command.exclude.sort_unstable();
        command.exclude.dedup();
        Ok(PreparedBroadcast { command, frame })
    }
}

fn validate_connection_ids(ids: &mut Vec<uuid::Uuid>) -> Result<(), ConnectionOperationError> {
    if ids.len() > MAX_BROADCAST_CONNECTION_TARGETS {
        return Err(ConnectionOperationError::BackplaneTargetLimitExceeded);
    }
    ids.sort_unstable();
    ids.dedup();
    Ok(())
}
