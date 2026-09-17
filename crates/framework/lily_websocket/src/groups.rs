use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::request::{MAX_CANONICAL_ROOM_BYTES, is_canonical_room};

#[derive(Debug, Clone)]
pub(crate) struct GroupManager {
    /// Namespaces (similar to Socket.IO namespaces)
    namespaces: Arc<RwLock<HashMap<String, Namespace>>>,
}

/// Namespace contains multiple rooms and manages connections within that namespace
#[derive(Debug, Clone)]
struct Namespace {
    /// Namespace name
    #[cfg(test)]
    name: String,
    /// Rooms within this namespace
    rooms: HashMap<String, Room>,
    /// All connections in this namespace (not in any specific room)
    connections: HashSet<Uuid>,
    /// Creation timestamp
    #[cfg(test)]
    created_at: std::time::SystemTime,
}

/// Room within a namespace (similar to Socket.IO rooms or SignalR groups)
#[derive(Debug, Clone)]
struct Room {
    /// Room name
    #[cfg(test)]
    name: String,
    /// Connections in this room
    connections: HashSet<Uuid>,
    /// Room creation timestamp
    #[cfg(test)]
    created_at: std::time::SystemTime,
}

impl Default for GroupManager {
    fn default() -> Self {
        Self::new()
    }
}

impl GroupManager {
    /// Create new group manager
    pub(crate) fn new() -> Self {
        Self {
            namespaces: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub(crate) fn with_namespaces(names: impl IntoIterator<Item = String>) -> Self {
        let mut namespaces = HashMap::new();
        for name in names {
            namespaces
                .entry(name.clone())
                .or_insert_with(|| Namespace::new(name));
        }
        Self {
            namespaces: Arc::new(RwLock::new(namespaces)),
        }
    }

    /// Add connection to namespace
    pub(crate) async fn add_connection_to_namespace(
        &self,
        namespace_name: &str,
        connection_id: Uuid,
    ) -> Result<(), GroupError> {
        let mut namespaces = self.namespaces.write().await;
        let namespace =
            namespaces
                .get_mut(namespace_name)
                .ok_or_else(|| GroupError::NamespaceNotFound {
                    namespace: namespace_name.to_string(),
                })?;

        namespace.connections.insert(connection_id);
        Ok(())
    }

    /// Remove connection from namespace
    pub(crate) async fn remove_connection_from_namespace(
        &self,
        namespace_name: &str,
        connection_id: Uuid,
    ) -> Result<(), GroupError> {
        let mut namespaces = self.namespaces.write().await;
        if let Some(namespace) = namespaces.get_mut(namespace_name) {
            namespace.connections.remove(&connection_id);

            // Remove connection from all rooms in this namespace
            for room in namespace.rooms.values_mut() {
                room.connections.remove(&connection_id);
            }
            namespace
                .rooms
                .retain(|_, room| !room.connections.is_empty());
        }
        Ok(())
    }

    /// Join a room while atomically enforcing per-connection and room-name
    /// limits under the namespace write lock.
    pub(crate) async fn join_room_bounded(
        &self,
        namespace_name: &str,
        room_name: &str,
        connection_id: Uuid,
        max_rooms_per_connection: usize,
        max_room_name_length: usize,
    ) -> Result<(), GroupError> {
        if !is_canonical_room(room_name, max_room_name_length) {
            return Err(GroupError::InvalidRoomName {
                maximum: max_room_name_length.min(MAX_CANONICAL_ROOM_BYTES),
            });
        }

        let mut namespaces = self.namespaces.write().await;
        let namespace =
            namespaces
                .get_mut(namespace_name)
                .ok_or_else(|| GroupError::NamespaceNotFound {
                    namespace: namespace_name.to_string(),
                })?;
        let already_joined = namespace
            .rooms
            .get(room_name)
            .is_some_and(|room| room.connections.contains(&connection_id));
        let joined_room_count = namespace
            .rooms
            .values()
            .filter(|room| room.connections.contains(&connection_id))
            .count();
        if !already_joined && joined_room_count >= max_rooms_per_connection {
            return Err(GroupError::ConnectionRoomLimit {
                connection_id,
                maximum: max_rooms_per_connection,
            });
        }

        let room = namespace
            .rooms
            .entry(room_name.to_string())
            .or_insert_with(|| Room::new(room_name.to_string()));
        room.connections.insert(connection_id);
        Ok(())
    }

    /// Leave room
    pub(crate) async fn leave_room(
        &self,
        namespace_name: &str,
        room_name: &str,
        connection_id: Uuid,
    ) -> Result<(), GroupError> {
        let mut namespaces = self.namespaces.write().await;
        if let Some(namespace) = namespaces.get_mut(namespace_name)
            && let Some(room) = namespace.rooms.get_mut(room_name)
        {
            room.connections.remove(&connection_id);

            if room.connections.is_empty() {
                namespace.rooms.remove(room_name);
            }
        }
        Ok(())
    }

    /// Get all connections in namespace
    pub(crate) async fn get_namespace_connections(&self, namespace_name: &str) -> Vec<Uuid> {
        let namespaces = self.namespaces.read().await;
        namespaces
            .get(namespace_name)
            .map(|ns| ns.connections.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Get all connections in room
    pub(crate) async fn get_room_connections(
        &self,
        namespace_name: &str,
        room_name: &str,
    ) -> Vec<Uuid> {
        self.find_room_connections(namespace_name, room_name)
            .await
            .unwrap_or_default()
    }

    /// Preserve room existence and membership from one index read.
    pub(crate) async fn find_room_connections(
        &self,
        namespace_name: &str,
        room_name: &str,
    ) -> Option<Vec<Uuid>> {
        let namespaces = self.namespaces.read().await;
        namespaces
            .get(namespace_name)
            .and_then(|ns| ns.rooms.get(room_name))
            .map(|room| room.connections.iter().cloned().collect())
    }

    /// Get room info
    #[cfg(test)]
    pub(crate) async fn get_room_info(
        &self,
        namespace_name: &str,
        room_name: &str,
    ) -> Option<RoomInfo> {
        let namespaces = self.namespaces.read().await;
        namespaces
            .get(namespace_name)
            .and_then(|ns| ns.rooms.get(room_name))
            .map(|room| RoomInfo {
                name: room.name.clone(),
                connection_count: room.connections.len(),
                created_at: room.created_at,
            })
    }

    /// Get namespace statistics
    #[cfg(test)]
    pub(crate) async fn get_namespace_stats(&self, namespace_name: &str) -> Option<NamespaceStats> {
        let namespaces = self.namespaces.read().await;
        namespaces.get(namespace_name).map(|ns| NamespaceStats {
            name: ns.name.clone(),
            total_connections: ns.connections.len(),
            total_rooms: ns.rooms.len(),
            room_stats: ns
                .rooms
                .iter()
                .map(|(name, room)| (name.clone(), room.connections.len()))
                .collect(),
            created_at: ns.created_at,
        })
    }
}

impl Namespace {
    fn new(name: String) -> Self {
        #[cfg(not(test))]
        let _ = name;
        Self {
            #[cfg(test)]
            name,
            rooms: HashMap::new(),
            connections: HashSet::new(),
            #[cfg(test)]
            created_at: std::time::SystemTime::now(),
        }
    }
}

impl Room {
    fn new(name: String) -> Self {
        #[cfg(not(test))]
        let _ = name;
        Self {
            #[cfg(test)]
            name,
            connections: HashSet::new(),
            #[cfg(test)]
            created_at: std::time::SystemTime::now(),
        }
    }
}

/// Room information for external queries
#[derive(Debug, Clone)]
#[cfg(test)]
#[allow(dead_code)]
pub(crate) struct RoomInfo {
    pub name: String,
    pub connection_count: usize,
    pub created_at: std::time::SystemTime,
}

/// Namespace statistics
#[derive(Debug, Clone)]
#[cfg(test)]
#[allow(dead_code)]
pub(crate) struct NamespaceStats {
    pub name: String,
    pub total_connections: usize,
    pub total_rooms: usize,
    pub room_stats: HashMap<String, usize>,
    pub created_at: std::time::SystemTime,
}

/// Typed namespace and room membership failures.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum GroupError {
    /// The requested namespace is not registered in this application.
    #[error("Namespace not found: {namespace}")]
    NamespaceNotFound {
        /// Missing namespace token.
        namespace: String,
    },
    /// A room token violates the canonical ASCII token or configured byte bound.
    #[error(
        "Invalid room name (must contain 1..={maximum} ASCII letters, digits, '.', '_' or '-' bytes)"
    )]
    InvalidRoomName {
        /// Maximum accepted canonical token byte length.
        maximum: usize,
    },
    /// A connection already belongs to its configured maximum number of rooms.
    #[error("Connection {connection_id} reached its room limit ({maximum})")]
    ConnectionRoomLimit {
        /// Connection whose membership was rejected.
        connection_id: Uuid,
        /// Maximum room memberships admitted for one connection.
        maximum: usize,
    },
}

#[cfg(test)]
mod local_delivery_qualification_tests {
    use super::*;

    #[tokio::test]
    async fn room_lookup_distinguishes_absent_from_retained_empty_entry() {
        let manager = GroupManager::with_namespaces(["orders".to_owned()]);
        assert_eq!(manager.find_room_connections("orders", "empty").await, None);
        // Deliberately construct the storage regression that a Vec-only query
        // cannot detect. Production joins never create a retained empty room.
        manager
            .namespaces
            .write()
            .await
            .get_mut("orders")
            .unwrap()
            .rooms
            .insert("empty".to_owned(), Room::new("empty".to_owned()));
        assert_eq!(
            manager.find_room_connections("orders", "empty").await,
            Some(vec![])
        );
        assert_eq!(
            manager.find_room_connections("billing", "empty").await,
            None
        );
        manager
            .leave_room("orders", "empty", Uuid::new_v4())
            .await
            .unwrap();
        assert_eq!(manager.find_room_connections("orders", "empty").await, None);
    }
}
