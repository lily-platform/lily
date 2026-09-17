//! Error contracts for Lily application components.

/// DTO-facing CRUD service errors.
pub mod base_service;
/// Consumer lifecycle and handler errors.
pub mod consumer;
/// HTTP request, response, and public error contracts.
pub mod http_api;
/// RabbitMQ-backed message broker errors.
pub mod message_broker;
pub use message_broker::{MessageBrokerError, QueueHandlerError, QueueHandlerFailureClass};
/// MongoDB repository and migration errors.
pub mod mongodb;
/// Queue handler service errors.
pub mod service;

pub use base_service::{BaseServiceError, BaseServiceOperation};
