//! Production foundation for Lily-owned AsyncAPI 3.1 documents.
//!
//! This crate intentionally models only the bounded AsyncAPI subset emitted by
//! Lily's accepted WebSocket and RabbitMQ runtime plans. It is not a generic
//! user-authored document builder. Transport crates expose opt-in integration;
//! application code reads the resulting immutable [`AsyncApiService`] snapshot
//! and decides how or whether to serve or export it.
//!
//! Typed payload DTOs use the exact re-exported [`schemars`] version so a
//! transport facade can offer one dependency authority to downstream users.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod build;
mod config;
mod contribution;
mod error;
mod extensions;
mod limits;
mod model;
mod schema;
mod service;
mod validation;

pub use config::{
    AsyncApiApiKeyLocation, AsyncApiConfig, AsyncApiHttpApiKeyLocation, AsyncApiOAuthFlow,
    AsyncApiOAuthFlows, AsyncApiSecurityScheme, AsyncApiServer, AsyncApiServerProtocol,
    AsyncApiTag,
};
pub use error::AsyncApiBuildError;
pub use model::AsyncApiDocument;
pub use service::{AsyncApiService, AsyncApiServiceError, AsyncApiSnapshot};

/// The exact schema implementation used by Lily's AsyncAPI adapters.
pub use schemars;

/// Cross-crate ABI used only by Lily transport composition roots and derives.
///
/// This module is public because proc-macro output and sibling crates need
/// stable paths. It is deliberately hidden from normal documentation and does
/// not constitute a general-purpose document mutation API.
#[doc(hidden)]
pub mod __private {
    pub use crate::build::{build_document, prepare_document};
    pub use crate::contribution::{
        Action, AmqpExchangeBinding, AmqpExchangeKind, AmqpQueueBinding, ChannelBinding,
        ChannelDescriptor, CorrelationLocation, Documentation, MessageDescriptor, MessageHeaders,
        OperationDescriptor, PayloadSchema, RabbitMqEnvelopeHeaders, ReplyDescriptor,
        TransportContribution, TransportKind,
    };
    pub use crate::extensions::{
        CustomWebSocketErrorRepresentation, CustomWebSocketProtocolExtension,
        DeliveryGuaranteeExtension, LilyExtension, RabbitMqDeadLetterDescriptor,
        RabbitMqExchangeKind, RabbitMqMainTopologyDescriptor, RabbitMqQueueType,
        RabbitMqRetryAttemptDescriptor, RabbitMqRetryBucketDescriptor, RabbitMqTopologyExtension,
        RabbitMqTopologyOwnership, SettlementExtension, TransactionalInboxBackend,
        WebSocketCloseDescriptor, WebSocketContentKind, WebSocketEmitDescriptor,
        WebSocketEmitTarget, WebSocketErrorDescriptor, WebSocketOutcomeExtension,
        WebSocketOutcomeKind, WebSocketProtocolExtension, WebSocketWireFormat,
    };
    pub use crate::schema::{SchemaDirection, SchemaFactory};
    pub use crate::service::PreparedAsyncApi;

    use crate::{AsyncApiService, AsyncApiServiceError};

    /// Creates an unattached service for a Lily composition root.
    pub fn new_service<K>() -> AsyncApiService<K> {
        AsyncApiService::new()
    }

    /// Atomically attaches a fully prepared document to its marker-bound service.
    pub fn attach_document<K>(
        service: &AsyncApiService<K>,
        prepared: PreparedAsyncApi<K>,
    ) -> Result<(), AsyncApiServiceError> {
        service.attach(prepared)
    }

    /// Selects the exact advertised AMQP/AMQPS server keys for a Consumer
    /// transport contribution.
    pub fn amqp_server_names(
        config: &crate::AsyncApiConfig,
    ) -> Result<Vec<String>, crate::AsyncApiBuildError> {
        config.amqp_server_names()
    }
}
