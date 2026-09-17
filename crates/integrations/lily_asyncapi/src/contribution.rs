use serde_json::Value;

use crate::{extensions::LilyExtension, schema::SchemaFactory};

/// The transport protocol represented by one accepted contribution.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TransportKind {
    WebSocket,
    Amqp,
}

impl TransportKind {
    pub(crate) const fn prefix(self) -> &'static str {
        match self {
            Self::WebSocket => "ws",
            Self::Amqp => "amqp",
        }
    }
}

/// Fail-closed documentation state baked into accepted runtime metadata.
#[doc(hidden)]
#[derive(Debug)]
pub enum Documentation<T> {
    Documented(T),
    Skipped { identity: String },
    Unspecified { identity: String },
}

/// A data-only contribution from one already accepted runtime plan.
#[doc(hidden)]
#[derive(Debug)]
pub struct TransportContribution {
    pub(crate) transport: TransportKind,
    pub(crate) channels: Vec<Documentation<ChannelDescriptor>>,
    pub(crate) messages: Vec<Documentation<MessageDescriptor>>,
    pub(crate) operations: Vec<Documentation<OperationDescriptor>>,
}

impl TransportContribution {
    pub fn new(transport: TransportKind) -> Self {
        Self {
            transport,
            channels: Vec::new(),
            messages: Vec::new(),
            operations: Vec::new(),
        }
    }

    pub fn channel(mut self, channel: Documentation<ChannelDescriptor>) -> Self {
        self.channels.push(channel);
        self
    }

    pub fn message(mut self, message: Documentation<MessageDescriptor>) -> Self {
        self.messages.push(message);
        self
    }

    pub fn operation(mut self, operation: Documentation<OperationDescriptor>) -> Self {
        self.operations.push(operation);
        self
    }
}

/// A physical WebSocket endpoint or AMQP queue/routing-key channel.
#[doc(hidden)]
#[derive(Debug)]
pub struct ChannelDescriptor {
    pub(crate) identity: String,
    pub(crate) explicit_id: Option<String>,
    pub(crate) address: String,
    pub(crate) title: Option<String>,
    pub(crate) summary: Option<String>,
    pub(crate) description: Option<String>,
    pub(crate) servers: Vec<String>,
    pub(crate) messages: Vec<String>,
    pub(crate) binding: Option<ChannelBinding>,
    pub(crate) extensions: Vec<LilyExtension>,
}

impl ChannelDescriptor {
    pub fn new(identity: impl Into<String>, address: impl Into<String>) -> Self {
        Self {
            identity: identity.into(),
            explicit_id: None,
            address: address.into(),
            title: None,
            summary: None,
            description: None,
            servers: Vec::new(),
            messages: Vec::new(),
            binding: None,
            extensions: Vec::new(),
        }
    }

    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.explicit_id = Some(id.into());
        self
    }

    pub fn title(mut self, title: impl Into<String>) -> Self {
        self.title = Some(title.into());
        self
    }

    pub fn summary(mut self, summary: impl Into<String>) -> Self {
        self.summary = Some(summary.into());
        self
    }

    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn server(mut self, server: impl Into<String>) -> Self {
        self.servers.push(server.into());
        self
    }

    pub fn message(mut self, runtime_identity: impl Into<String>) -> Self {
        self.messages.push(runtime_identity.into());
        self
    }

    pub fn binding(mut self, binding: ChannelBinding) -> Self {
        self.binding = Some(binding);
        self
    }

    pub fn extension(mut self, extension: LilyExtension) -> Self {
        self.extensions.push(extension);
        self
    }
}

/// An exact reusable wire message accepted by a channel.
#[doc(hidden)]
#[derive(Debug)]
pub struct MessageDescriptor {
    pub(crate) identity: String,
    pub(crate) explicit_id: Option<String>,
    pub(crate) name: Option<String>,
    pub(crate) title: Option<String>,
    pub(crate) summary: Option<String>,
    pub(crate) description: Option<String>,
    pub(crate) content_type: String,
    pub(crate) payload: PayloadSchema,
    pub(crate) headers: Option<MessageHeaders>,
    pub(crate) correlation: Option<CorrelationLocation>,
    pub(crate) tags: Vec<String>,
    pub(crate) examples: Vec<Value>,
    pub(crate) deprecated: bool,
    pub(crate) extensions: Vec<LilyExtension>,
}

impl MessageDescriptor {
    pub fn new(
        identity: impl Into<String>,
        content_type: impl Into<String>,
        payload: PayloadSchema,
    ) -> Self {
        Self {
            identity: identity.into(),
            explicit_id: None,
            name: None,
            title: None,
            summary: None,
            description: None,
            content_type: content_type.into(),
            payload,
            headers: None,
            correlation: None,
            tags: Vec::new(),
            examples: Vec::new(),
            deprecated: false,
            extensions: Vec::new(),
        }
    }

    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.explicit_id = Some(id.into());
        self
    }

    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn title(mut self, title: impl Into<String>) -> Self {
        self.title = Some(title.into());
        self
    }

    pub fn summary(mut self, summary: impl Into<String>) -> Self {
        self.summary = Some(summary.into());
        self
    }

    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn headers(mut self, schema: SchemaFactory) -> Self {
        self.headers = Some(MessageHeaders::Generated(schema));
        self
    }

    /// Binds the exact framework-owned RabbitMQ envelope headers accepted by
    /// one immutable consumer dispatch key.
    pub fn rabbitmq_headers(
        mut self,
        schema_version: u16,
        content_kind: impl Into<String>,
    ) -> Self {
        self.headers = Some(MessageHeaders::RabbitMqEnvelope(
            RabbitMqEnvelopeHeaders::new(schema_version, content_kind),
        ));
        self
    }

    pub fn correlation(mut self, location: CorrelationLocation) -> Self {
        self.correlation = Some(location);
        self
    }

    pub fn tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    pub fn example(mut self, example: Value) -> Self {
        self.examples.push(example);
        self
    }

    /// Parses one static JSON example retained by transport macro metadata.
    pub fn example_json(mut self, example: &str) -> Result<Self, crate::AsyncApiBuildError> {
        let value = serde_json::from_str(example).map_err(|error| {
            crate::AsyncApiBuildError::Validation {
                field: "message.examples".to_owned(),
                detail: format!("example is not valid JSON: {error}"),
            }
        })?;
        self.examples.push(value);
        Ok(self)
    }

    pub fn deprecated(mut self, deprecated: bool) -> Self {
        self.deprecated = deprecated;
        self
    }

    pub fn extension(mut self, extension: LilyExtension) -> Self {
        self.extensions.push(extension);
        self
    }
}

/// Closed schema authority for message headers.
///
/// The generated variant remains available to Lily's WebSocket projection.
/// RabbitMQ uses a framework-owned value object so transport adapters cannot
/// provide arbitrary JSON or accidentally document a different envelope than
/// the one enforced at delivery admission.
#[doc(hidden)]
#[derive(Debug)]
pub enum MessageHeaders {
    Generated(SchemaFactory),
    RabbitMqEnvelope(RabbitMqEnvelopeHeaders),
}

/// Exact required header values for one RabbitMQ dispatch key.
#[doc(hidden)]
#[derive(Debug)]
pub struct RabbitMqEnvelopeHeaders {
    pub(crate) schema_version: u16,
    pub(crate) content_kind: String,
}

impl RabbitMqEnvelopeHeaders {
    pub fn new(schema_version: u16, content_kind: impl Into<String>) -> Self {
        Self {
            schema_version,
            content_kind: content_kind.into(),
        }
    }
}

/// Schema authority for a message payload.
#[doc(hidden)]
#[derive(Debug)]
pub enum PayloadSchema {
    Generated(SchemaFactory),
    String,
    Binary,
    Opaque,
    Empty,
}

/// A fixed, framework-owned correlation location.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CorrelationLocation {
    WebSocketAckId,
    RabbitMqCorrelationId,
    RabbitMqMessageId,
}

impl CorrelationLocation {
    pub(crate) const fn as_expression(self) -> &'static str {
        match self {
            Self::WebSocketAckId => "$message.payload#/ack_id",
            Self::RabbitMqCorrelationId => "$message.header#/correlation_id",
            Self::RabbitMqMessageId => "$message.header#/message_id",
        }
    }
}

/// Direction of an operation from the Lily application's perspective.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Send,
    Receive,
}

/// An operation bound to one channel and an explicit subset of its messages.
#[doc(hidden)]
#[derive(Debug)]
pub struct OperationDescriptor {
    pub(crate) identity: String,
    pub(crate) explicit_id: Option<String>,
    pub(crate) action: Action,
    pub(crate) channel: String,
    pub(crate) title: Option<String>,
    pub(crate) summary: Option<String>,
    pub(crate) description: Option<String>,
    pub(crate) tags: Vec<String>,
    pub(crate) security: Vec<String>,
    pub(crate) messages: Vec<String>,
    pub(crate) reply: Option<ReplyDescriptor>,
    pub(crate) extensions: Vec<LilyExtension>,
}

impl OperationDescriptor {
    pub fn new(
        identity: impl Into<String>,
        action: Action,
        channel_identity: impl Into<String>,
    ) -> Self {
        Self {
            identity: identity.into(),
            explicit_id: None,
            action,
            channel: channel_identity.into(),
            title: None,
            summary: None,
            description: None,
            tags: Vec::new(),
            security: Vec::new(),
            messages: Vec::new(),
            reply: None,
            extensions: Vec::new(),
        }
    }

    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.explicit_id = Some(id.into());
        self
    }

    pub fn title(mut self, title: impl Into<String>) -> Self {
        self.title = Some(title.into());
        self
    }

    pub fn summary(mut self, summary: impl Into<String>) -> Self {
        self.summary = Some(summary.into());
        self
    }

    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    pub fn security(mut self, scheme: impl Into<String>) -> Self {
        self.security.push(scheme.into());
        self
    }

    pub fn message(mut self, message_identity: impl Into<String>) -> Self {
        self.messages.push(message_identity.into());
        self
    }

    pub fn reply(mut self, reply: ReplyDescriptor) -> Self {
        self.reply = Some(reply);
        self
    }

    pub fn extension(mut self, extension: LilyExtension) -> Self {
        self.extensions.push(extension);
        self
    }
}

/// A typed AsyncAPI v3 operation reply.
#[doc(hidden)]
#[derive(Debug)]
pub struct ReplyDescriptor {
    pub(crate) channel: String,
    pub(crate) messages: Vec<String>,
}

impl ReplyDescriptor {
    pub fn new(channel_identity: impl Into<String>) -> Self {
        Self {
            channel: channel_identity.into(),
            messages: Vec::new(),
        }
    }

    pub fn message(mut self, message_identity: impl Into<String>) -> Self {
        self.messages.push(message_identity.into());
        self
    }
}

/// Official, version-pinned channel binding descriptors.
#[doc(hidden)]
#[derive(Debug)]
pub enum ChannelBinding {
    WebSocket {
        query: Option<SchemaFactory>,
        headers: Option<SchemaFactory>,
    },
    AmqpQueue(AmqpQueueBinding),
    AmqpRoutingKey(AmqpExchangeBinding),
}

impl ChannelBinding {
    pub fn websocket(query: Option<SchemaFactory>, headers: Option<SchemaFactory>) -> Self {
        Self::WebSocket { query, headers }
    }
}

/// Official AMQP 0.3.0 queue channel binding fields.
#[doc(hidden)]
#[derive(Debug)]
pub struct AmqpQueueBinding {
    pub(crate) name: String,
    pub(crate) durable: bool,
    pub(crate) exclusive: bool,
    pub(crate) auto_delete: bool,
    pub(crate) vhost: String,
}

impl AmqpQueueBinding {
    pub fn new(
        name: impl Into<String>,
        durable: bool,
        exclusive: bool,
        auto_delete: bool,
        vhost: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            durable,
            exclusive,
            auto_delete,
            vhost: vhost.into(),
        }
    }
}

/// Official AMQP 0.3.0 exchange/routing-key channel binding fields.
#[doc(hidden)]
#[derive(Debug)]
pub struct AmqpExchangeBinding {
    pub(crate) name: String,
    pub(crate) kind: AmqpExchangeKind,
    pub(crate) durable: bool,
    pub(crate) auto_delete: bool,
    pub(crate) vhost: String,
}

impl AmqpExchangeBinding {
    pub fn new(
        name: impl Into<String>,
        kind: AmqpExchangeKind,
        durable: bool,
        auto_delete: bool,
        vhost: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            kind,
            durable,
            auto_delete,
            vhost: vhost.into(),
        }
    }
}

/// Official AMQP exchange types.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AmqpExchangeKind {
    Topic,
    Direct,
    Fanout,
    Default,
    Headers,
}

impl AmqpExchangeKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Topic => "topic",
            Self::Direct => "direct",
            Self::Fanout => "fanout",
            Self::Default => "default",
            Self::Headers => "headers",
        }
    }
}
