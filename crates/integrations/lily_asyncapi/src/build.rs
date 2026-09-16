use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use serde_json::{Value, json};

use crate::{
    AsyncApiBuildError, AsyncApiConfig, AsyncApiDocument,
    contribution::{
        Action, AmqpExchangeBinding, AmqpQueueBinding, ChannelBinding, ChannelDescriptor,
        Documentation, MessageDescriptor, MessageHeaders, OperationDescriptor, PayloadSchema,
        RabbitMqEnvelopeHeaders, TransportContribution, TransportKind,
    },
    extensions::{ExtensionScope, build_extensions},
    limits,
    model::{
        ChannelObject, ComponentsObject, CorrelationIdObject, MessageExample, MessageObject,
        OperationAction, OperationObject, OperationReply, Reference, ServerObject, TagObject,
    },
    schema::SchemaRegistry,
    service::{AsyncApiService, PreparedAsyncApi},
    validation::{
        GeneratedKeyTracker, TextKind, canonical_document_bytes, normalize_json, validate_count,
        validate_example, validate_identifier, validate_mime, validate_optional_description,
        validate_optional_text, validate_required, validate_required_text, validate_security_name,
        validate_server_name,
    },
};

type RuntimeKey = (TransportKind, String);

/// Builds a complete immutable document and its byte-stable compact JSON.
#[doc(hidden)]
pub fn build_document(
    config: &AsyncApiConfig,
    contributions: Vec<TransportContribution>,
) -> Result<(AsyncApiDocument, Arc<[u8]>), AsyncApiBuildError> {
    config.validate_references()?;

    let mut channels = Vec::new();
    let mut messages = Vec::new();
    let mut operations = Vec::new();
    let mut channel_statuses = BTreeMap::new();
    let mut message_statuses = BTreeMap::new();
    let mut operation_statuses = BTreeMap::new();
    for contribution in contributions {
        collect_documented(
            contribution.transport,
            "channel",
            contribution.channels,
            |descriptor| &descriptor.identity,
            &mut channel_statuses,
            &mut channels,
        )?;
        collect_documented(
            contribution.transport,
            "message",
            contribution.messages,
            |descriptor| &descriptor.identity,
            &mut message_statuses,
            &mut messages,
        )?;
        collect_documented(
            contribution.transport,
            "operation",
            contribution.operations,
            |descriptor| &descriptor.identity,
            &mut operation_statuses,
            &mut operations,
        )?;
    }

    validate_count("channels", channels.len(), limits::CHANNEL_COUNT)?;
    validate_count("messages", messages.len(), limits::MESSAGE_COUNT)?;
    validate_count("operations", operations.len(), limits::OPERATION_COUNT)?;

    if (!channels.is_empty() || !messages.is_empty() || !operations.is_empty())
        && !config.has_servers()
    {
        return Err(AsyncApiBuildError::validation(
            "servers",
            "at least one advertised server is required when transport operations are documented",
        ));
    }

    sort_contributions(&mut channels, |descriptor| &descriptor.identity);
    sort_contributions(&mut messages, |descriptor| &descriptor.identity);
    sort_contributions(&mut operations, |descriptor| &descriptor.identity);

    let mut key_tracker = GeneratedKeyTracker::default();
    let mut document_key_owners = BTreeMap::new();
    let channel_keys = assign_keys(
        &channels,
        "channel",
        |descriptor| (&descriptor.identity, descriptor.explicit_id.as_deref()),
        &mut key_tracker,
        &mut document_key_owners,
    )?;
    let message_keys = assign_keys(
        &messages,
        "message",
        |descriptor| (&descriptor.identity, descriptor.explicit_id.as_deref()),
        &mut key_tracker,
        &mut document_key_owners,
    )?;
    let operation_keys = assign_keys(
        &operations,
        "operation",
        |descriptor| (&descriptor.identity, descriptor.explicit_id.as_deref()),
        &mut key_tracker,
        &mut document_key_owners,
    )?;

    let info = config.info();
    let known_tags: BTreeMap<String, TagObject> = info
        .tags
        .iter()
        .cloned()
        .map(|tag| (tag.name.clone(), tag))
        .collect();
    let servers = config.server_objects()?;
    let mut schema_registry = SchemaRegistry::default();
    let message_objects =
        build_messages(messages, &message_keys, &known_tags, &mut schema_registry)?;
    let channel_objects = build_channels(
        channels,
        &channel_keys,
        &message_keys,
        &message_objects,
        &servers,
        &mut schema_registry,
    )?;
    let operation_objects = build_operations(
        operations,
        &operation_keys,
        &channel_keys,
        &message_keys,
        &channel_objects,
        &message_objects,
        &known_tags,
        config,
    )?;
    validate_reachability(&channel_objects, &message_objects, &operation_objects)?;

    let schemas = schema_registry.into_schemas();
    for schema_name in schemas.keys() {
        if let Some((kind, owner)) = document_key_owners.get(schema_name) {
            return Err(AsyncApiBuildError::Collision {
                kind: "schema",
                identifier: schema_name.clone(),
                detail: format!(
                    "component key is already owned by {kind} `{}:{}`",
                    owner.0.prefix(),
                    owner.1
                ),
            });
        }
    }
    let components = ComponentsObject {
        schemas,
        messages: message_objects,
        security_schemes: config.security_scheme_objects(),
    };
    let document = AsyncApiDocument::new(
        info,
        servers,
        channel_objects,
        operation_objects,
        components,
    );
    let bytes: Arc<[u8]> = canonical_document_bytes(&document)?.into();
    Ok((document, bytes))
}

fn validate_reachability(
    channels: &BTreeMap<String, ChannelObject>,
    messages: &BTreeMap<String, MessageObject>,
    operations: &BTreeMap<String, OperationObject>,
) -> Result<(), AsyncApiBuildError> {
    let mut referenced_channels = BTreeSet::new();
    let mut referenced_channel_messages = BTreeSet::new();
    for operation in operations.values() {
        referenced_channels.insert(operation.channel.reference.as_str());
        referenced_channel_messages.extend(
            operation
                .messages
                .iter()
                .map(|message| message.reference.as_str()),
        );
        if let Some(reply) = &operation.reply {
            referenced_channels.insert(reply.channel.reference.as_str());
            referenced_channel_messages.extend(
                reply
                    .messages
                    .iter()
                    .map(|message| message.reference.as_str()),
            );
        }
    }

    let mut channel_message_components = BTreeSet::new();
    for (channel_key, channel) in channels {
        let channel_reference = format!("#/channels/{channel_key}");
        if !referenced_channels.contains(channel_reference.as_str()) {
            return Err(AsyncApiBuildError::Reference {
                owner: format!("channel:{channel_key}"),
                target: channel_reference,
                detail: "documented channel is not reachable from any operation or reply"
                    .to_owned(),
            });
        }
        for (message_key, message) in &channel.messages {
            channel_message_components.insert(message.reference.as_str());
            let local_reference = format!("#/channels/{channel_key}/messages/{message_key}");
            if !referenced_channel_messages.contains(local_reference.as_str()) {
                return Err(AsyncApiBuildError::Reference {
                    owner: format!("channel:{channel_key}"),
                    target: local_reference,
                    detail: "channel-local message is not reachable from any operation or reply"
                        .to_owned(),
                });
            }
        }
    }

    for message_key in messages.keys() {
        let component_reference = format!("#/components/messages/{message_key}");
        if !channel_message_components.contains(component_reference.as_str()) {
            return Err(AsyncApiBuildError::Reference {
                owner: format!("message:{message_key}"),
                target: component_reference,
                detail: "documented message is not reachable from any channel".to_owned(),
            });
        }
    }
    Ok(())
}

/// Builds and binds a document into an opaque marker-specific attachment token.
#[doc(hidden)]
pub fn prepare_document<K>(
    config: &AsyncApiConfig,
    contributions: Vec<TransportContribution>,
) -> Result<PreparedAsyncApi<K>, AsyncApiBuildError> {
    let (document, bytes) = build_document(config, contributions)?;
    Ok(AsyncApiService::<K>::prepare(document, bytes))
}

fn collect_documented<T>(
    transport: TransportKind,
    kind: &'static str,
    records: Vec<Documentation<T>>,
    identity: impl Fn(&T) -> &str,
    statuses: &mut BTreeMap<RuntimeKey, bool>,
    output: &mut Vec<(TransportKind, T)>,
) -> Result<(), AsyncApiBuildError> {
    for record in records {
        match record {
            Documentation::Documented(record) => {
                let record_identity = identity(&record).to_owned();
                validate_required(
                    "documentation.documented.identity",
                    &record_identity,
                    limits::RUNTIME_IDENTITY_BYTES,
                )?;
                register_documentation_status(statuses, transport, kind, &record_identity, true)?;
                output.push((transport, record));
            }
            Documentation::Skipped { identity } => {
                validate_required(
                    "documentation.skipped.identity",
                    &identity,
                    limits::RUNTIME_IDENTITY_BYTES,
                )?;
                register_documentation_status(statuses, transport, kind, &identity, false)?;
            }
            Documentation::Unspecified { identity } => {
                validate_required(
                    "documentation.unspecified.identity",
                    &identity,
                    limits::RUNTIME_IDENTITY_BYTES,
                )?;
                return Err(AsyncApiBuildError::validation(
                    "documentation.status",
                    format!("{kind} `{identity}` has unspecified AsyncAPI documentation status"),
                ));
            }
        }
    }
    Ok(())
}

fn register_documentation_status(
    statuses: &mut BTreeMap<RuntimeKey, bool>,
    transport: TransportKind,
    kind: &'static str,
    identity: &str,
    documented: bool,
) -> Result<(), AsyncApiBuildError> {
    let key = (transport, identity.to_owned());
    match statuses.insert(key, documented) {
        None => Ok(()),
        Some(previous) if previous == documented => Err(AsyncApiBuildError::duplicate(
            kind,
            format!("{}:{identity}", transport.prefix()),
        )),
        Some(_) => Err(AsyncApiBuildError::Collision {
            kind,
            identifier: format!("{}:{identity}", transport.prefix()),
            detail: "the same accepted runtime identity is both documented and skipped".to_owned(),
        }),
    }
}

fn sort_contributions<T>(records: &mut [(TransportKind, T)], identity: impl Fn(&T) -> &str) {
    records.sort_by(|(left_transport, left), (right_transport, right)| {
        left_transport
            .cmp(right_transport)
            .then_with(|| identity(left).cmp(identity(right)))
    });
}

fn assign_keys<T>(
    records: &[(TransportKind, T)],
    kind: &'static str,
    identity_and_id: impl Fn(&T) -> (&str, Option<&str>),
    tracker: &mut GeneratedKeyTracker,
    owners: &mut BTreeMap<String, (&'static str, RuntimeKey)>,
) -> Result<BTreeMap<RuntimeKey, String>, AsyncApiBuildError> {
    let mut keys = BTreeMap::new();
    for (transport, descriptor) in records {
        let (identity, explicit_id) = identity_and_id(descriptor);
        validate_required("runtime.identity", identity, limits::RUNTIME_IDENTITY_BYTES)?;
        let runtime_key = (*transport, identity.to_owned());
        if keys.contains_key(&runtime_key) {
            return Err(AsyncApiBuildError::duplicate(kind, identity));
        }

        let key = if let Some(explicit_id) = explicit_id {
            validate_identifier("explicit.identifier", explicit_id)?;
            explicit_id.to_owned()
        } else {
            tracker.generate(&format!("{}_{}_", transport.prefix(), kind), identity)?
        };
        if let Some((existing_kind, existing)) =
            owners.insert(key.clone(), (kind, runtime_key.clone()))
        {
            return Err(AsyncApiBuildError::Collision {
                kind,
                identifier: key,
                detail: format!(
                    "owned by both {existing_kind} `{}:{}` and {kind} `{}:{}`",
                    existing.0.prefix(),
                    existing.1,
                    transport.prefix(),
                    identity
                ),
            });
        }
        keys.insert(runtime_key, key);
    }
    Ok(keys)
}

fn build_messages(
    messages: Vec<(TransportKind, MessageDescriptor)>,
    keys: &BTreeMap<RuntimeKey, String>,
    known_tags: &BTreeMap<String, TagObject>,
    schemas: &mut SchemaRegistry,
) -> Result<BTreeMap<String, MessageObject>, AsyncApiBuildError> {
    let mut output = BTreeMap::new();
    for (transport, message) in messages {
        validate_message(&message)?;
        let key = lookup_key(keys, transport, &message.identity, "message")?;
        let content_type = validate_mime("message.content_type", &message.content_type)?;
        let payload = match message.payload {
            PayloadSchema::Generated(factory) => {
                let name = schemas.register(factory)?;
                Some(schema_reference(&name))
            }
            PayloadSchema::String => Some(json!({ "type": "string" })),
            PayloadSchema::Binary => Some(json!({
                "type": "string",
                "format": "binary"
            })),
            PayloadSchema::Opaque => Some(json!({})),
            PayloadSchema::Empty => None,
        };
        let headers = message
            .headers
            .map(|headers| build_message_headers(headers, schemas))
            .transpose()?;
        let tags = resolve_tags(message.tags, known_tags, "message.tags")?;
        let examples = normalize_examples(message.examples)?;
        let extensions = build_extensions(
            transport,
            ExtensionScope::Message,
            &message.identity,
            message.extensions,
        )?;

        let object = MessageObject {
            name: message.name,
            title: message.title,
            summary: message.summary,
            description: message.description,
            content_type: Some(content_type),
            payload,
            headers,
            correlation_id: message.correlation.map(|location| CorrelationIdObject {
                location: location.as_expression().to_owned(),
                description: None,
            }),
            tags,
            examples,
            deprecated: message.deprecated,
            bindings: BTreeMap::new(),
            extensions,
        };
        if output.insert(key.clone(), object).is_some() {
            return Err(AsyncApiBuildError::duplicate("message", key));
        }
    }
    Ok(output)
}

fn build_message_headers(
    headers: MessageHeaders,
    schemas: &mut SchemaRegistry,
) -> Result<Value, AsyncApiBuildError> {
    match headers {
        MessageHeaders::Generated(factory) => schemas
            .register(factory)
            .map(|name| schema_reference(&name)),
        MessageHeaders::RabbitMqEnvelope(headers) => rabbitmq_envelope_headers(headers),
    }
}

fn rabbitmq_envelope_headers(
    headers: RabbitMqEnvelopeHeaders,
) -> Result<Value, AsyncApiBuildError> {
    if headers.schema_version == 0 {
        return Err(AsyncApiBuildError::validation(
            "message.headers.x-lily-schema-version",
            "must be a positive u16",
        ));
    }
    validate_required(
        "message.headers.x-lily-content-kind",
        &headers.content_kind,
        64,
    )?;
    if !headers
        .content_kind
        .bytes()
        .enumerate()
        .all(|(index, byte)| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || (index != 0 && matches!(byte, b'-' | b'_' | b'.' | b'+'))
        })
    {
        return Err(AsyncApiBuildError::validation(
            "message.headers.x-lily-content-kind",
            "must be the canonical lower-case runtime token",
        ));
    }

    Ok(json!({
        "type": "object",
        "additionalProperties": true,
        "required": [
            "x-lily-event-id",
            "x-lily-schema-version",
            "x-lily-content-kind"
        ],
        "properties": {
            "x-lily-event-id": {
                "type": "string",
                "minLength": 36,
                "maxLength": 36,
                "pattern": "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
                "not": { "const": "00000000-0000-0000-0000-000000000000" }
            },
            "x-lily-schema-version": {
                "type": "string",
                "const": headers.schema_version.to_string()
            },
            "x-lily-content-kind": {
                "type": "string",
                "const": headers.content_kind
            }
        }
    }))
}

fn validate_message(message: &MessageDescriptor) -> Result<(), AsyncApiBuildError> {
    validate_optional_text(
        "message.name",
        message.name.as_deref(),
        limits::IDENTIFIER_BYTES,
        TextKind::Token,
    )?;
    validate_optional_text(
        "message.title",
        message.title.as_deref(),
        limits::SUMMARY_BYTES,
        TextKind::Token,
    )?;
    validate_optional_text(
        "message.summary",
        message.summary.as_deref(),
        limits::SUMMARY_BYTES,
        TextKind::Token,
    )?;
    validate_optional_description(
        "message.description",
        message.description.as_deref(),
        limits::DESCRIPTION_BYTES,
    )?;
    validate_count(
        "message.tags",
        message.tags.len(),
        limits::EFFECTIVE_TAG_COUNT,
    )?;
    for tag in &message.tags {
        validate_required("message.tags[]", tag, limits::TAG_NAME_BYTES)?;
    }
    validate_count(
        "message.examples",
        message.examples.len(),
        limits::EXAMPLE_COUNT,
    )?;
    Ok(())
}

fn normalize_examples(examples: Vec<Value>) -> Result<Vec<MessageExample>, AsyncApiBuildError> {
    let mut total = 0usize;
    let mut output = Vec::with_capacity(examples.len());
    for example in examples {
        let example = validate_example(&example)?;
        let bytes =
            serde_json::to_vec(&example).map_err(|error| AsyncApiBuildError::Serialization {
                detail: error.to_string(),
            })?;
        total = total.checked_add(bytes.len()).ok_or_else(|| {
            AsyncApiBuildError::validation("message.examples", "total byte count overflowed")
        })?;
        if total > limits::EXAMPLE_TOTAL_BYTES {
            return Err(AsyncApiBuildError::validation(
                "message.examples",
                format!(
                    "total canonical JSON exceeds {} bytes",
                    limits::EXAMPLE_TOTAL_BYTES
                ),
            ));
        }
        output.push(MessageExample { payload: example });
    }
    Ok(output)
}

fn build_channels(
    channels: Vec<(TransportKind, ChannelDescriptor)>,
    channel_keys: &BTreeMap<RuntimeKey, String>,
    message_keys: &BTreeMap<RuntimeKey, String>,
    message_objects: &BTreeMap<String, MessageObject>,
    servers: &BTreeMap<String, ServerObject>,
    schemas: &mut SchemaRegistry,
) -> Result<BTreeMap<String, ChannelObject>, AsyncApiBuildError> {
    let mut output = BTreeMap::new();
    for (transport, mut channel) in channels {
        validate_channel(&channel)?;
        sort_unique(&mut channel.servers, "channel.servers")?;
        sort_unique(&mut channel.messages, "channel.messages")?;
        if channel.servers.is_empty() {
            return Err(AsyncApiBuildError::validation(
                "channel.servers",
                "a documented channel must reference at least one advertised server",
            ));
        }
        if channel.messages.is_empty() {
            return Err(AsyncApiBuildError::validation(
                "channel.messages",
                "a documented channel must expose at least one exact message",
            ));
        }

        let server_refs = channel
            .servers
            .into_iter()
            .map(|name| {
                let Some(server) = servers.get(&name) else {
                    return Err(AsyncApiBuildError::Reference {
                        owner: channel.identity.clone(),
                        target: name,
                        detail: "advertised server is not registered".to_owned(),
                    });
                };
                let protocol_matches = match transport {
                    TransportKind::WebSocket => matches!(server.protocol.as_str(), "ws" | "wss"),
                    TransportKind::Amqp => matches!(server.protocol.as_str(), "amqp" | "amqps"),
                };
                if !protocol_matches {
                    return Err(AsyncApiBuildError::Reference {
                        owner: channel.identity.clone(),
                        target: name,
                        detail: format!(
                            "server protocol `{}` does not match the {} contribution",
                            server.protocol,
                            transport.prefix()
                        ),
                    });
                }
                Ok(Reference::new(format!("#/servers/{name}")))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut local_messages = BTreeMap::new();
        for identity in channel.messages {
            let key = lookup_key(message_keys, transport, &identity, "message")?;
            if !message_objects.contains_key(key) {
                return Err(AsyncApiBuildError::Reference {
                    owner: channel.identity.clone(),
                    target: identity,
                    detail: "message was skipped or was not accepted".to_owned(),
                });
            }
            local_messages.insert(
                key.clone(),
                Reference::new(format!("#/components/messages/{key}")),
            );
        }
        let bindings = build_channel_binding(transport, channel.binding, schemas)?;
        let extensions = build_extensions(
            transport,
            ExtensionScope::Channel,
            &channel.identity,
            channel.extensions,
        )?;
        let key = lookup_key(channel_keys, transport, &channel.identity, "channel")?;
        let object = ChannelObject {
            address: Some(channel.address),
            title: channel.title,
            summary: channel.summary,
            description: channel.description,
            servers: server_refs,
            messages: local_messages,
            bindings,
            extensions,
        };
        if output.insert(key.clone(), object).is_some() {
            return Err(AsyncApiBuildError::duplicate("channel", key));
        }
    }
    Ok(output)
}

fn validate_channel(channel: &ChannelDescriptor) -> Result<(), AsyncApiBuildError> {
    validate_required_text(
        "channel.address",
        &channel.address,
        limits::CHANNEL_ADDRESS_BYTES,
        TextKind::Token,
    )?;
    validate_optional_text(
        "channel.title",
        channel.title.as_deref(),
        limits::SUMMARY_BYTES,
        TextKind::Token,
    )?;
    validate_optional_text(
        "channel.summary",
        channel.summary.as_deref(),
        limits::SUMMARY_BYTES,
        TextKind::Token,
    )?;
    validate_optional_description(
        "channel.description",
        channel.description.as_deref(),
        limits::DESCRIPTION_BYTES,
    )?;
    validate_count(
        "channel.servers",
        channel.servers.len(),
        limits::SERVER_COUNT,
    )?;
    for server in &channel.servers {
        validate_server_name("channel.servers[]", server)?;
    }
    validate_count(
        "channel.messages",
        channel.messages.len(),
        limits::MESSAGE_COUNT,
    )?;
    for message in &channel.messages {
        validate_required(
            "channel.messages[]",
            message,
            limits::RUNTIME_IDENTITY_BYTES,
        )?;
    }
    Ok(())
}

fn build_channel_binding(
    transport: TransportKind,
    binding: Option<ChannelBinding>,
    schemas: &mut SchemaRegistry,
) -> Result<BTreeMap<String, Value>, AsyncApiBuildError> {
    let Some(binding) = binding else {
        return Ok(BTreeMap::new());
    };
    let (name, value) = match binding {
        ChannelBinding::WebSocket { query, headers } => {
            if transport != TransportKind::WebSocket {
                return Err(AsyncApiBuildError::validation(
                    "channel.binding",
                    "WebSocket binding cannot be attached to an AMQP contribution",
                ));
            }
            let query = query
                .map(|factory| {
                    schemas
                        .register_object(factory, "websocket.query")
                        .map(|name| schema_reference(&name))
                })
                .transpose()?;
            let headers = headers
                .map(|factory| {
                    schemas
                        .register_object(factory, "websocket.headers")
                        .map(|name| schema_reference(&name))
                })
                .transpose()?;
            let mut value = serde_json::Map::new();
            value.insert("bindingVersion".to_owned(), json!("0.1.0"));
            value.insert("method".to_owned(), json!("GET"));
            if let Some(query) = query {
                value.insert("query".to_owned(), query);
            }
            if let Some(headers) = headers {
                value.insert("headers".to_owned(), headers);
            }
            ("ws", Value::Object(value))
        }
        ChannelBinding::AmqpQueue(binding) => {
            require_amqp(transport)?;
            ("amqp", amqp_queue_binding(binding)?)
        }
        ChannelBinding::AmqpRoutingKey(binding) => {
            require_amqp(transport)?;
            ("amqp", amqp_exchange_binding(binding)?)
        }
    };
    Ok(BTreeMap::from([(name.to_owned(), normalize_json(&value)?)]))
}

fn require_amqp(transport: TransportKind) -> Result<(), AsyncApiBuildError> {
    if transport == TransportKind::Amqp {
        Ok(())
    } else {
        Err(AsyncApiBuildError::validation(
            "channel.binding",
            "AMQP binding cannot be attached to a WebSocket contribution",
        ))
    }
}

fn amqp_queue_binding(binding: AmqpQueueBinding) -> Result<Value, AsyncApiBuildError> {
    validate_required("amqp.queue.name", &binding.name, 255)?;
    validate_amqp_virtual_host("amqp.queue.vhost", &binding.vhost)?;
    Ok(json!({
        "bindingVersion": "0.3.0",
        "is": "queue",
        "queue": {
            "autoDelete": binding.auto_delete,
            "durable": binding.durable,
            "exclusive": binding.exclusive,
            "name": binding.name,
            "vhost": binding.vhost
        }
    }))
}

fn amqp_exchange_binding(binding: AmqpExchangeBinding) -> Result<Value, AsyncApiBuildError> {
    validate_required("amqp.exchange.name", &binding.name, 255)?;
    validate_amqp_virtual_host("amqp.exchange.vhost", &binding.vhost)?;
    Ok(json!({
        "bindingVersion": "0.3.0",
        "exchange": {
            "autoDelete": binding.auto_delete,
            "durable": binding.durable,
            "name": binding.name,
            "type": binding.kind.as_str(),
            "vhost": binding.vhost
        },
        "is": "routingKey"
    }))
}

fn validate_amqp_virtual_host(
    field: &'static str,
    virtual_host: &str,
) -> Result<(), AsyncApiBuildError> {
    if virtual_host.len() > 255 || virtual_host.chars().any(char::is_control) {
        return Err(AsyncApiBuildError::validation(
            field,
            "must contain at most 255 control-free UTF-8 bytes",
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn build_operations(
    operations: Vec<(TransportKind, OperationDescriptor)>,
    operation_keys: &BTreeMap<RuntimeKey, String>,
    channel_keys: &BTreeMap<RuntimeKey, String>,
    message_keys: &BTreeMap<RuntimeKey, String>,
    channel_objects: &BTreeMap<String, ChannelObject>,
    message_objects: &BTreeMap<String, MessageObject>,
    known_tags: &BTreeMap<String, TagObject>,
    config: &AsyncApiConfig,
) -> Result<BTreeMap<String, OperationObject>, AsyncApiBuildError> {
    let mut output = BTreeMap::new();
    for (transport, mut operation) in operations {
        validate_operation(&operation)?;
        sort_unique(&mut operation.tags, "operation.tags")?;
        sort_unique(&mut operation.security, "operation.security")?;
        sort_unique(&mut operation.messages, "operation.messages")?;
        if operation.messages.is_empty() {
            return Err(AsyncApiBuildError::validation(
                "operation.messages",
                "Lily operations must enumerate their exact channel-local message subset",
            ));
        }
        let channel_key = lookup_key(channel_keys, transport, &operation.channel, "channel")?;
        let channel =
            channel_objects
                .get(channel_key)
                .ok_or_else(|| AsyncApiBuildError::Reference {
                    owner: operation.identity.clone(),
                    target: operation.channel.clone(),
                    detail: "channel was skipped or was not accepted".to_owned(),
                })?;
        let messages = operation
            .messages
            .into_iter()
            .map(|identity| {
                operation_message_reference(
                    &operation.identity,
                    transport,
                    &identity,
                    channel_key,
                    channel,
                    message_keys,
                    message_objects,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let reply = operation
            .reply
            .map(|mut reply| {
                sort_unique(&mut reply.messages, "operation.reply.messages")?;
                if reply.messages.is_empty() {
                    return Err(AsyncApiBuildError::validation(
                        "operation.reply.messages",
                        "reply must enumerate at least one exact message",
                    ));
                }
                let reply_channel_key =
                    lookup_key(channel_keys, transport, &reply.channel, "channel")?;
                let reply_channel = channel_objects.get(reply_channel_key).ok_or_else(|| {
                    AsyncApiBuildError::Reference {
                        owner: operation.identity.clone(),
                        target: reply.channel.clone(),
                        detail: "reply channel was skipped or was not accepted".to_owned(),
                    }
                })?;
                let reply_messages = reply
                    .messages
                    .into_iter()
                    .map(|identity| {
                        operation_message_reference(
                            &operation.identity,
                            transport,
                            &identity,
                            reply_channel_key,
                            reply_channel,
                            message_keys,
                            message_objects,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(OperationReply {
                    channel: Reference::new(format!("#/channels/{reply_channel_key}")),
                    messages: reply_messages,
                })
            })
            .transpose()?;
        let tags = resolve_tags(operation.tags, known_tags, "operation.tags")?;
        let security = operation
            .security
            .into_iter()
            .map(|name| {
                if !config.has_security_scheme(&name) {
                    return Err(AsyncApiBuildError::Reference {
                        owner: operation.identity.clone(),
                        target: name,
                        detail: "security scheme is not registered".to_owned(),
                    });
                }
                Ok(Reference::new(format!(
                    "#/components/securitySchemes/{name}"
                )))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let extensions = build_extensions(
            transport,
            ExtensionScope::Operation,
            &operation.identity,
            operation.extensions,
        )?;
        let key = lookup_key(operation_keys, transport, &operation.identity, "operation")?;
        let object = OperationObject {
            action: match operation.action {
                Action::Send => OperationAction::Send,
                Action::Receive => OperationAction::Receive,
            },
            channel: Reference::new(format!("#/channels/{channel_key}")),
            title: operation.title,
            summary: operation.summary,
            description: operation.description,
            tags,
            security,
            messages,
            reply,
            bindings: BTreeMap::new(),
            extensions,
        };
        if output.insert(key.clone(), object).is_some() {
            return Err(AsyncApiBuildError::duplicate("operation", key));
        }
    }
    Ok(output)
}

fn validate_operation(operation: &OperationDescriptor) -> Result<(), AsyncApiBuildError> {
    validate_required(
        "operation.channel",
        &operation.channel,
        limits::RUNTIME_IDENTITY_BYTES,
    )?;
    validate_optional_text(
        "operation.title",
        operation.title.as_deref(),
        limits::SUMMARY_BYTES,
        TextKind::Token,
    )?;
    validate_optional_text(
        "operation.summary",
        operation.summary.as_deref(),
        limits::SUMMARY_BYTES,
        TextKind::Token,
    )?;
    validate_optional_description(
        "operation.description",
        operation.description.as_deref(),
        limits::DESCRIPTION_BYTES,
    )?;
    validate_count(
        "operation.tags",
        operation.tags.len(),
        limits::EFFECTIVE_TAG_COUNT,
    )?;
    for tag in &operation.tags {
        validate_required("operation.tags[]", tag, limits::TAG_NAME_BYTES)?;
    }
    validate_count(
        "operation.security",
        operation.security.len(),
        limits::EFFECTIVE_SECURITY_COUNT,
    )?;
    for security in &operation.security {
        validate_security_name("operation.security[]", security)?;
    }
    validate_count(
        "operation.messages",
        operation.messages.len(),
        limits::MESSAGE_COUNT,
    )?;
    for message in &operation.messages {
        validate_required(
            "operation.messages[]",
            message,
            limits::RUNTIME_IDENTITY_BYTES,
        )?;
    }
    if let Some(reply) = &operation.reply {
        validate_required(
            "operation.reply.channel",
            &reply.channel,
            limits::RUNTIME_IDENTITY_BYTES,
        )?;
        validate_count(
            "operation.reply.messages",
            reply.messages.len(),
            limits::MESSAGE_COUNT,
        )?;
        for message in &reply.messages {
            validate_required(
                "operation.reply.messages[]",
                message,
                limits::RUNTIME_IDENTITY_BYTES,
            )?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn operation_message_reference(
    operation_identity: &str,
    transport: TransportKind,
    identity: &str,
    channel_key: &str,
    channel: &ChannelObject,
    message_keys: &BTreeMap<RuntimeKey, String>,
    message_objects: &BTreeMap<String, MessageObject>,
) -> Result<Reference, AsyncApiBuildError> {
    let key = lookup_key(message_keys, transport, identity, "message")?;
    if !message_objects.contains_key(key) || !channel.messages.contains_key(key) {
        return Err(AsyncApiBuildError::Reference {
            owner: operation_identity.to_owned(),
            target: identity.to_owned(),
            detail: "message is not part of the operation's channel".to_owned(),
        });
    }
    Ok(Reference::new(format!(
        "#/channels/{channel_key}/messages/{key}"
    )))
}

fn lookup_key<'a>(
    keys: &'a BTreeMap<RuntimeKey, String>,
    transport: TransportKind,
    identity: &str,
    kind: &'static str,
) -> Result<&'a String, AsyncApiBuildError> {
    keys.get(&(transport, identity.to_owned()))
        .ok_or_else(|| AsyncApiBuildError::Reference {
            owner: format!("{} contribution", transport.prefix()),
            target: identity.to_owned(),
            detail: format!("{kind} was not documented or accepted"),
        })
}

fn resolve_tags(
    mut names: Vec<String>,
    known_tags: &BTreeMap<String, TagObject>,
    field: &'static str,
) -> Result<Vec<TagObject>, AsyncApiBuildError> {
    sort_unique(&mut names, field)?;
    names
        .into_iter()
        .map(|name| {
            known_tags
                .get(&name)
                .cloned()
                .ok_or_else(|| AsyncApiBuildError::Reference {
                    owner: field.to_owned(),
                    target: name,
                    detail: "tag is not registered in AsyncApiConfig".to_owned(),
                })
        })
        .collect()
}

fn sort_unique(values: &mut [String], field: &'static str) -> Result<(), AsyncApiBuildError> {
    values.sort();
    if values.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(AsyncApiBuildError::validation(
            field,
            "duplicate entries are not allowed",
        ));
    }
    Ok(())
}

fn schema_reference(name: &str) -> Value {
    json!({ "$ref": format!("#/components/schemas/{name}") })
}

#[cfg(test)]
mod tests {
    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::{
        AsyncApiServer, AsyncApiServerProtocol, AsyncApiTag,
        contribution::ReplyDescriptor,
        extensions::{
            LilyExtension, WebSocketContentKind, WebSocketOutcomeExtension, WebSocketOutcomeKind,
            WebSocketProtocolExtension, WebSocketWireFormat,
        },
        schema::SchemaFactory,
    };

    #[derive(Debug, Serialize, Deserialize, JsonSchema)]
    struct Event {
        value: String,
    }

    fn config() -> AsyncApiConfig {
        let mut config = AsyncApiConfig::new("Orders", "1.0.0").expect("config");
        config
            .add_server(
                AsyncApiServer::new("public", "events.example.com", AsyncApiServerProtocol::Wss)
                    .expect("server")
                    .with_pathname("/socket")
                    .expect("pathname"),
            )
            .expect("register server");
        config
            .add_tag(AsyncApiTag::new("orders").expect("tag"))
            .expect("register tag");
        config
    }

    fn contribution(reverse: bool) -> TransportContribution {
        let channel = Documentation::Documented(
            ChannelDescriptor::new("socket", "/socket")
                .server("public")
                .message("orders.created")
                .binding(ChannelBinding::websocket(None, None)),
        );
        let message = Documentation::Documented(
            MessageDescriptor::new(
                "orders.created",
                "application/json",
                PayloadSchema::Generated(SchemaFactory::inbound::<Event>()),
            )
            .tag("orders"),
        );
        let operation = Documentation::Documented(
            OperationDescriptor::new("orders.receive", Action::Receive, "socket")
                .message("orders.created")
                .tag("orders"),
        );
        let contribution = TransportContribution::new(TransportKind::WebSocket);
        if reverse {
            contribution
                .operation(operation)
                .message(message)
                .channel(channel)
        } else {
            contribution
                .channel(channel)
                .message(message)
                .operation(operation)
        }
    }

    #[test]
    fn document_is_byte_stable_and_uses_channel_local_operation_message_refs() {
        let (_, first) = build_document(&config(), vec![contribution(false)]).expect("first");
        let (_, second) = build_document(&config(), vec![contribution(true)]).expect("second");
        assert_eq!(first, second);
        let value: Value = serde_json::from_slice(&first).expect("json");
        assert_eq!(value["asyncapi"], "3.1.0");
        let operation = value["operations"]
            .as_object()
            .and_then(|operations| operations.values().next())
            .expect("operation");
        assert!(
            operation["messages"][0]["$ref"]
                .as_str()
                .expect("ref")
                .starts_with("#/channels/")
        );
        assert!(
            !operation
                .as_object()
                .expect("object")
                .contains_key("deprecated")
        );
    }

    #[test]
    fn binary_payload_describes_wire_bytes_without_claiming_base64_encoding() {
        let contribution = TransportContribution::new(TransportKind::WebSocket)
            .channel(Documentation::Documented(
                ChannelDescriptor::new("socket", "/socket")
                    .server("public")
                    .message("binary.event")
                    .binding(ChannelBinding::websocket(None, None)),
            ))
            .message(Documentation::Documented(MessageDescriptor::new(
                "binary.event",
                "application/octet-stream",
                PayloadSchema::Binary,
            )))
            .operation(Documentation::Documented(
                OperationDescriptor::new("binary.receive", Action::Receive, "socket")
                    .message("binary.event"),
            ));

        let (_, bytes) = build_document(&config(), vec![contribution]).expect("binary document");
        let document: Value = serde_json::from_slice(&bytes).expect("canonical JSON");
        let payload = document["components"]["messages"]
            .as_object()
            .and_then(|messages| messages.values().next())
            .and_then(|message| message.get("payload"))
            .expect("binary payload schema");
        assert_eq!(payload["type"], "string");
        assert_eq!(payload["format"], "binary");
        assert!(payload.get("contentEncoding").is_none());
    }

    #[test]
    fn unspecified_and_cross_channel_messages_fail_closed() {
        let unspecified = TransportContribution::new(TransportKind::WebSocket).operation(
            Documentation::Unspecified {
                identity: "orders.receive".to_owned(),
            },
        );
        assert!(build_document(&config(), vec![unspecified]).is_err());

        let invalid = TransportContribution::new(TransportKind::WebSocket)
            .channel(Documentation::Documented(
                ChannelDescriptor::new("socket", "/socket")
                    .server("public")
                    .message("orders.created"),
            ))
            .message(Documentation::Documented(MessageDescriptor::new(
                "orders.created",
                "application/json",
                PayloadSchema::Generated(SchemaFactory::inbound::<Event>()),
            )))
            .message(Documentation::Documented(MessageDescriptor::new(
                "orders.other",
                "application/json",
                PayloadSchema::Generated(SchemaFactory::inbound::<Event>()),
            )))
            .operation(Documentation::Documented(
                OperationDescriptor::new("orders.receive", Action::Receive, "socket")
                    .message("orders.other"),
            ));
        assert!(matches!(
            build_document(&config(), vec![invalid]),
            Err(AsyncApiBuildError::Reference { .. })
        ));
    }

    #[test]
    fn duplicate_and_conflicting_documentation_statuses_fail_closed() {
        let duplicate_skip = TransportContribution::new(TransportKind::WebSocket)
            .message(Documentation::Skipped {
                identity: "orders.created".to_owned(),
            })
            .message(Documentation::Skipped {
                identity: "orders.created".to_owned(),
            });
        assert!(matches!(
            build_document(&config(), vec![duplicate_skip]),
            Err(AsyncApiBuildError::Duplicate { .. })
        ));

        let conflicting = TransportContribution::new(TransportKind::WebSocket)
            .message(Documentation::Documented(MessageDescriptor::new(
                "orders.created",
                "application/json",
                PayloadSchema::Opaque,
            )))
            .message(Documentation::Skipped {
                identity: "orders.created".to_owned(),
            });
        assert!(matches!(
            build_document(&config(), vec![conflicting]),
            Err(AsyncApiBuildError::Collision { .. })
        ));
    }

    #[test]
    fn orphan_messages_channels_and_channel_local_messages_fail_closed() {
        let orphan_message = TransportContribution::new(TransportKind::WebSocket).message(
            Documentation::Documented(MessageDescriptor::new(
                "orders.created",
                "application/json",
                PayloadSchema::Opaque,
            )),
        );
        assert!(matches!(
            build_document(&config(), vec![orphan_message]),
            Err(AsyncApiBuildError::Reference { .. })
        ));

        let orphan_channel = TransportContribution::new(TransportKind::WebSocket)
            .channel(Documentation::Documented(
                ChannelDescriptor::new("socket", "/socket")
                    .server("public")
                    .message("orders.created"),
            ))
            .message(Documentation::Documented(MessageDescriptor::new(
                "orders.created",
                "application/json",
                PayloadSchema::Opaque,
            )));
        assert!(matches!(
            build_document(&config(), vec![orphan_channel]),
            Err(AsyncApiBuildError::Reference { .. })
        ));

        let unused_local_message = TransportContribution::new(TransportKind::WebSocket)
            .channel(Documentation::Documented(
                ChannelDescriptor::new("socket", "/socket")
                    .server("public")
                    .message("orders.created")
                    .message("orders.cancelled"),
            ))
            .message(Documentation::Documented(MessageDescriptor::new(
                "orders.created",
                "application/json",
                PayloadSchema::Opaque,
            )))
            .message(Documentation::Documented(MessageDescriptor::new(
                "orders.cancelled",
                "application/json",
                PayloadSchema::Opaque,
            )))
            .operation(Documentation::Documented(
                OperationDescriptor::new("orders.receive", Action::Receive, "socket")
                    .message("orders.created"),
            ));
        assert!(matches!(
            build_document(&config(), vec![unused_local_message]),
            Err(AsyncApiBuildError::Reference { .. })
        ));
    }

    #[test]
    fn contribution_metadata_bounds_accept_maximum_and_reject_plus_one() {
        let message = MessageDescriptor::new("message", "application/json", PayloadSchema::Opaque)
            .name("n".repeat(limits::IDENTIFIER_BYTES))
            .title("t".repeat(limits::SUMMARY_BYTES))
            .summary("s".repeat(limits::SUMMARY_BYTES))
            .description("d".repeat(limits::DESCRIPTION_BYTES));
        assert!(validate_message(&message).is_ok());
        let over = MessageDescriptor::new("message", "application/json", PayloadSchema::Opaque)
            .name("n".repeat(limits::IDENTIFIER_BYTES + 1));
        assert!(validate_message(&over).is_err());

        let channel = ChannelDescriptor::new("channel", "a".repeat(limits::CHANNEL_ADDRESS_BYTES))
            .title("t".repeat(limits::SUMMARY_BYTES))
            .summary("s".repeat(limits::SUMMARY_BYTES))
            .description("d".repeat(limits::DESCRIPTION_BYTES));
        assert!(validate_channel(&channel).is_ok());
        let over = ChannelDescriptor::new("channel", "a".repeat(limits::CHANNEL_ADDRESS_BYTES + 1));
        assert!(validate_channel(&over).is_err());

        let operation = OperationDescriptor::new("operation", Action::Receive, "channel")
            .title("t".repeat(limits::SUMMARY_BYTES))
            .summary("s".repeat(limits::SUMMARY_BYTES))
            .description("d".repeat(limits::DESCRIPTION_BYTES));
        assert!(validate_operation(&operation).is_ok());
        let over = OperationDescriptor::new("operation", Action::Receive, "channel")
            .description("d".repeat(limits::DESCRIPTION_BYTES + 1));
        assert!(validate_operation(&over).is_err());
    }

    #[test]
    fn contribution_references_are_individually_bounded_and_control_safe() {
        let channel = ChannelDescriptor::new("channel", "/events")
            .server("s".repeat(limits::SERVER_NAME_BYTES))
            .message("m".repeat(limits::RUNTIME_IDENTITY_BYTES));
        assert!(validate_channel(&channel).is_ok());
        assert!(
            validate_channel(
                &ChannelDescriptor::new("channel", "/events")
                    .server("s".repeat(limits::SERVER_NAME_BYTES + 1))
                    .message("message")
            )
            .is_err()
        );
        assert!(
            validate_channel(
                &ChannelDescriptor::new("channel", "/events")
                    .server("public")
                    .message("m".repeat(limits::RUNTIME_IDENTITY_BYTES + 1))
            )
            .is_err()
        );

        let message = MessageDescriptor::new("message", "application/json", PayloadSchema::Opaque)
            .tag("t".repeat(limits::TAG_NAME_BYTES));
        assert!(validate_message(&message).is_ok());
        assert!(
            validate_message(
                &MessageDescriptor::new("message", "application/json", PayloadSchema::Opaque)
                    .tag("tag\nleak")
            )
            .is_err()
        );
        assert!(
            validate_message(
                &MessageDescriptor::new("message", "application/json", PayloadSchema::Opaque)
                    .tag("t".repeat(limits::TAG_NAME_BYTES + 1))
            )
            .is_err()
        );

        let operation = OperationDescriptor::new(
            "operation",
            Action::Receive,
            "c".repeat(limits::RUNTIME_IDENTITY_BYTES),
        )
        .tag("t".repeat(limits::TAG_NAME_BYTES))
        .security("s".repeat(limits::SECURITY_NAME_BYTES))
        .message("m".repeat(limits::RUNTIME_IDENTITY_BYTES))
        .reply(
            ReplyDescriptor::new("r".repeat(limits::RUNTIME_IDENTITY_BYTES))
                .message("m".repeat(limits::RUNTIME_IDENTITY_BYTES)),
        );
        assert!(validate_operation(&operation).is_ok());
        assert!(
            validate_operation(
                &OperationDescriptor::new("operation", Action::Receive, "channel")
                    .security("s".repeat(limits::SECURITY_NAME_BYTES + 1))
                    .message("message")
            )
            .is_err()
        );
        assert!(
            validate_operation(
                &OperationDescriptor::new("operation", Action::Receive, "channel")
                    .message("bad\nreference")
            )
            .is_err()
        );
        assert!(
            validate_operation(
                &OperationDescriptor::new("operation", Action::Receive, "channel")
                    .security("bad security")
                    .message("message")
            )
            .is_err()
        );
        assert!(
            validate_operation(
                &OperationDescriptor::new("operation", Action::Receive, "channel")
                    .message("message")
                    .reply(ReplyDescriptor::new("reply\nchannel").message("reply"))
            )
            .is_err()
        );
    }

    #[test]
    fn examples_enforce_per_message_total_boundary() {
        let exact = (0..limits::EXAMPLE_COUNT)
            .map(|_| Value::String("x".repeat(8190)))
            .collect();
        assert_eq!(
            normalize_examples(exact).expect("exact total").len(),
            limits::EXAMPLE_COUNT
        );
        let over = (0..limits::EXAMPLE_COUNT)
            .map(|index| Value::String("x".repeat(if index == 0 { 8191 } else { 8190 })))
            .collect();
        assert!(normalize_examples(over).is_err());
    }

    #[test]
    fn rabbitmq_headers_are_exact_string_dispatch_contracts() {
        let headers =
            rabbitmq_envelope_headers(RabbitMqEnvelopeHeaders::new(17, "json")).expect("headers");
        assert_eq!(
            headers["required"],
            json!([
                "x-lily-event-id",
                "x-lily-schema-version",
                "x-lily-content-kind"
            ])
        );
        assert_eq!(
            headers["properties"]["x-lily-schema-version"]["const"],
            "17"
        );
        assert_eq!(
            headers["properties"]["x-lily-content-kind"]["const"],
            "json"
        );
        assert_eq!(headers["additionalProperties"], true);
        assert_eq!(
            headers["properties"]["x-lily-event-id"]["not"]["const"],
            "00000000-0000-0000-0000-000000000000"
        );

        assert!(rabbitmq_envelope_headers(RabbitMqEnvelopeHeaders::new(0, "json")).is_err());
        assert!(rabbitmq_envelope_headers(RabbitMqEnvelopeHeaders::new(1, "JSON")).is_err());
        assert!(rabbitmq_envelope_headers(RabbitMqEnvelopeHeaders::new(1, "-json")).is_err());
        assert!(rabbitmq_envelope_headers(RabbitMqEnvelopeHeaders::new(1, "app/json")).is_err());
    }

    #[test]
    fn explicit_identifier_collisions_and_duplicates_fail_closed() {
        let duplicate_key = TransportContribution::new(TransportKind::WebSocket)
            .message(Documentation::Documented(
                MessageDescriptor::new("first", "application/json", PayloadSchema::Opaque)
                    .id("same"),
            ))
            .message(Documentation::Documented(
                MessageDescriptor::new("second", "application/json", PayloadSchema::Opaque)
                    .id("same"),
            ));
        assert!(matches!(
            build_document(&config(), vec![duplicate_key]),
            Err(AsyncApiBuildError::Collision { .. })
        ));

        let duplicate_source = TransportContribution::new(TransportKind::WebSocket)
            .message(Documentation::Documented(MessageDescriptor::new(
                "same-source",
                "application/json",
                PayloadSchema::Opaque,
            )))
            .message(Documentation::Documented(MessageDescriptor::new(
                "same-source",
                "application/json",
                PayloadSchema::Opaque,
            )));
        assert!(matches!(
            build_document(&config(), vec![duplicate_source]),
            Err(AsyncApiBuildError::Duplicate { .. })
        ));
    }

    #[test]
    fn binding_versions_and_full_generated_keys_are_golden() {
        let (_, bytes) = build_document(&config(), vec![contribution(false)]).expect("document");
        let value: Value = serde_json::from_slice(&bytes).expect("json");
        let (channel_key, channel) = value["channels"]
            .as_object()
            .and_then(|channels| channels.iter().next())
            .expect("channel");
        assert!(channel_key.starts_with("ws_channel_"));
        assert_eq!(channel_key.len(), "ws_channel_".len() + 64);
        assert_eq!(channel["bindings"]["ws"]["bindingVersion"], "0.1.0");
        assert_eq!(channel["bindings"]["ws"]["method"], "GET");
        let message_key = value["components"]["messages"]
            .as_object()
            .and_then(|messages| messages.keys().next())
            .expect("message key");
        assert!(message_key.starts_with("ws_message_"));
        assert_eq!(message_key.len(), "ws_message_".len() + 64);
    }

    #[test]
    fn typed_extensions_flow_from_descriptors_into_the_document() {
        let contribution = TransportContribution::new(TransportKind::WebSocket)
            .channel(Documentation::Documented(
                ChannelDescriptor::new("socket", "/socket")
                    .server("public")
                    .message("orders.created"),
            ))
            .message(Documentation::Documented(
                MessageDescriptor::new("orders.created", "application/json", PayloadSchema::Opaque)
                    .extension(LilyExtension::WebSocketProtocol(
                        WebSocketProtocolExtension::new(
                            "orders",
                            "orders:created",
                            WebSocketContentKind::Json,
                            "application/json",
                            "identity",
                        )
                        .wire_format(WebSocketWireFormat::Text),
                    )),
            ))
            .operation(Documentation::Documented(
                OperationDescriptor::new("orders.receive", Action::Receive, "socket")
                    .message("orders.created")
                    .extension(LilyExtension::WebSocketOutcome(
                        WebSocketOutcomeExtension::new(vec![WebSocketOutcomeKind::NoReply]),
                    )),
            ));
        let (_, bytes) = build_document(&config(), vec![contribution]).expect("document");
        let value: Value = serde_json::from_slice(&bytes).expect("JSON");
        let message = value["components"]["messages"]
            .as_object()
            .and_then(|messages| messages.values().next())
            .expect("message");
        assert_eq!(message["x-lily-websocket-protocol"]["envelope_version"], 2);
        let operation = value["operations"]
            .as_object()
            .and_then(|operations| operations.values().next())
            .expect("operation");
        assert_eq!(
            operation["x-lily-websocket-outcome"]["outcomes"][0],
            "no_reply"
        );
        assert!(operation.get("x-lily-runtime-identity").is_some());
    }

    #[test]
    fn websocket_binding_rejects_non_object_query_and_header_schemas() {
        let scalar_query = TransportContribution::new(TransportKind::WebSocket)
            .message(Documentation::Documented(MessageDescriptor::new(
                "event",
                "application/json",
                PayloadSchema::Opaque,
            )))
            .channel(Documentation::Documented(
                ChannelDescriptor::new("socket", "/socket")
                    .server("public")
                    .message("event")
                    .binding(ChannelBinding::websocket(
                        Some(SchemaFactory::inbound::<String>()),
                        None,
                    )),
            ));
        assert!(matches!(
            build_document(&config(), vec![scalar_query]),
            Err(AsyncApiBuildError::Schema { .. })
        ));

        let scalar_headers = TransportContribution::new(TransportKind::WebSocket)
            .message(Documentation::Documented(MessageDescriptor::new(
                "event",
                "application/json",
                PayloadSchema::Opaque,
            )))
            .channel(Documentation::Documented(
                ChannelDescriptor::new("socket", "/socket")
                    .server("public")
                    .message("event")
                    .binding(ChannelBinding::websocket(
                        None,
                        Some(SchemaFactory::inbound::<String>()),
                    )),
            ));
        assert!(matches!(
            build_document(&config(), vec![scalar_headers]),
            Err(AsyncApiBuildError::Schema { .. })
        ));
    }

    #[test]
    fn amqp_channel_binding_uses_exact_030_shape() {
        let mut config = AsyncApiConfig::new("Orders", "1.0.0").expect("config");
        config
            .add_server(
                AsyncApiServer::new(
                    "broker",
                    "rabbitmq.example.com:5671",
                    AsyncApiServerProtocol::Amqps,
                )
                .expect("server"),
            )
            .expect("server registration");
        let contribution = TransportContribution::new(TransportKind::Amqp)
            .channel(Documentation::Documented(
                ChannelDescriptor::new("orders.queue", "orders")
                    .server("broker")
                    .message("orders.created.v1")
                    .binding(ChannelBinding::AmqpQueue(AmqpQueueBinding::new(
                        "orders", true, false, false, "/",
                    ))),
            ))
            .message(Documentation::Documented(MessageDescriptor::new(
                "orders.created.v1",
                "application/json",
                PayloadSchema::Generated(SchemaFactory::inbound::<Event>()),
            )))
            .operation(Documentation::Documented(
                OperationDescriptor::new("orders.consume.v1", Action::Receive, "orders.queue")
                    .message("orders.created.v1"),
            ));
        let (_, bytes) = build_document(&config, vec![contribution]).expect("document");
        let value: Value = serde_json::from_slice(&bytes).expect("json");
        let channel = value["channels"]
            .as_object()
            .and_then(|channels| channels.values().next())
            .expect("channel");
        assert_eq!(channel["bindings"]["amqp"]["bindingVersion"], "0.3.0");
        assert_eq!(channel["bindings"]["amqp"]["is"], "queue");
        assert_eq!(channel["bindings"]["amqp"]["queue"]["name"], "orders");
    }

    #[test]
    fn amqp_channel_binding_preserves_the_runtime_empty_virtual_host() {
        let queue = amqp_queue_binding(AmqpQueueBinding::new("orders", true, false, false, ""))
            .expect("empty RabbitMQ vhost is a valid runtime identity");
        assert_eq!(queue["queue"]["vhost"], "");

        let exchange = amqp_exchange_binding(AmqpExchangeBinding::new(
            "orders",
            crate::contribution::AmqpExchangeKind::Topic,
            true,
            false,
            "",
        ))
        .expect("empty RabbitMQ vhost is a valid runtime identity");
        assert_eq!(exchange["exchange"]["vhost"], "");
    }

    #[test]
    fn channel_cannot_reference_a_server_for_another_transport() {
        let mut config = AsyncApiConfig::new("Orders", "1.0.0").expect("config");
        config
            .add_server(
                AsyncApiServer::new(
                    "broker",
                    "rabbitmq.example.com",
                    AsyncApiServerProtocol::Amqps,
                )
                .expect("server"),
            )
            .expect("server registration");
        let contribution = TransportContribution::new(TransportKind::WebSocket)
            .channel(Documentation::Documented(
                ChannelDescriptor::new("socket", "/socket")
                    .server("broker")
                    .message("event"),
            ))
            .message(Documentation::Documented(MessageDescriptor::new(
                "event",
                "application/json",
                PayloadSchema::Opaque,
            )))
            .operation(Documentation::Documented(
                OperationDescriptor::new("receive", Action::Receive, "socket").message("event"),
            ));
        assert!(matches!(
            build_document(&config, vec![contribution]),
            Err(AsyncApiBuildError::Reference { .. })
        ));
    }
}
