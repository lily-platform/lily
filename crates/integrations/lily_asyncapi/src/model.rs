use std::collections::BTreeMap;

use serde::Serialize;
use serde_json::Value;

pub(crate) const ASYNCAPI_VERSION: &str = "3.1.0";

/// Lily's immutable, validated AsyncAPI 3.1 document subset.
///
/// The document has no mutation methods. It can only be produced from an
/// accepted transport contribution through Lily's hidden composition ABI.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AsyncApiDocument {
    asyncapi: &'static str,
    info: AsyncApiInfo,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    servers: BTreeMap<String, ServerObject>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    channels: BTreeMap<String, ChannelObject>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    operations: BTreeMap<String, OperationObject>,
    #[serde(skip_serializing_if = "ComponentsObject::is_empty")]
    components: ComponentsObject,
}

impl AsyncApiDocument {
    /// Returns the exact AsyncAPI specification version emitted by Lily.
    pub const fn specification_version(&self) -> &'static str {
        self.asyncapi
    }

    /// Returns the document title.
    pub fn title(&self) -> &str {
        &self.info.title
    }

    /// Returns the application API version.
    pub fn version(&self) -> &str {
        &self.info.version
    }

    /// Returns the number of advertised servers.
    pub fn server_count(&self) -> usize {
        self.servers.len()
    }

    /// Returns the number of physical channels.
    pub fn channel_count(&self) -> usize {
        self.channels.len()
    }

    /// Returns the number of application operations.
    pub fn operation_count(&self) -> usize {
        self.operations.len()
    }

    /// Returns the number of reusable message components.
    pub fn message_count(&self) -> usize {
        self.components.messages.len()
    }

    /// Returns the number of reusable schema components.
    pub fn schema_count(&self) -> usize {
        self.components.schemas.len()
    }

    pub(crate) fn new(
        info: AsyncApiInfo,
        servers: BTreeMap<String, ServerObject>,
        channels: BTreeMap<String, ChannelObject>,
        operations: BTreeMap<String, OperationObject>,
        components: ComponentsObject,
    ) -> Self {
        Self {
            asyncapi: ASYNCAPI_VERSION,
            info,
            servers,
            channels,
            operations,
            components,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct AsyncApiInfo {
    pub(crate) title: String,
    pub(crate) version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) description: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) tags: Vec<TagObject>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct TagObject {
    pub(crate) name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) description: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct ServerObject {
    pub(crate) host: String,
    pub(crate) protocol: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) pathname: Option<String>,
    #[serde(rename = "protocolVersion", skip_serializing_if = "Option::is_none")]
    pub(crate) protocol_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) description: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) security: Vec<Reference>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct ChannelObject {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) summary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) description: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) servers: Vec<Reference>,
    pub(crate) messages: BTreeMap<String, Reference>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) bindings: BTreeMap<String, Value>,
    #[serde(flatten, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) extensions: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum OperationAction {
    Send,
    Receive,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct OperationObject {
    pub(crate) action: OperationAction,
    pub(crate) channel: Reference,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) summary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) description: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) tags: Vec<TagObject>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) security: Vec<Reference>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) messages: Vec<Reference>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) reply: Option<OperationReply>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) bindings: BTreeMap<String, Value>,
    #[serde(flatten, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) extensions: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct OperationReply {
    pub(crate) channel: Reference,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) messages: Vec<Reference>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct MessageObject {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) summary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) description: Option<String>,
    #[serde(rename = "contentType", skip_serializing_if = "Option::is_none")]
    pub(crate) content_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) payload: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) headers: Option<Value>,
    #[serde(rename = "correlationId", skip_serializing_if = "Option::is_none")]
    pub(crate) correlation_id: Option<CorrelationIdObject>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) tags: Vec<TagObject>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) examples: Vec<MessageExample>,
    #[serde(skip_serializing_if = "is_false")]
    pub(crate) deprecated: bool,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) bindings: BTreeMap<String, Value>,
    #[serde(flatten, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) extensions: BTreeMap<String, Value>,
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct MessageExample {
    pub(crate) payload: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct CorrelationIdObject {
    pub(crate) location: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) description: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Default)]
pub(crate) struct ComponentsObject {
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) schemas: BTreeMap<String, Value>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) messages: BTreeMap<String, MessageObject>,
    #[serde(rename = "securitySchemes", skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) security_schemes: BTreeMap<String, Value>,
}

impl ComponentsObject {
    fn is_empty(&self) -> bool {
        self.schemas.is_empty() && self.messages.is_empty() && self.security_schemes.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct Reference {
    #[serde(rename = "$ref")]
    pub(crate) reference: String,
}

impl Reference {
    pub(crate) fn new(reference: impl Into<String>) -> Self {
        Self {
            reference: reference.into(),
        }
    }
}
