//! Frozen CAP-ASYNC-00 bounds.

pub(crate) const DOCUMENT_TITLE_BYTES: usize = 128;
pub(crate) const DOCUMENT_VERSION_BYTES: usize = 64;
pub(crate) const DOCUMENT_DESCRIPTION_BYTES: usize = 8 * 1024;
pub(crate) const SERVER_COUNT: usize = 16;
pub(crate) const SERVER_NAME_BYTES: usize = 64;
pub(crate) const SERVER_HOST_BYTES: usize = 512;
pub(crate) const SERVER_PATHNAME_BYTES: usize = 1024;
pub(crate) const SERVER_PROTOCOL_VERSION_BYTES: usize = 32;
pub(crate) const SERVER_DESCRIPTION_BYTES: usize = 2 * 1024;
pub(crate) const IDENTIFIER_BYTES: usize = 128;
pub(crate) const RUNTIME_IDENTITY_BYTES: usize = 1024;
pub(crate) const SCHEMA_ID_BYTES: usize = 1024;
pub(crate) const CONTENT_TYPE_BYTES: usize = 256;
pub(crate) const CHANNEL_ADDRESS_BYTES: usize = 1024;
pub(crate) const SUMMARY_BYTES: usize = 256;
pub(crate) const DESCRIPTION_BYTES: usize = 8 * 1024;
pub(crate) const DOCUMENT_TAG_COUNT: usize = 64;
#[allow(dead_code)] // Consumed by transport-level metadata inheritance in CAP-ASYNC-01/02B.
pub(crate) const METADATA_TAG_COUNT: usize = 16;
pub(crate) const EFFECTIVE_TAG_COUNT: usize = 32;
pub(crate) const TAG_NAME_BYTES: usize = 64;
pub(crate) const TAG_DESCRIPTION_BYTES: usize = 2 * 1024;
pub(crate) const SECURITY_SCHEME_COUNT: usize = 32;
#[allow(dead_code)] // Consumed by transport-level metadata inheritance in CAP-ASYNC-01/02B.
pub(crate) const METADATA_SECURITY_COUNT: usize = 16;
pub(crate) const EFFECTIVE_SECURITY_COUNT: usize = 32;
pub(crate) const SECURITY_NAME_BYTES: usize = 64;
pub(crate) const SECURITY_DESCRIPTION_BYTES: usize = 2 * 1024;
pub(crate) const API_KEY_PARAMETER_BYTES: usize = 128;
pub(crate) const HTTP_SCHEME_BYTES: usize = 64;
pub(crate) const BEARER_FORMAT_BYTES: usize = 64;
pub(crate) const URL_BYTES: usize = 2 * 1024;
pub(crate) const OAUTH_FLOW_COUNT: usize = 4;
pub(crate) const OAUTH_SCOPE_COUNT: usize = 64;
pub(crate) const REQUIRED_SCOPE_COUNT: usize = 32;
pub(crate) const SCOPE_NAME_BYTES: usize = 128;
pub(crate) const SCOPE_DESCRIPTION_BYTES: usize = 2 * 1024;
pub(crate) const EXAMPLE_COUNT: usize = 8;
pub(crate) const EXAMPLE_BYTES: usize = 16 * 1024;
pub(crate) const EXAMPLE_TOTAL_BYTES: usize = 64 * 1024;
pub(crate) const EXAMPLE_DEPTH: usize = 32;
pub(crate) const EXAMPLE_NODES: usize = 4096;
#[allow(dead_code)] // Consumed by the WebSocket projection in CAP-ASYNC-01.
pub(crate) const WEBSOCKET_ERROR_CODE_BYTES: usize = 64;
#[allow(dead_code)] // Consumed by the WebSocket projection in CAP-ASYNC-01.
pub(crate) const WEBSOCKET_PUBLIC_MESSAGE_BYTES: usize = 1024;
#[allow(dead_code)] // Consumed by the WebSocket projection in CAP-ASYNC-01.
pub(crate) const WEBSOCKET_ERROR_COUNT: usize = 32;
#[allow(dead_code)] // Consumed by the WebSocket projection in CAP-ASYNC-01.
pub(crate) const WEBSOCKET_ERROR_DESCRIPTION_BYTES: usize = 2 * 1024;
#[allow(dead_code)] // Consumed by the WebSocket projection in CAP-ASYNC-01.
pub(crate) const WEBSOCKET_CLOSE_REASON_BYTES: usize = 123;
pub(crate) const CONTENT_ENCODING_BYTES: usize = 64;
pub(crate) const WEBSOCKET_NAMESPACE_BYTES: usize = 128;
pub(crate) const WEBSOCKET_EVENT_BYTES: usize = 256;
pub(crate) const WEBSOCKET_CONTENT_TYPE_BYTES: usize = 128;
pub(crate) const WEBSOCKET_PROTOCOL_NAME_BYTES: usize = 64;
pub(crate) const WEBSOCKET_PROTOCOL_VERSION_BYTES: usize = 64;
pub(crate) const WEBSOCKET_SUBPROTOCOL_BYTES: usize = 128;
pub(crate) const WEBSOCKET_SUBPROTOCOL_COUNT: usize = 16;
pub(crate) const WEBSOCKET_WIRE_FORMAT_COUNT: usize = 2;
pub(crate) const WEBSOCKET_OUTCOME_COUNT: usize = 5;
pub(crate) const RABBITMQ_NAME_BYTES: usize = 255;
pub(crate) const RABBITMQ_RETRY_BUCKET_COUNT: usize = 32;
pub(crate) const RABBITMQ_RETRY_ATTEMPT_COUNT: usize = 100;
pub(crate) const RABBITMQ_RETRY_CANDIDATE_COUNT: usize = 3;
pub(crate) const LILY_EXTENSION_COUNT: usize = 6;
pub(crate) const LILY_EXTENSION_BYTES: usize = 16 * 1024;
pub(crate) const RABBITMQ_TOPOLOGY_EXTENSION_BYTES: usize = 64 * 1024;
pub(crate) const CHANNEL_COUNT: usize = 1024;
pub(crate) const OPERATION_COUNT: usize = 4096;
pub(crate) const MESSAGE_COUNT: usize = 4096;
pub(crate) const SCHEMA_COUNT: usize = 2048;
pub(crate) const DOCUMENT_BYTES: usize = 8 * 1024 * 1024;
