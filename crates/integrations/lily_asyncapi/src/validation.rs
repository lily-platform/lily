use std::{collections::BTreeMap, io};

use serde::Serialize;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use url::{Host, Url};

use crate::{
    AsyncApiBuildError,
    limits::{
        CONTENT_TYPE_BYTES, DOCUMENT_BYTES, EXAMPLE_BYTES, EXAMPLE_DEPTH, EXAMPLE_NODES,
        IDENTIFIER_BYTES, RUNTIME_IDENTITY_BYTES, SERVER_HOST_BYTES, SERVER_NAME_BYTES,
        SERVER_PATHNAME_BYTES, URL_BYTES,
    },
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TextKind {
    Token,
    Description,
}

pub(crate) fn validate_required_text<'a>(
    field: &'static str,
    value: &'a str,
    max_bytes: usize,
    kind: TextKind,
) -> Result<&'a str, AsyncApiBuildError> {
    if value.is_empty() {
        return Err(validation(field, "must not be empty"));
    }
    if value.trim() != value {
        return Err(validation(
            field,
            "must not have leading or trailing whitespace",
        ));
    }
    if value.len() > max_bytes {
        return Err(validation(
            field,
            format!(
                "must not exceed {max_bytes} UTF-8 bytes (received {})",
                value.len()
            ),
        ));
    }

    let invalid_control = value.chars().any(|character| {
        character.is_control()
            && !(kind == TextKind::Description && matches!(character, '\t' | '\r' | '\n'))
    });
    if invalid_control {
        return Err(validation(field, "contains a forbidden control character"));
    }

    Ok(value)
}

pub(crate) fn validate_required<'a>(
    field: &'static str,
    value: &'a str,
    max_bytes: usize,
) -> Result<&'a str, AsyncApiBuildError> {
    validate_required_text(field, value, max_bytes, TextKind::Token)
}

pub(crate) fn validate_optional_description<'a>(
    field: &'static str,
    value: Option<&'a str>,
    max_bytes: usize,
) -> Result<Option<&'a str>, AsyncApiBuildError> {
    validate_optional_text(field, value, max_bytes, TextKind::Description)
}

pub(crate) fn validate_optional_text<'a>(
    field: &'static str,
    value: Option<&'a str>,
    max_bytes: usize,
    kind: TextKind,
) -> Result<Option<&'a str>, AsyncApiBuildError> {
    value
        .map(|value| validate_required_text(field, value, max_bytes, kind))
        .transpose()
}

pub(crate) fn validate_count(
    field: &'static str,
    count: usize,
    maximum: usize,
) -> Result<(), AsyncApiBuildError> {
    if count > maximum {
        return Err(validation(
            field,
            format!("must contain at most {maximum} entries (received {count})"),
        ));
    }
    Ok(())
}

pub(crate) fn validate_server_key<'a>(
    field: &'static str,
    value: &'a str,
) -> Result<&'a str, AsyncApiBuildError> {
    validate_grammar(field, value, SERVER_NAME_BYTES, false)
}

pub(crate) fn validate_server_name<'a>(
    field: &'static str,
    value: &'a str,
) -> Result<&'a str, AsyncApiBuildError> {
    validate_server_key(field, value)
}

pub(crate) fn validate_component_key<'a>(
    field: &'static str,
    value: &'a str,
) -> Result<&'a str, AsyncApiBuildError> {
    validate_grammar(field, value, IDENTIFIER_BYTES, true)
}

pub(crate) fn validate_identifier<'a>(
    field: &'static str,
    value: &'a str,
) -> Result<&'a str, AsyncApiBuildError> {
    validate_component_key(field, value)
}

pub(crate) fn validate_component_identifier<'a>(
    field: &'static str,
    value: &'a str,
) -> Result<&'a str, AsyncApiBuildError> {
    validate_component_key(field, value)
}

pub(crate) fn validate_security_name<'a>(
    field: &'static str,
    value: &'a str,
) -> Result<&'a str, AsyncApiBuildError> {
    validate_grammar(field, value, crate::limits::SECURITY_NAME_BYTES, true)
}

fn validate_grammar<'a>(
    field: &'static str,
    value: &'a str,
    max_bytes: usize,
    allow_dot: bool,
) -> Result<&'a str, AsyncApiBuildError> {
    validate_required_text(field, value, max_bytes, TextKind::Token)?;
    if !value.bytes().all(|byte| {
        byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-' || (allow_dot && byte == b'.')
    }) {
        let grammar = if allow_dot {
            "ASCII letters, digits, '.', '_' and '-'"
        } else {
            "ASCII letters, digits, '_' and '-'"
        };
        return Err(validation(field, format!("must contain only {grammar}")));
    }
    Ok(value)
}

pub(crate) fn normalize_server_host(
    field: &'static str,
    value: &str,
) -> Result<String, AsyncApiBuildError> {
    validate_required_text(field, value, SERVER_HOST_BYTES, TextKind::Token)?;
    if value
        .bytes()
        .any(|byte| matches!(byte, b'{' | b'}' | b'/' | b'?' | b'#'))
    {
        return Err(validation(
            field,
            "must be a concrete host with an optional port, without a scheme, path or variables",
        ));
    }

    // A private, unknown scheme keeps an explicitly supplied port intact. A
    // known scheme such as `https` would silently erase its default port even
    // though AsyncAPI's `host` value owns that port.
    let parsed = Url::parse(&format!("lily-server://{value}/"))
        .map_err(|_| validation(field, "is not a valid concrete host with an optional port"))?;
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(validation(field, "must not contain user information"));
    }

    let host = parsed
        .host()
        .ok_or_else(|| validation(field, "must include a host"))?;
    let mut normalized = match host {
        Host::Domain(domain) => domain.to_ascii_lowercase(),
        Host::Ipv4(address) => address.to_string(),
        Host::Ipv6(address) => format!("[{address}]"),
    };
    if let Some(port) = parsed.port() {
        normalized.push(':');
        normalized.push_str(&port.to_string());
    }
    Ok(normalized)
}

#[allow(dead_code)] // Compatibility helper for the hidden transport contribution ABI.
pub(crate) fn validate_host(
    field: &'static str,
    value: &str,
) -> Result<String, AsyncApiBuildError> {
    normalize_server_host(field, value)
}

pub(crate) fn normalize_server_pathname(
    field: &'static str,
    value: &str,
) -> Result<String, AsyncApiBuildError> {
    validate_required_text(field, value, SERVER_PATHNAME_BYTES, TextKind::Token)?;
    if !value.starts_with('/') {
        return Err(validation(field, "must begin with '/'"));
    }
    if value
        .bytes()
        .any(|byte| matches!(byte, b'{' | b'}' | b'?' | b'#'))
    {
        return Err(validation(
            field,
            "must be a concrete pathname without variables, query or fragment",
        ));
    }
    Ok(value.to_owned())
}

#[allow(dead_code)] // Compatibility helper for the hidden transport contribution ABI.
pub(crate) fn validate_pathname(
    field: &'static str,
    value: &str,
) -> Result<String, AsyncApiBuildError> {
    normalize_server_pathname(field, value)
}

pub(crate) fn normalize_absolute_http_url(
    field: &'static str,
    value: &str,
) -> Result<String, AsyncApiBuildError> {
    validate_required_text(field, value, URL_BYTES, TextKind::Token)?;
    let mut parsed =
        Url::parse(value).map_err(|_| validation(field, "must be a valid absolute HTTP(S) URL"))?;
    if !matches!(parsed.scheme(), "http" | "https") || parsed.host().is_none() {
        return Err(validation(field, "must be an absolute HTTP(S) URL"));
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(validation(field, "must not contain credentials"));
    }
    if parsed.fragment().is_some() {
        return Err(validation(field, "must not contain a fragment"));
    }

    // An empty path and `/` identify the same absolute HTTP resource. Retaining
    // the URL crate's canonical slash prevents registration-order differences.
    parsed.set_fragment(None);
    Ok(parsed.to_string())
}

#[allow(dead_code)] // Compatibility helper for the hidden transport contribution ABI.
pub(crate) fn validate_absolute_http_url(
    field: &'static str,
    value: &str,
) -> Result<String, AsyncApiBuildError> {
    normalize_absolute_http_url(field, value)
}

pub(crate) fn normalize_mime(
    field: &'static str,
    value: &str,
) -> Result<String, AsyncApiBuildError> {
    validate_required_text(field, value, CONTENT_TYPE_BYTES, TextKind::Token)?;
    let parsed = value
        .parse::<mime::Mime>()
        .map_err(|_| validation(field, "must be a valid MIME type"))?;
    if parsed.type_() == mime::STAR || parsed.subtype() == mime::STAR {
        return Err(validation(field, "must be a concrete MIME type"));
    }
    Ok(parsed.to_string())
}

pub(crate) fn validate_mime(
    field: &'static str,
    value: &str,
) -> Result<String, AsyncApiBuildError> {
    normalize_mime(field, value)
}

pub(crate) fn canonicalize_json(value: &Value) -> Value {
    match value {
        Value::Object(object) => {
            let sorted = object
                .iter()
                .map(|(key, value)| (key.clone(), canonicalize_json(value)))
                .collect::<BTreeMap<_, _>>();
            Value::Object(sorted.into_iter().collect::<Map<_, _>>())
        }
        Value::Array(values) => Value::Array(values.iter().map(canonicalize_json).collect()),
        _ => value.clone(),
    }
}

pub(crate) fn normalize_json<T: Serialize + ?Sized>(
    value: &T,
) -> Result<Value, AsyncApiBuildError> {
    // The owning metadata object applies its own byte/depth/node budget. A
    // schema and a message example intentionally do not share one size limit.
    let value = serde_json::to_value(value).map_err(|error| AsyncApiBuildError::Serialization {
        detail: error.to_string(),
    })?;
    Ok(canonicalize_json(&value))
}

pub(crate) fn validate_example(value: &Value) -> Result<Value, AsyncApiBuildError> {
    validate_and_canonicalize_json(
        "message.example",
        value,
        EXAMPLE_BYTES,
        EXAMPLE_DEPTH,
        EXAMPLE_NODES,
    )
}

pub(crate) fn validate_and_canonicalize_json(
    field: &'static str,
    value: &Value,
    max_bytes: usize,
    max_depth: usize,
    max_nodes: usize,
) -> Result<Value, AsyncApiBuildError> {
    let mut nodes = 0_usize;
    inspect_json(field, value, 1, max_depth, &mut nodes, max_nodes)?;
    let canonical = canonicalize_json(value);
    let bytes = serde_json::to_vec(&canonical)
        .map_err(|error| validation(field, format!("could not be serialized: {error}")))?;
    if bytes.len() > max_bytes {
        return Err(validation(
            field,
            format!(
                "canonical JSON must not exceed {max_bytes} bytes (received {})",
                bytes.len()
            ),
        ));
    }
    Ok(canonical)
}

fn inspect_json(
    field: &'static str,
    value: &Value,
    depth: usize,
    max_depth: usize,
    nodes: &mut usize,
    max_nodes: usize,
) -> Result<(), AsyncApiBuildError> {
    if depth > max_depth {
        return Err(validation(
            field,
            format!("JSON nesting depth must not exceed {max_depth}"),
        ));
    }
    *nodes = nodes
        .checked_add(1)
        .ok_or_else(|| validation(field, "JSON node count overflowed"))?;
    if *nodes > max_nodes {
        return Err(validation(
            field,
            format!("JSON node count must not exceed {max_nodes}"),
        ));
    }

    match value {
        Value::Array(values) => {
            for value in values {
                inspect_json(field, value, depth + 1, max_depth, nodes, max_nodes)?;
            }
        }
        Value::Object(values) => {
            for value in values.values() {
                inspect_json(field, value, depth + 1, max_depth, nodes, max_nodes)?;
            }
        }
        _ => {}
    }
    Ok(())
}

#[allow(dead_code)] // Used by bounded transport-owned extension objects in later checkpoints.
pub(crate) fn canonical_json_bytes<T: Serialize + ?Sized>(
    field: &'static str,
    value: &T,
    max_bytes: usize,
) -> Result<Vec<u8>, AsyncApiBuildError> {
    // Key order does not change serialized byte length. This first pass stops
    // before allocating a second, normalized document representation.
    serialize_bounded(field, value, max_bytes)?;
    let value = serde_json::to_value(value).map_err(|error| AsyncApiBuildError::Serialization {
        detail: error.to_string(),
    })?;
    let canonical = canonicalize_json(&value);
    serialize_bounded(field, &canonical, max_bytes)
}

pub(crate) fn canonical_document_bytes<T: Serialize + ?Sized>(
    document: &T,
) -> Result<Vec<u8>, AsyncApiBuildError> {
    if let Err(error) = serialize_bounded("document", document, DOCUMENT_BYTES) {
        return match error {
            AsyncApiBuildError::Validation { .. } => Err(AsyncApiBuildError::DocumentTooLarge {
                actual: DOCUMENT_BYTES + 1,
                maximum: DOCUMENT_BYTES,
            }),
            error => Err(error),
        };
    }
    let value =
        serde_json::to_value(document).map_err(|error| AsyncApiBuildError::Serialization {
            detail: error.to_string(),
        })?;
    let canonical = canonicalize_json(&value);
    serialize_bounded("document", &canonical, DOCUMENT_BYTES).map_err(|error| match error {
        AsyncApiBuildError::Validation { .. } => AsyncApiBuildError::DocumentTooLarge {
            actual: DOCUMENT_BYTES + 1,
            maximum: DOCUMENT_BYTES,
        },
        error => error,
    })
}

fn serialize_bounded<T: Serialize + ?Sized>(
    field: &'static str,
    value: &T,
    maximum: usize,
) -> Result<Vec<u8>, AsyncApiBuildError> {
    let mut writer = BoundedWriter::new(maximum);
    match serde_json::to_writer(&mut writer, value) {
        Ok(()) => Ok(writer.bytes),
        Err(_error) if writer.exceeded => Err(validation(
            field,
            format!("canonical JSON must not exceed {maximum} bytes"),
        )),
        Err(error) => Err(AsyncApiBuildError::Serialization {
            detail: error.to_string(),
        }),
    }
}

struct BoundedWriter {
    bytes: Vec<u8>,
    maximum: usize,
    exceeded: bool,
}

impl BoundedWriter {
    fn new(maximum: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(maximum.min(16 * 1024)),
            maximum,
            exceeded: false,
        }
    }
}

impl io::Write for BoundedWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let remaining = self.maximum.saturating_sub(self.bytes.len());
        if buffer.len() > remaining {
            self.exceeded = true;
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "bounded JSON writer limit exceeded",
            ));
        }
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(crate) fn generated_key(
    prefix: &str,
    source_identity: &str,
) -> Result<String, AsyncApiBuildError> {
    validate_required_text(
        "generated_key.prefix",
        prefix,
        IDENTIFIER_BYTES - 64,
        TextKind::Token,
    )?;
    if !prefix
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(validation(
            "generated_key.prefix",
            "must contain only ASCII letters, digits, '.', '_' and '-'",
        ));
    }
    validate_required_text(
        "generated_key.source_identity",
        source_identity,
        RUNTIME_IDENTITY_BYTES,
        TextKind::Token,
    )?;

    let digest = Sha256::digest(source_identity.as_bytes());
    let key = format!("{prefix}{digest:x}");
    validate_component_key("generated_key", &key)?;
    Ok(key)
}

#[derive(Clone, Debug, Default)]
pub(crate) struct GeneratedKeyTracker {
    sources_by_digest: BTreeMap<String, String>,
}

impl GeneratedKeyTracker {
    pub(crate) fn generate(
        &mut self,
        prefix: &str,
        source_identity: &str,
    ) -> Result<String, AsyncApiBuildError> {
        let key = generated_key(prefix, source_identity)?;
        let digest = key
            .get(key.len().saturating_sub(64)..)
            .ok_or_else(|| validation("generated_key", "SHA-256 digest suffix is missing"))?;

        match self.sources_by_digest.get(digest) {
            Some(existing) if existing != source_identity => Err(validation(
                "generated_key",
                "SHA-256 key collision was detected for different source identities",
            )),
            Some(_) => Ok(key),
            None => {
                self.sources_by_digest
                    .insert(digest.to_owned(), source_identity.to_owned());
                Ok(key)
            }
        }
    }
}

fn validation(field: &'static str, detail: impl Into<String>) -> AsyncApiBuildError {
    AsyncApiBuildError::validation(field, detail)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn required_text_enforces_byte_bound_and_control_policy() {
        assert!(validate_required_text("title", "title", 5, TextKind::Token).is_ok());
        assert!(validate_required_text("title", " title", 16, TextKind::Token).is_err());
        assert!(validate_required_text("title", "x\n", 16, TextKind::Token).is_err());
        assert!(validate_required_text("description", "x\ny", 16, TextKind::Description).is_ok());
        assert!(validate_required_text("description", "éé", 3, TextKind::Description).is_err());
    }

    #[test]
    fn identifiers_use_the_frozen_grammars() {
        assert!(validate_server_key("server", "public_ws-1").is_ok());
        assert!(validate_server_key("server", "public.ws").is_err());
        assert!(validate_component_key("component", "orders.created-v2").is_ok());
        assert!(validate_component_key("component", "orders/created").is_err());
    }

    #[test]
    fn server_host_and_path_are_concrete_and_normalized() {
        assert_eq!(
            normalize_server_host("host", "EXAMPLE.com:8443").unwrap(),
            "example.com:8443"
        );
        assert_eq!(
            normalize_server_host("host", "[2001:db8::1]:443").unwrap(),
            "[2001:db8::1]:443"
        );
        assert!(normalize_server_host("host", "wss://example.com").is_err());
        assert!(normalize_server_host("host", "{tenant}.example.com").is_err());
        assert!(normalize_server_pathname("pathname", "/socket").is_ok());
        assert!(normalize_server_pathname("pathname", "socket").is_err());
        assert!(normalize_server_pathname("pathname", "/{tenant}").is_err());
    }

    #[test]
    fn absolute_http_urls_reject_credentials_and_fragments() {
        assert_eq!(
            normalize_absolute_http_url("url", "HTTPS://EXAMPLE.COM/oauth").unwrap(),
            "https://example.com/oauth"
        );
        assert!(normalize_absolute_http_url("url", "ftp://example.com/token").is_err());
        assert!(normalize_absolute_http_url("url", "https://user@example.com/token").is_err());
        assert!(normalize_absolute_http_url("url", "https://example.com/token#secret").is_err());
    }

    #[test]
    fn mime_is_parsed_and_wildcards_are_rejected() {
        assert_eq!(
            normalize_mime("mime", "application/json").unwrap(),
            "application/json"
        );
        assert!(normalize_mime("mime", "application/*").is_err());
        assert!(normalize_mime("mime", "not a mime").is_err());
    }

    #[test]
    fn canonical_json_sorts_every_object_without_reordering_arrays() {
        let value = json!({"z": {"b": 2, "a": 1}, "a": [{"y": 2, "x": 1}, 3]});
        let bytes = canonical_json_bytes("example", &value, 1024).unwrap();
        assert_eq!(
            String::from_utf8(bytes).unwrap(),
            r#"{"a":[{"x":1,"y":2},3],"z":{"a":1,"b":2}}"#
        );
    }

    #[test]
    fn json_limits_are_exact_and_fail_at_plus_one() {
        let exact_nodes = json!([null, null, null]);
        assert!(validate_and_canonicalize_json("example", &exact_nodes, 64, 2, 4).is_ok());
        assert!(validate_and_canonicalize_json("example", &exact_nodes, 64, 2, 3).is_err());

        let exact_depth = json!([[null]]);
        assert!(validate_and_canonicalize_json("example", &exact_depth, 64, 3, 3).is_ok());
        assert!(validate_and_canonicalize_json("example", &exact_depth, 64, 2, 3).is_err());

        let bytes = canonical_json_bytes("example", &json!("abcd"), 6).unwrap();
        assert_eq!(bytes.len(), 6);
        assert!(canonical_json_bytes("example", &json!("abcd"), 5).is_err());
    }

    #[test]
    fn generated_keys_are_full_sha256_stable_and_bounded() {
        let mut tracker = GeneratedKeyTracker::default();
        let key = tracker.generate("ws_", "orders.created").unwrap();
        assert_eq!(key.len(), 3 + 64);
        assert_eq!(key, tracker.generate("ws_", "orders.created").unwrap());
        assert_ne!(key, tracker.generate("ws_", "orders.updated").unwrap());
        assert!(tracker.generate(&"x".repeat(65), "orders.created").is_err());

        let collision_key = generated_key("ws_", "collision-source").unwrap();
        let collision_digest = collision_key[collision_key.len() - 64..].to_owned();
        tracker
            .sources_by_digest
            .insert(collision_digest, "different-source".to_owned());
        assert!(tracker.generate("ws_", "collision-source").is_err());
        assert!(
            tracker
                .generate("ws_", &"i".repeat(RUNTIME_IDENTITY_BYTES))
                .is_ok()
        );
        assert!(
            tracker
                .generate("ws_", &"i".repeat(RUNTIME_IDENTITY_BYTES + 1))
                .is_err()
        );
    }

    #[test]
    fn canonical_document_bound_is_exact() {
        let exact = "x".repeat(DOCUMENT_BYTES - 2);
        assert_eq!(
            canonical_document_bytes(&exact).unwrap().len(),
            DOCUMENT_BYTES
        );
        let over = "x".repeat(DOCUMENT_BYTES - 1);
        assert!(matches!(
            canonical_document_bytes(&over),
            Err(AsyncApiBuildError::DocumentTooLarge { .. })
        ));
    }

    #[test]
    fn content_type_bound_is_checked_before_parsing() {
        let prefix = "application/";
        let exact = format!("{prefix}{}", "a".repeat(CONTENT_TYPE_BYTES - prefix.len()));
        let over = format!(
            "{prefix}{}",
            "a".repeat(CONTENT_TYPE_BYTES + 1 - prefix.len())
        );
        assert!(normalize_mime("mime", &exact).is_ok());
        assert!(normalize_mime("mime", &over).is_err());
    }
}
