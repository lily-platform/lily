use serde::{Serialize, de::DeserializeOwned};

use crate::CacheError;

const MAGIC: &[u8; 4] = b"LYC1";
const VERSION: u8 = 1;
const JSON_KIND: u8 = 1;
const BINARY_KIND: u8 = 2;
const HEADER_LEN: usize = 6;

pub(crate) fn encode_json<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, CacheError> {
    let payload =
        serde_json::to_vec(value).map_err(|error| CacheError::Serialization(error.to_string()))?;
    Ok(encode(JSON_KIND, &payload))
}

pub(crate) fn encode_json_text(value: &str) -> Result<Vec<u8>, CacheError> {
    let value = serde_json::from_str::<serde_json::Value>(value)
        .unwrap_or_else(|_| serde_json::Value::String(value.to_owned()));
    encode_json(&value)
}

pub(crate) fn encode_bytes(value: &[u8]) -> Vec<u8> {
    encode(BINARY_KIND, value)
}

pub(crate) fn decode_json<T: DeserializeOwned>(value: &[u8]) -> Result<T, CacheError> {
    let payload = decode(value, JSON_KIND)?;
    serde_json::from_slice(payload).map_err(|error| CacheError::Serialization(error.to_string()))
}

pub(crate) fn decode_json_text(value: &[u8]) -> Result<String, CacheError> {
    let json: serde_json::Value = decode_json(value)?;
    match json {
        serde_json::Value::String(value) => Ok(value),
        value => serde_json::to_string(&value)
            .map_err(|error| CacheError::Serialization(error.to_string())),
    }
}

pub(crate) fn decode_bytes(value: &[u8]) -> Result<Vec<u8>, CacheError> {
    Ok(decode(value, BINARY_KIND)?.to_vec())
}

fn encode(kind: u8, payload: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(HEADER_LEN + payload.len());
    encoded.extend_from_slice(MAGIC);
    encoded.push(VERSION);
    encoded.push(kind);
    encoded.extend_from_slice(payload);
    encoded
}

fn decode(value: &[u8], expected_kind: u8) -> Result<&[u8], CacheError> {
    if value.len() < HEADER_LEN || &value[..4] != MAGIC {
        return Err(CacheError::IncompatiblePayload(
            "missing Lily cache envelope magic".into(),
        ));
    }
    if value[4] != VERSION {
        return Err(CacheError::IncompatiblePayload(format!(
            "unsupported envelope version {}",
            value[4]
        )));
    }
    if value[5] != expected_kind {
        return Err(CacheError::IncompatiblePayload(
            "payload kind does not match the requested operation".into(),
        ));
    }
    Ok(&value[HEADER_LEN..])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Serialize, serde::Deserialize, PartialEq, Eq)]
    struct Session {
        subject: String,
    }

    #[test]
    fn json_and_binary_round_trip_are_versioned_and_distinct() {
        let session = Session {
            subject: "user-1".into(),
        };
        let json = encode_json(&session).unwrap();
        assert_eq!(decode_json::<Session>(&json).unwrap(), session);

        let binary = encode_bytes(&[0, 1, 255]);
        assert_eq!(decode_bytes(&binary).unwrap(), vec![0, 1, 255]);
        assert!(matches!(
            decode_json::<Session>(&binary),
            Err(CacheError::IncompatiblePayload(_))
        ));
    }

    #[test]
    fn unknown_and_unenveloped_payloads_fail_closed() {
        let mut future = encode_json(&"value").unwrap();
        future[4] = 2;
        assert!(matches!(
            decode_json::<String>(&future),
            Err(CacheError::IncompatiblePayload(_))
        ));
        assert!(matches!(
            decode_json::<String>(b"legacy"),
            Err(CacheError::IncompatiblePayload(_))
        ));
    }
}
