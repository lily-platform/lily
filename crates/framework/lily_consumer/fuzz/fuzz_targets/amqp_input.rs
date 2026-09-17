use arbitrary::Arbitrary;
use lily_queue::fuzzing::{AMQPValue, BasicProperties, FieldTable, ShortString};
use serde::Deserialize;

const MAX_TRANSLATED_HEADERS: usize = 80;
const MAX_TRANSLATED_NODES: usize = 512;
const MAX_TRANSLATED_DEPTH: usize = 6;

#[derive(Debug, Default, Deserialize, Arbitrary)]
#[serde(default)]
pub(crate) struct PropertiesInput {
    pub(crate) headers: Vec<HeaderInput>,
    pub(crate) content_type: Option<String>,
    pub(crate) message_id: Option<String>,
}

impl PropertiesInput {
    pub(crate) fn into_properties(self) -> BasicProperties {
        let mut nodes = MAX_TRANSLATED_NODES;
        let headers = table(self.headers, &mut nodes, 0);
        let mut properties = BasicProperties::default().with_headers(headers);
        if let Some(content_type) = self.content_type {
            properties = properties.with_content_type(short_string(content_type));
        }
        if let Some(message_id) = self.message_id {
            properties = properties.with_message_id(short_string(message_id));
        }
        properties
    }
}

#[derive(Debug, Default, Deserialize, Arbitrary)]
#[serde(default)]
pub(crate) struct HeaderInput {
    pub(crate) key: String,
    pub(crate) nodes: Vec<ValueNodeInput>,
}

/// Flat prefix-tree node. The Arbitrary decode itself is therefore
/// non-recursive; only the separately bounded translator walks nesting.
#[derive(Debug, Default, Deserialize, Arbitrary)]
#[serde(default)]
pub(crate) struct ValueNodeInput {
    pub(crate) kind: u8,
    pub(crate) children: u8,
    pub(crate) signed: i64,
    pub(crate) unsigned: u64,
    pub(crate) float: f64,
    pub(crate) text: String,
    pub(crate) bytes: Vec<u8>,
    pub(crate) key: String,
}

fn table(entries: Vec<HeaderInput>, nodes: &mut usize, depth: usize) -> FieldTable {
    let mut table = FieldTable::default();
    if depth > MAX_TRANSLATED_DEPTH {
        return table;
    }
    for entry in entries.into_iter().take(MAX_TRANSLATED_HEADERS) {
        if *nodes == 0 {
            break;
        }
        *nodes -= 1;
        let mut cursor = 0;
        table.insert(
            short_string(entry.key),
            value(&entry.nodes, &mut cursor, nodes, depth),
        );
    }
    table
}

fn value(
    input: &[ValueNodeInput],
    cursor: &mut usize,
    nodes: &mut usize,
    depth: usize,
) -> AMQPValue {
    if *nodes == 0 || depth > MAX_TRANSLATED_DEPTH {
        return AMQPValue::Void;
    }
    let Some(node) = input.get(*cursor) else {
        return AMQPValue::Void;
    };
    *cursor += 1;
    *nodes -= 1;

    match node.kind % 11 {
        0 => AMQPValue::Void,
        1 => AMQPValue::Boolean(node.unsigned & 1 == 1),
        2 => AMQPValue::LongInt(node.signed as i32),
        3 => AMQPValue::LongLongInt(node.signed),
        4 => AMQPValue::Timestamp(node.unsigned),
        5 => AMQPValue::Double(node.float),
        6 => AMQPValue::ShortString(short_string(node.text.clone())),
        7 => AMQPValue::LongString(node.text.clone().into()),
        8 => AMQPValue::ByteArray(node.bytes.clone().into()),
        9 => {
            let mut values = Vec::new();
            if depth < MAX_TRANSLATED_DEPTH {
                for _ in 0..usize::from(node.children.min(32)) {
                    if *cursor >= input.len() || *nodes == 0 {
                        break;
                    }
                    values.push(value(input, cursor, nodes, depth + 1));
                }
            }
            AMQPValue::FieldArray(values.into())
        }
        _ => {
            let mut values = FieldTable::default();
            if depth < MAX_TRANSLATED_DEPTH {
                for _ in 0..usize::from(node.children.min(32)) {
                    let Some(child) = input.get(*cursor) else {
                        break;
                    };
                    if *nodes == 0 {
                        break;
                    }
                    let key = child.key.clone();
                    values.insert(short_string(key), value(input, cursor, nodes, depth + 1));
                }
            }
            AMQPValue::FieldTable(values)
        }
    }
}

fn short_string(mut value: String) -> ShortString {
    while value.len() > 255 {
        value.pop();
    }
    ShortString::try_new(value).expect("fuzz AMQP short string is explicitly byte-bounded")
}
