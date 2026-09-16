//! Typed JSONL formatting. Span fields are retained as values, so formatting
//! an event never reparses a cached JSON string or splices serialized objects.

use std::{collections::BTreeMap, fmt, io};

use opentelemetry_sdk::trace::Tracer;
use serde::ser::{Serialize, SerializeMap, SerializeSeq, Serializer};
use serde_json::Value;
use tracing::{
    field::{Field, Visit},
    span::{Attributes, Id, Record},
    Event, Subscriber,
};
use tracing_log::NormalizeEvent;
use tracing_serde::fields::AsMap;
use tracing_subscriber::{
    field::RecordFields,
    fmt::{
        format::Writer,
        time::{FormatTime, SystemTime},
        FmtContext, FormatEvent, FormatFields,
    },
    layer::Context,
    registry::{LookupSpan, SpanRef},
    Layer,
};

use super::correlation::span_context;

#[derive(Default)]
struct Fields(BTreeMap<&'static str, Value>);

impl Visit for Fields {
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.0.insert(field.name(), value.into());
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.0.insert(field.name(), value.into());
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.insert(field.name(), value.into());
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.0.insert(field.name(), value.into());
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name(), value.into());
    }
    fn record_bytes(&mut self, field: &Field, value: &[u8]) {
        self.0.insert(field.name(), value.into());
    }
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if !field.name().starts_with("log.") {
            self.0.insert(
                field.name().strip_prefix("r#").unwrap_or(field.name()),
                format!("{value:?}").into(),
            );
        }
    }
}

pub(super) struct JsonFieldsLayer;

impl<S> Layer<S> for JsonFieldsLayer
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_new_span(&self, attributes: &Attributes<'_>, id: &Id, context: Context<'_, S>) {
        if let Some(span) = context.span(id) {
            let mut fields = Fields::default();
            attributes.record(&mut fields);
            span.extensions_mut().insert(fields);
        }
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, context: Context<'_, S>) {
        if let Some(span) = context.span(id) {
            if let Some(fields) = span.extensions_mut().get_mut::<Fields>() {
                values.record(fields);
            }
        }
    }
}

/// The adjacent JsonFieldsLayer owns typed span fields. Avoid also rendering
/// the unused FormattedFields string maintained by tracing-subscriber's layer.
pub(super) struct NoFields;

impl<'writer> FormatFields<'writer> for NoFields {
    fn format_fields<R: RecordFields>(&self, _: Writer<'writer>, _: R) -> fmt::Result {
        Ok(())
    }
}

pub(super) struct JsonFormat(pub(super) Tracer);

struct DisplayValue<T>(T);
impl<T: fmt::Display> Serialize for DisplayValue<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&self.0)
    }
}

struct JsonSpan<'a, 'b, S: LookupSpan<'a>>(&'b SpanRef<'a, S>);
impl<S> Serialize for JsonSpan<'_, '_, S>
where
    S: for<'lookup> LookupSpan<'lookup>,
{
    fn serialize<Ser: Serializer>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error> {
        let mut map = serializer.serialize_map(None)?;
        let extensions = self.0.extensions();
        if let Some(fields) = extensions.get::<Fields>() {
            for (key, value) in &fields.0 {
                // The canonical span name has one authoritative value.
                if *key != "name" {
                    map.serialize_entry(key, value)?;
                }
            }
        }
        map.serialize_entry("name", self.0.name())?;
        map.end()
    }
}

struct JsonScope<'a, 'b, S: LookupSpan<'a>>(&'b SpanRef<'a, S>);
impl<S> Serialize for JsonScope<'_, '_, S>
where
    S: for<'lookup> LookupSpan<'lookup>,
{
    fn serialize<Ser: Serializer>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error> {
        let mut sequence = serializer.serialize_seq(None)?;
        for span in self.0.scope().from_root() {
            sequence.serialize_element(&JsonSpan(&span))?;
        }
        sequence.end()
    }
}

struct JsonEventFields<'a, 'b>(&'a Event<'b>);

struct LastFieldVisitor<'a, S: SerializeMap> {
    inner: tracing_serde::SerdeMapVisitor<S>,
    fields: &'a tracing::field::FieldSet,
    recorded: &'a [bool],
}

impl<S: SerializeMap> LastFieldVisitor<'_, S> {
    fn is_last(&self, field: &Field) -> bool {
        !self.fields.iter().any(|other| {
            other.index() > field.index()
                && other.name() == field.name()
                && self.recorded[other.index()]
        })
    }
}

struct RecordedFields(Vec<bool>);
impl Visit for RecordedFields {
    fn record_debug(&mut self, field: &Field, _: &dyn fmt::Debug) {
        self.0[field.index()] = true;
    }
}

macro_rules! forward_last_field {
    ($($method:ident($value:ty)),* $(,)?) => {
        $(fn $method(&mut self, field: &Field, value: $value) {
            if self.is_last(field) {
                self.inner.$method(field, value);
            }
        })*
    };
}

impl<S: SerializeMap> Visit for LastFieldVisitor<'_, S> {
    forward_last_field! {
        record_f64(f64), record_i64(i64), record_u64(u64), record_bool(bool),
        record_str(&str), record_bytes(&[u8]), record_debug(&dyn fmt::Debug),
        record_i128(i128), record_u128(u128),
        record_error(&(dyn std::error::Error + 'static)),
    }
}

impl Serialize for JsonEventFields<'_, '_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let fields = self.0.metadata().fields();
        let repeated = fields.iter().enumerate().any(|(index, field)| {
            fields
                .iter()
                .take(index)
                .any(|prior| prior.name() == field.name())
        });
        if repeated {
            // Some SDK macros emit an implicit message and an explicit message
            // field. Match span recording and existing JSON consumers: the last
            // value wins, but the serialized object must contain the key once.
            // Stream directly into the bounded writer; do not first allocate a
            // map containing potentially oversized user values.
            // Empty/None values declare fields without recording them, so only
            // an actually recorded later value may replace an earlier one.
            let mut recorded = RecordedFields(vec![false; fields.len()]);
            self.0.record(&mut recorded);
            let mut visitor = LastFieldVisitor {
                inner: tracing_serde::SerdeMapVisitor::new(serializer.serialize_map(None)?),
                fields,
                recorded: &recorded.0,
            };
            self.0.record(&mut visitor);
            visitor.inner.finish()
        } else {
            self.0.field_map().serialize(serializer)
        }
    }
}

struct WriteAdapter<'a, 'b>(&'a mut Writer<'b>);
impl io::Write for WriteAdapter<'_, '_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let text = std::str::from_utf8(bytes).map_err(io::Error::other)?;
        self.0.write_str(text).map_err(io::Error::other)?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<S> FormatEvent<S, NoFields> for JsonFormat
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn format_event(
        &self,
        context: &FmtContext<'_, S, NoFields>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let normalized = event.normalized_metadata();
        let metadata = normalized.as_ref().unwrap_or_else(|| event.metadata());
        let parent = context.parent_span();
        let identity = parent.as_ref().and_then(|span| span_context(span, &self.0));
        let mut timestamp = String::new();
        SystemTime.format_time(&mut Writer::new(&mut timestamp))?;
        let write = || -> Result<(), serde_json::Error> {
            let mut serializer = serde_json::Serializer::new(WriteAdapter(&mut writer));
            let mut map = serializer.serialize_map(None)?;
            map.serialize_entry("timestamp", &timestamp)?;
            map.serialize_entry("level", metadata.level().as_str())?;
            if let Some(identity) = identity {
                map.serialize_entry("trace_id", &DisplayValue(identity.trace_id()))?;
                map.serialize_entry("span_id", &DisplayValue(identity.span_id()))?;
                map.serialize_entry(
                    "trace_flags",
                    &DisplayValue(format_args!("{:02x}", identity.trace_flags())),
                )?;
            }
            // User fields remain nested, and cannot overwrite correlation keys.
            map.serialize_entry("fields", &JsonEventFields(event))?;
            map.serialize_entry("target", metadata.target())?;
            if let Some(parent) = &parent {
                map.serialize_entry("span", &JsonSpan(parent))?;
                map.serialize_entry("spans", &JsonScope(parent))?;
            }
            SerializeMap::end(map)
        };
        write().map_err(|_| fmt::Error)?;
        writeln!(writer)
    }
}
