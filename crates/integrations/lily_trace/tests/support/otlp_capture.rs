//! Validate the Collector's OTLP JSON, independently of exporter success counters.

use serde::Serialize;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::Path,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Identity {
    pub trace_id: String,
    pub span_id: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct ExpectedMethod {
    pub case_id: u64,
    pub name: &'static str,
    pub lifecycle: &'static str,
    pub outcome: Option<&'static str>,
    pub code: Option<&'static str>,
    pub status: u64,
    pub identity: Identity,
    pub parent: Option<Identity>,
    pub min_ms: f64,
    pub max_ms: f64,
}

#[derive(Clone)]
pub struct Capture {
    pub spans: Vec<Value>,
    pub logs: Vec<Value>,
    pub metrics: Vec<Value>,
}

type Verification<T> = Result<T, String>;

fn check(condition: bool, message: impl Into<String>) -> Verification<()> {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}

fn attributes(record: &Value) -> Verification<BTreeMap<&str, &Value>> {
    let mut result = BTreeMap::new();
    for attribute in record["attributes"]
        .as_array()
        .ok_or("missing attributes array")?
    {
        let key = attribute["key"]
            .as_str()
            .ok_or("attribute key must be a string")?;
        check(
            result.insert(key, &attribute["value"]).is_none(),
            format!("duplicate attribute: {key}"),
        )?;
    }
    Ok(result)
}

fn string_attribute(record: &Value, key: &str, expected: Option<&str>) -> Verification<()> {
    let attrs = attributes(record)?;
    match expected {
        Some(expected) => check(
            attrs.get(key).and_then(|v| v["stringValue"].as_str()) == Some(expected),
            format!("{key} must equal {expected:?}"),
        ),
        None => check(!attrs.contains_key(key), format!("unexpected {key}")),
    }
}

fn duration(record: &Value) -> Verification<f64> {
    let duration = attributes(record)?
        .get("lily.duration_ms")
        .and_then(|value| value["doubleValue"].as_f64())
        .ok_or("duration must be doubleValue milliseconds")?;
    check(duration.is_finite() && duration >= 0.0, "invalid duration")?;
    Ok(duration)
}

fn timestamp(record: &Value, field: &str) -> Verification<u64> {
    let value = record[field]
        .as_str()
        .and_then(|v| v.parse().ok())
        .ok_or_else(|| format!("missing OTLP timestamp: {field}"))?;
    check(value > 0, format!("zero timestamp: {field}"))?;
    Ok(value)
}

fn omitted_or_zero(record: &Value, field: &str) -> Verification<()> {
    check(
        record
            .get(field)
            .is_none_or(|value| value.as_u64() == Some(0)),
        format!("nonzero or invalid {field}"),
    )
}

fn signal(
    path: &Path,
    resource_key: &str,
    scope_key: &str,
    record_key: &str,
    service: &str,
) -> Verification<Vec<Value>> {
    let source = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let mut records = Vec::new();
    for batch in serde_json::Deserializer::from_str(&source).into_iter::<Value>() {
        let batch = batch.map_err(|e| format!("invalid or truncated {}: {e}", path.display()))?;
        for resource in batch[resource_key]
            .as_array()
            .ok_or("missing OTLP resource array")?
        {
            string_attribute(&resource["resource"], "service.name", Some(service))?;
            for scope in resource[scope_key]
                .as_array()
                .ok_or("missing OTLP scope array")?
            {
                records.extend(
                    scope[record_key]
                        .as_array()
                        .ok_or("missing OTLP records")?
                        .iter()
                        .cloned(),
                );
            }
        }
    }
    Ok(records)
}

impl Capture {
    pub fn read(directory: &Path, service: &str) -> Verification<Self> {
        Ok(Self {
            spans: signal(
                &directory.join("traces.jsonl"),
                "resourceSpans",
                "scopeSpans",
                "spans",
                service,
            )?,
            logs: signal(
                &directory.join("logs.jsonl"),
                "resourceLogs",
                "scopeLogs",
                "logRecords",
                service,
            )?,
            metrics: signal(
                &directory.join("metrics.jsonl"),
                "resourceMetrics",
                "scopeMetrics",
                "metrics",
                service,
            )?,
        })
    }

    pub fn verify(&self, expected: &[ExpectedMethod], secret: &str) -> Verification<Value> {
        check(!expected.is_empty(), "an empty workload cannot qualify")?;
        let parents: HashSet<_> = expected
            .iter()
            .filter_map(|method| method.parent.as_ref().map(|v| v.span_id.as_str()))
            .collect();
        check(
            self.spans.len() == expected.len() + parents.len(),
            "missing or extra spans",
        )?;
        check(
            self.logs.len() == expected.len() * 2,
            "missing or extra lifecycle logs",
        )?;
        let mut by_id = HashMap::new();
        for span in &self.spans {
            let id = span["spanId"].as_str().ok_or("missing spanId")?;
            check(by_id.insert(id, span).is_none(), "duplicate exported span")?;
            for field in [
                "droppedAttributesCount",
                "droppedEventsCount",
                "droppedLinksCount",
            ] {
                omitted_or_zero(span, field)?;
            }
        }
        let mut expected_ids = HashSet::new();
        let mut validated_logs = HashSet::new();
        let mut methods = Vec::new();
        for method in expected {
            check(
                expected_ids.insert(&method.identity.span_id),
                "duplicate expected identity",
            )?;
            let span = by_id
                .get(method.identity.span_id.as_str())
                .ok_or("method span identity missing")?;
            let context = || format!("case {} ({})", method.case_id, method.name);
            check(
                span["traceId"] == method.identity.trace_id,
                format!("{}: wrong trace ID", context()),
            )?;
            check(
                span["name"] == method.name,
                format!("{}: wrong span name", context()),
            )?;
            check(
                span["flags"].as_u64().is_some_and(|flags| flags & 1 == 1),
                "span must be sampled",
            )?;
            string_attribute(span, "lily.instrumentation", Some("method"))?;
            string_attribute(span, "case_id", Some(&method.case_id.to_string()))?;
            string_attribute(span, "lily.lifecycle", Some(method.lifecycle))?;
            string_attribute(span, "lily.outcome", method.outcome)?;
            string_attribute(span, "lily.error_code", method.code)?;
            let status = span["status"].get("code").map_or(Some(0), Value::as_u64);
            check(
                status == Some(method.status),
                format!("{}: incorrect OTel status", context()),
            )?;
            let start = timestamp(span, "startTimeUnixNano")?;
            let end = timestamp(span, "endTimeUnixNano")?;
            check(end >= start, "span ends before it starts")?;
            let measured = duration(span)?;
            check(
                measured >= method.min_ms && measured <= method.max_ms,
                format!(
                    "{}: duration {measured}ms is outside measured caller bounds [{}, {}]",
                    context(),
                    method.min_ms,
                    method.max_ms
                ),
            )?;
            if let Some(parent) = &method.parent {
                let parent_span = by_id
                    .get(parent.span_id.as_str())
                    .ok_or("request parent missing")?;
                check(
                    span["parentSpanId"] == parent.span_id && span["traceId"] == parent.trace_id,
                    "method attached to wrong request",
                )?;
                check(
                    parent_span["traceId"] == parent.trace_id
                        && parent_span["name"] == "qualification.live.request",
                    "incorrect exported request parent",
                )?;
                check(
                    parent_span
                        .get("events")
                        .is_none_or(|v| v.as_array().is_some_and(Vec::is_empty)),
                    "manual request acquired method events",
                )?;
                check(
                    timestamp(parent_span, "startTimeUnixNano")? <= start
                        && timestamp(parent_span, "endTimeUnixNano")? >= end,
                    "method lies outside its parent time range",
                )?;
            } else {
                check(
                    span.get("parentSpanId")
                        .is_none_or(|v| v.as_str() == Some("")),
                    "root method acquired an unrelated parent",
                )?;
            }
            let events = span["events"]
                .as_array()
                .ok_or("method span has no events")?;
            check(
                events.len() == 2,
                "method must have exactly two span events",
            )?;
            let logs: Vec<_> = self
                .logs
                .iter()
                .enumerate()
                .filter(|(_, log)| log["spanId"] == method.identity.span_id)
                .collect();
            check(
                logs.len() == 2,
                "method must have exactly two correlated logs",
            )?;
            let mut last_event_time = start;
            let mut last_log_time = start;
            for (index, phase) in ["started", method.lifecycle].iter().enumerate() {
                let event = &events[index];
                let (log_index, log) = logs[index];
                check(
                    validated_logs.insert(log_index),
                    "log belongs to multiple methods",
                )?;
                check(
                    log["traceId"] == method.identity.trace_id,
                    "log trace ID differs from its span",
                )?;
                check(
                    log["flags"].as_u64().is_some_and(|flags| flags & 1 == 1),
                    "log lost its sampling flag",
                )?;
                omitted_or_zero(event, "droppedAttributesCount")?;
                omitted_or_zero(log, "droppedAttributesCount")?;
                let event_time = timestamp(event, "timeUnixNano")?;
                let log_time = timestamp(log, "observedTimeUnixNano")?;
                check(
                    event_time >= last_event_time && event_time <= end,
                    "unordered or unscoped event timestamp",
                )?;
                check(
                    log_time >= last_log_time && log_time <= end,
                    "unordered or unscoped log timestamp",
                )?;
                last_event_time = event_time;
                last_log_time = log_time;
                for record in [event, log] {
                    string_attribute(record, "lily.operation", Some(method.name))?;
                    string_attribute(record, "lily.lifecycle", Some(phase))?;
                    string_attribute(
                        record,
                        "lily.outcome",
                        if index == 0 { None } else { method.outcome },
                    )?;
                    string_attribute(
                        record,
                        "lily.error_code",
                        if index == 0 { None } else { method.code },
                    )?;
                    if index == 0 {
                        check(
                            !attributes(record)?.contains_key("lily.duration_ms"),
                            "started event already has a duration",
                        )?;
                    } else {
                        check(
                            duration(record)? == measured,
                            "span, event and log durations differ",
                        )?;
                    }
                }
                string_attribute(log, "target", Some("live_otlp_qualification"))?;
                let message = if index == 0 {
                    "method started"
                } else {
                    "method finished"
                };
                check(
                    event["name"] == message && log["body"]["stringValue"] == message,
                    "unexpected lifecycle message",
                )?;
            }
            methods.push(
                json!({"case_id": method.case_id, "lifecycle": method.lifecycle,
                "outcome": method.outcome, "error_code": method.code, "duration_ms": measured,
                "trace_id": method.identity.trace_id, "span_id": method.identity.span_id}),
            );
        }
        check(
            validated_logs.len() == self.logs.len(),
            "orphan lifecycle logs",
        )?;
        let serialized = serde_json::to_string(&(&self.spans, &self.logs, &self.metrics))
            .map_err(|e| e.to_string())?;
        check(
            !serialized.contains(secret),
            "application return/error payload leaked into telemetry",
        )?;
        // The SDK may emit cumulative metric snapshots at flush and shutdown.
        let probe: Vec<_> = self
            .metrics
            .iter()
            .filter(|v| v["name"] == "lily.qualification.methods")
            .collect();
        check(
            !probe.is_empty(),
            "shutdown did not export the metric probe",
        )?;
        for metric in probe {
            let points = metric["sum"]["dataPoints"]
                .as_array()
                .ok_or("missing counter data points")?;
            check(points.len() == 1, "unexpected metric series")?;
            check(
                points[0]["asInt"]
                    .as_str()
                    .and_then(|v| v.parse::<usize>().ok())
                    == Some(expected.len()),
                "counter was not fully flushed",
            )?;
        }
        Ok(
            json!({"status": "passed", "spans": self.spans.len(), "logs": self.logs.len(),
            "method_cases": methods, "metrics_probe": "passed"}),
        )
    }

    pub fn negative_controls(
        &self,
        expected: &[ExpectedMethod],
        secret: &str,
    ) -> Verification<Vec<&'static str>> {
        // Corrupt real received payloads. Every mutation must be rejected by
        // the same validator that accepted the unmodified Collector output.
        let mut passed = Vec::new();
        for name in [
            "lost_log",
            "duplicate_log",
            "duplicate_span",
            "duplicate_attribute",
            "wrong_trace_id",
            "wrong_error_code",
            "wrong_outcome",
            "wrong_duration",
            "wrong_status",
            "missing_span_event",
            "dropped_attributes",
            "payload_leak",
            "wrong_metric",
        ] {
            let mut bad = self.clone();
            match name {
                "lost_log" => {
                    bad.logs.pop();
                }
                "duplicate_log" => bad.logs.push(bad.logs[0].clone()),
                "duplicate_span" => bad.spans[0] = bad.spans[1].clone(),
                "duplicate_attribute" => {
                    let span = bad
                        .spans
                        .iter_mut()
                        .find(|v| v["spanId"] == expected[0].identity.span_id)
                        .unwrap();
                    span["attributes"].as_array_mut().unwrap().push(json!({
                        "key": "lily.lifecycle", "value": {"stringValue": "started"}
                    }));
                }
                "wrong_trace_id" => {
                    bad.logs[0]["traceId"] = json!("00000000000000000000000000000000")
                }
                "wrong_error_code" | "wrong_outcome" | "wrong_duration" => {
                    let key = match name {
                        "wrong_error_code" => "lily.error_code",
                        "wrong_outcome" => "lily.outcome",
                        _ => "lily.duration_ms",
                    };
                    let attribute = bad
                        .logs
                        .iter_mut()
                        .flat_map(|v| v["attributes"].as_array_mut().unwrap())
                        .find(|v| v["key"] == key)
                        .unwrap();
                    attribute["value"] = if name == "wrong_duration" {
                        json!({"doubleValue": -1.0})
                    } else {
                        json!({"stringValue": "incorrect"})
                    };
                }
                "wrong_status" => {
                    let span = bad
                        .spans
                        .iter_mut()
                        .find(|v| v["spanId"] == expected[0].identity.span_id)
                        .unwrap();
                    span["status"]["code"] = json!(2);
                }
                "missing_span_event" => {
                    bad.spans
                        .iter_mut()
                        .find(|v| v.get("events").is_some())
                        .unwrap()["events"]
                        .as_array_mut()
                        .unwrap()
                        .pop();
                }
                "dropped_attributes" => bad.logs[0]["droppedAttributesCount"] = json!(1),
                "payload_leak" => bad.spans[0]["attributes"]
                    .as_array_mut()
                    .unwrap()
                    .push(json!({"key":"leaked", "value":{"stringValue":secret}})),
                "wrong_metric" => {
                    bad.metrics.clear();
                }
                _ => unreachable!(),
            }
            check(
                bad.verify(expected, secret).is_err(),
                format!("validator accepted corruption: {name}"),
            )?;
            passed.push(name);
        }
        Ok(passed)
    }
}
