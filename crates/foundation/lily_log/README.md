# Lily Log - OpenTelemetry ClickHouse Schema Management

OpenTelemetry verilerinin ClickHouse'da saklanması için Rust schema tanımları.

## 📋 Genel Bakış

Bu crate, OTEL Collector tarafından ClickHouse'a aktarılan OpenTelemetry verilerinin Rust struct karşılıklarını sağlar. Tüm OTEL tablolarının detaylı schema'larını içerir.

## 🗄️ OTEL Tabloları

### 1. **otel_traces** - Distributed Tracing
Dağıtık sistemlerde trace ve span verilerini saklar.

**Özellikler:**
- Trace ID ve Span ID ile hiyerarşik yapı
- Span duration ve status bilgileri
- Resource ve span attributes (key-value pairs)
- Events (span içindeki olaylar)
- Links (diğer span'lere bağlantılar)

**Kullanım:**
```rust
use lily_log::OtelTrace;

// ClickHouse'dan trace verisi çekildiğinde
let trace: OtelTrace = /* ... */;
println!("Service: {}, Duration: {}ms", 
    trace.service_name, 
    trace.duration as f64 / 1_000_000.0
);
```

### 2. **otel_logs** - Application Logs
Uygulama loglarını structured format'ta saklar.

**Özellikler:**
- Severity levels (DEBUG, INFO, WARN, ERROR, FATAL)
- Trace context (TraceId, SpanId)
- Resource, scope ve log attributes
- Kubernetes metadata (cluster, namespace, pod)

**Kullanım:**
```rust
use lily_log::OtelLog;

let log: OtelLog = /* ... */;
if log.severity_number >= 17 {
    println!("ERROR: {} - {}", log.service_name, log.body);
}
```

### 3. **otel_metrics_sum** - Sum/Counter Metrics
Kümülatif toplam ve counter metrikleri.

**Özellikler:**
- Monotonic counters (requests, bytes sent)
- Up/down counters (active connections, queue size)
- Aggregation temporality (DELTA, CUMULATIVE)
- Exemplars (trace context ile bağlantılı örnekler)

### 4. **otel_metrics_gauge** - Gauge Metrics
Anlık ölçüm değerleri (CPU, memory, temperature).

### 5. **otel_metrics_histogram** - Histogram Metrics
Dağılım metrikleri (latency, request size).

**Özellikler:**
- Bucket counts ve explicit bounds
- Min, max, sum, count
- Percentile hesaplamaları için veri

### 6. **otel_metrics_summary** - Summary Metrics
Quantile metrikleri (p50, p95, p99).

**Özellikler:**
- Pre-calculated quantiles
- Count ve sum değerleri

### 7. **otel_metrics_exponential_histogram** - Exponential Histogram
Exponential bucket'lı histogram metrikleri.

**Özellikler:**
- Dynamic bucket sizing
- Positive ve negative buckets
- Scale factor

## 📊 Database Schema

Tüm entity'ler ClickHouse'daki OTEL tablolarının birebir Rust karşılıklarıdır:

| Rust Struct | ClickHouse Table | Açıklama |
|------------|------------------|----------|
| `OtelTrace` | `otel.otel_traces` | Distributed tracing spans |
| `OtelLog` | `otel.otel_logs` | Application logs |
| `OtelMetricSum` | `otel.otel_metrics_sum` | Sum/counter metrics |
| `OtelMetricGauge` | `otel.otel_metrics_gauge` | Gauge metrics |
| `OtelMetricHistogram` | `otel.otel_metrics_histogram` | Histogram metrics |
| `OtelMetricSummary` | `otel.otel_metrics_summary` | Summary metrics |
| `OtelMetricExponentialHistogram` | `otel.otel_metrics_exponential_histogram` | Exponential histogram |

## 🔧 Kullanım

### Dependency Ekleme

```toml
[dependencies]
lily_log = { path = "../lily_log" }
clickhouse = "0.11"
serde = { version = "1.0", features = ["derive"] }
```

### ClickHouse Query Örneği

```rust
use lily_log::OtelTrace;
use clickhouse::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::default()
        .with_url("http://localhost:8123")
        .with_database("otel");

    // Son 100 trace'i çek
    let traces: Vec<OtelTrace> = client
        .query("SELECT * FROM otel_traces ORDER BY Timestamp DESC LIMIT 100")
        .fetch_all()
        .await?;

    for trace in traces {
        println!("Trace: {} - Service: {} - Duration: {}ms",
            trace.trace_id,
            trace.service_name,
            trace.duration as f64 / 1_000_000.0
        );
    }

    Ok(())
}
```

### Error Traces Bulma

```rust
use lily_log::OtelTrace;

async fn find_error_traces(client: &Client) -> Result<Vec<OtelTrace>, Error> {
    client
        .query("SELECT * FROM otel_traces WHERE StatusCode = 'ERROR' ORDER BY Timestamp DESC LIMIT 50")
        .fetch_all()
        .await
}
```

### Service Bazlı Log Analizi

```rust
use lily_log::OtelLog;

async fn analyze_service_logs(client: &Client, service: &str) -> Result<(), Error> {
    let logs: Vec<OtelLog> = client
        .query(&format!(
            "SELECT * FROM otel_logs WHERE ServiceName = '{}' AND SeverityNumber >= 17 LIMIT 100",
            service
        ))
        .fetch_all()
        .await?;

    let error_count = logs.len();
    println!("Service {} has {} errors", service, error_count);

    Ok(())
}
```

### Metrics Query

```rust
use lily_log::OtelMetricSum;

async fn get_request_metrics(client: &Client) -> Result<Vec<OtelMetricSum>, Error> {
    client
        .query("SELECT * FROM otel_metrics_sum WHERE MetricName = 'http.server.requests' ORDER BY TimeUnix DESC LIMIT 100")
        .fetch_all()
        .await
}
```

## 📝 Field Naming Convention

ClickHouse OTEL tabloları PascalCase field isimleri kullanır. Rust struct'larında `serde` rename ile eşleştirme yapılmıştır:

```rust
#[derive(Serialize, Deserialize)]
pub struct OtelTrace {
    #[serde(rename = "TraceId")]
    pub trace_id: String,  // Rust: snake_case, ClickHouse: PascalCase
    
    #[serde(rename = "ServiceName")]
    pub service_name: String,
    // ...
}
```

## 🎯 Özellikler

- ✅ **Tam OTEL Uyumluluğu**: Tüm OTEL Collector tablolarının karşılıkları
- ✅ **Type Safety**: Rust'ın güçlü tip sistemi ile veri güvenliği
- ✅ **Serde Integration**: JSON serialization/deserialization desteği
- ✅ **Zero Dependencies**: Sadece serde ve std::collections
- ✅ **Detaylı Dokümantasyon**: Her field için açıklama

## 🔍 Schema Detayları

### Trace Attributes

```rust
// Resource Attributes (service-level)
trace.resource_attributes.get("service.name");
trace.resource_attributes.get("deployment.environment");
trace.resource_attributes.get("host.name");

// Span Attributes (span-level)
trace.span_attributes.get("http.method");
trace.span_attributes.get("http.status_code");
trace.span_attributes.get("db.statement");
```

### Log Severity Levels

```rust
// SeverityNumber mapping
1-4   => TRACE
5-8   => DEBUG
9-12  => INFO
13-16 => WARN
17-20 => ERROR
21-24 => FATAL
```

### Metric Types

```rust
// Sum Metric
if metric.is_monotonic {
    println!("Counter metric: {}", metric.value);
} else {
    println!("UpDown counter: {}", metric.value);
}

// Histogram
let avg = histogram.sum / histogram.count as f64;
println!("Average: {}, Min: {}, Max: {}", avg, histogram.min, histogram.max);
```

## 🚀 Gelecek Geliştirmeler

- [ ] Repository pattern implementation
- [ ] Query builder utilities
- [ ] Aggregation helpers
- [ ] Time-series analysis functions
- [ ] Dashboard data preparation utilities

## 📚 Referanslar

- [OpenTelemetry Specification](https://opentelemetry.io/docs/specs/otel/)
- [ClickHouse OTEL Schema](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/clickhouseexporter)
- [OTEL Collector](https://opentelemetry.io/docs/collector/)

## 🤝 Katkıda Bulunma

Bu crate, Lily Framework'ün bir parçasıdır. Schema güncellemeleri ve yeni özellikler için PR gönderilebilir.

## 📄 Lisans

Lily Framework lisansı ile aynı.
