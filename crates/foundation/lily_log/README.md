# lily_log

ClickHouse schemas, explicit migrations and bounded analytics queries for
OpenTelemetry-shaped logs, traces and metrics. This crate uses Lilyrs' experimental
ClickHouse integration; it is not a tracing subscriber, OTLP receiver or exporter.
Use `lily_trace` to collect application telemetry and configure ingestion separately.

```toml
[dependencies]
lily_log = "0.1.0"
```

There are no optional features and no `lilyrs` facade feature for this crate.
It uses `lily_clickhouse`'s default `single` DI profile. Configure the shared
ClickHouse database through Lily configuration before resolving `LogService`.
Initialization does not create tables or install a Collector.

## Schemas and repositories

The root and `schemas` module export an entity, table and repository for each
of these families:

| Entity | Table |
| --- | --- |
| `OtelTrace` | `otel_traces` |
| `OtelLog` | `otel_logs` |
| `OtelMetricSum` | `otel_metrics_sum` |
| `OtelMetricGauge` | `otel_metrics_gauge` |
| `OtelMetricHistogram` | `otel_metrics_histogram` |
| `OtelMetricSummary` | `otel_metrics_summary` |
| `OtelMetricExponentialHistogram` | `otel_metrics_exponential_histogram` |

For example, `OtelTraceTable` and `OtelTraceRepository` accompany `OtelTrace`.
Entities implement Serde, `clickhouse::Row` and generated schema metadata.
Attribute maps use vectors of key/value pairs, including nested vectors where
the ClickHouse schema requires them. Match the deployed ingestion schema to
these column names and types; this package does not negotiate a Collector schema.

## Scoped analytics queries

`LogService` is a DI singleton. Each query receives an application-authorized
`AnalyticsQuery`; the authorization scope is data passed to the query, not the
service's DI lifetime.

- `AnalyticsScope::new(tenant_id, allowed_services)` requires a tenant and
  optionally a nonempty service allowlist of at most 100 entries. `None` means
  all services in that tenant, so grant it only after application authorization.
  Tenant filtering uses the `tenant.id` resource attribute.
- `AnalyticsPageRequest::new(limit, cursor)` defaults to 1,000 rows, permits
  `1..=1000`, and bounds offsets at 100,000. Cursors use `v1:<offset>` and do not
  carry authorization.
- `AnalyticsQuery::new(scope, page, cancellation)` takes the operation's
  `lily_clickhouse::CancellationToken`. Add that dependency if constructing this
  token directly; no mutable global query context is used.
- Query inputs are bound values. Time windows are limited to 24 hours. Log and
  trace bounds are epoch milliseconds; metric bounds are epoch nanoseconds.

```rust
use lily_log::{AnalyticsQuery, ClickhouseError, LogService, OtelTrace};

async fn recent_traces(
    service: &LogService,
    query: &AnalyticsQuery,
    from_millis: i64,
    to_millis: i64,
) -> Result<Vec<OtelTrace>, ClickhouseError> {
    service
        .query_traces_by_service("orders", from_millis, to_millis, query)
        .await
}
```

The application authenticates the caller before constructing `AnalyticsScope`.
Neither a tenant string nor an allowlist authenticates a request by itself.

## Explicit schema deployment

```rust
use lily_log::{OtelRetentionPolicy, otel_schema_migrations};

let retention = OtelRetentionPolicy::new(30)?;
let migrations = otel_schema_migrations("analytics", retention)?;
assert!(!migrations.is_empty());
# Ok::<(), lily_log::ClickhouseError>(())
```

Retention accepts `1..=3650` days. The migration factory produces versioned DDL
for the seven partitioned tables; creating this plan performs no database I/O.
Apply it explicitly with `lily_clickhouse::ClickhouseMigrationRunner` from a
deployment command. The runner validates migration history and checksums;
ClickHouse DDL does not have transactional rollback.

## Documentation and license

Full documentation and canonical application examples: [lilyrs.com](https://lilyrs.com).
Published API reference: [docs.rs/lily_log](https://docs.rs/lily_log).

Licensed under either [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
