# lily_clickhouse

Application-owned ClickHouse integration for Lily Framework.

The crate provides:

- mutually exclusive `single` and `factory` composition profiles;
- certificate-verifying HTTPS or explicit private HTTP;
- bounded connection concurrency, connect/query deadlines and cancellation;
- LZ4 compression and paired credential validation;
- generated schema/identifier allowlists and bound query values;
- bounded page, predicate, `IN` and write-batch operations;
- explicit version/checksum migration runner with a deployment DDL lock.

```toml
[clickhouse]
mode = "single"
host = "clickhouse.internal"
port = 8443
database = "analytics"
username = "${secret:clickhouse_user}"
password = "${secret:clickhouse_password}"
pool_size = 16
connection_timeout_secs = 5
query_timeout_secs = 30
use_tls = true
compression_enabled = true
```

Schema metadata is declared on the entity:

```rust,ignore
#[derive(Clone, serde::Serialize, serde::Deserialize, clickhouse::Row,
         lily_clickhouse::ClickhouseSchema)]
#[clickhouse(table = "events", order_by = "tenant_id, timestamp", engine = "MergeTree()")]
struct Event {
    tenant_id: String,
    timestamp: i64,
    message: String,
}
```

`ClickhouseTable` and `ClickhouseRepository` generate bounded insert/select/equality/delete/count APIs. They do not expose raw SQL or run DDL during service initialization.

Schema changes belong to a deployment command:

```rust,ignore
let migration = lily_clickhouse::ClickhouseMigration::new(
    1,
    "create events",
    vec![EventTable::migration_sql("analytics")?],
)?;
let runner = lily_clickhouse::ClickhouseMigrationRunner::new(database, vec![migration])?;
runner.apply(&deployment_operation).await?;
```

ClickHouse DDL is not transactional. The runner detects migration history/checksum divergence but does not claim automatic rollback; use a reviewed forward-fix or restore procedure.

See `docs/data/clickhouse.md` for the full usage and qualification contract.
