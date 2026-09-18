# lily_clickhouse

Experimental, application-owned ClickHouse integration for Lilyrs.

```toml
[dependencies]
lily_clickhouse = "0.1.0"
```

The default feature is `single`. For named database cells, select
`default-features = false, features = ["factory"]` instead; the two profiles
cannot be combined. The facade alternatives are `lilyrs` features `clickhouse`
or `clickhouse-factory`, both imported through `lilyrs::clickhouse`.

`ClickhouseSchema`, `ClickhouseTable` and `ClickhouseRepository` are re-exported
here; a separate `lily_clickhouse_derive` dependency is not required. User-owned
Serde and `clickhouse::Row` derives still require their own dependencies.

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

Configure the `[clickhouse]` section in the host's Lily configuration file.
The integration remains experimental and is not part of the connected
HTTP/WebSocket/Consumer examples.

## Documentation and license

Full documentation and canonical application examples: [lilyrs.com](https://lilyrs.com).
Published API reference: [docs.rs/lily_clickhouse](https://docs.rs/lily_clickhouse).

Licensed under either [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
