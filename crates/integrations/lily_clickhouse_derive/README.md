# lily_clickhouse_derive

Procedural derives for Lilyrs's experimental ClickHouse integration. Applications
import the macros from `lily_clickhouse` or `lilyrs::clickhouse`, without adding
this implementation package separately.

```toml
[dependencies]
lily_clickhouse = "0.1.0"
```

| Derive | Contract |
| --- | --- |
| `ClickhouseSchema` | Entity schema metadata, validated column types, table and ordering |
| `ClickhouseTable` | Typed table methods, migration DDL and `ServiceTrait` lifecycle |
| `ClickhouseRepository` | Optional delegation to a table, plus `ServiceTrait` lifecycle |

`ClickhouseSchema` accepts `#[clickhouse(table = "events", order_by = "id")]`.
The ordering must name declared columns; the supported engine is `MergeTree()`.
Field-level `#[clickhouse(type = "...")]` overrides are validated.

`ClickhouseTable` requires `#[entity_type(Entity)]` and a `db` field. A named
factory table additionally declares `#[cell_name("analytics")]`, injects
`clickhouse_factory: Arc<ClickhouseFactory>` and retains an unannotated
`db: Arc<DatabaseService>` for initialization.

`ClickhouseRepository` requires `#[table_type(Table)]`, `#[entity_type(Entity)]`
and a field named `table`, normally `Arc<Table>`. A service may instead use the
generated table directly. Both adapter derives implement `ServiceTrait`; do not
add a conflicting manual implementation. `Injectable` is a separate DI derive.

Select `single` (default) or `factory` on `lily_clickhouse`; disable the runtime's
default features for factory mode. Generated migration statements must be
applied explicitly; DI startup does not automatically create application tables.

## Documentation and license

Full documentation and canonical application examples: [lilyrs.com](https://lilyrs.com).
Published API reference: [docs.rs/lily_clickhouse_derive](https://docs.rs/lily_clickhouse_derive).

Licensed under either [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
