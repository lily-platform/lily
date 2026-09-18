# lily_postgresql_derive

Implementation of Lilyrs's optional `PgRepository` derive. Applications import
it from `lily_postgresql::PgRepository` or `lilyrs::postgresql::PgRepository`;
the runtime facade includes this macro package.

## Entity and repository contract

`#[pg(entity = Entity)]` is the supported derive option. The entity remains a
normal Diesel model with the query, identifier, insert and changeset capabilities
required by the generated methods. The derive does not infer table definitions,
replace Diesel's query DSL or register the repository with DI.

In single mode, the named-field repository contains exactly one
`Arc<PgDatabaseService>`; its field name is not significant. Factory mode uses
one `Arc<PgFactory>` plus one `OnceLock<Arc<PgDatabaseService>>`. The application's
`ServiceTrait::initialize` chooses the named database and fills that lock.

For container-managed repositories, separately derive `Injectable`, mark the
dependency field with `#[inject]`, and implement `ServiceTrait`. Additional
runtime fields require `Default`. Do not inject the factory's runtime lock.

## Generated API

The derive implements the runtime `PgRepository` trait and generates `create`,
`create_many`, `find_by_id`, `find_by_ids`, `update`, `delete_by_id`, `count` and
`exists`. Diesel validates entity and ID types at compile time. Callers choose
their own batch limits.

Each operation also has an explicit-executor `*_in` method accepting `PgExecutor`
as its first argument after `&self`. Use those methods with an existing connection
when sharing a transaction. The ordinary methods acquire through the configured
database; they do not automatically enlist in `PgDbContext` transactions. For
implicit context sharing, write repository methods using that context's
`with_connection` API instead.

Cargo-renamed runtime dependencies are supported. This macro crate has no Cargo
features; database composition modes belong to `lily_postgresql`.

## Documentation and license

Full documentation and canonical application examples: [lilyrs.com](https://lilyrs.com).
Published API reference: [docs.rs/lily_postgresql_derive](https://docs.rs/lily_postgresql_derive).

Licensed under either [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
