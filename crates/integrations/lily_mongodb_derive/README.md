# lily_mongodb_derive

Procedural derives for Lilyrs's MongoDB collection, repository and DTO-service
adapters. Application code imports these macros from `lily_mongodb` or
`lilyrs::mongodb`; a separate dependency on this implementation crate is unnecessary.

| Derive | Required metadata | Generated responsibility |
| --- | --- | --- |
| `MongoCollection` | `#[collection_type(Entity)]` | Typed collection CRUD, migration steps, `ServiceTrait` |
| `Repository` | `#[entity_type(Entity)]`, `#[collection_type(Adapter)]` | `MongoRepository<Entity>` delegation |
| `CrudService` | `#[entity_type(Entity)]`, `#[dto_type(Dto)]`, `#[repository_type(Repository)]` | `BaseService<Dto, Entity>` delegation and conversion |

## Collection shape

A collection has `db: Arc<DatabaseService>` and
`collection: Option<Collection<Entity>>` fields. In single mode, mark `db` with
`#[inject]`. Factory mode additionally injects
`mongo_factory: Arc<MongoFactory>`, declares `#[cell_name("...")]`, and leaves
`db` as runtime state. Select the corresponding mode on `lily_mongodb`.

`#[collection("name")]` overrides the default lower-case struct name.
`#[index(field = "created_at")]`, `#[unique_field("email")]` and
`#[unique_combination("tenant_id", "slug")]` produce validated migration steps;
they do not create indexes during startup. A unique combination accepts 2–8 fields.

## Repository and service composition

The repository is optional: services can call generated collection methods
directly. A repository uses a named `Arc<CollectionAdapter>` field whose name
contains `collection`. A CRUD service similarly uses an `Arc<Repository>` field
whose name contains `repository`, with `From` conversions in both DTO/entity
directions. Optional `#[gateway(GatewayType)]` notifications are best effort.

`Injectable` is a separate DI concern. Collections already receive `ServiceTrait`
from their derive; repositories and CRUD services must implement it themselves.
`CrudService` creates detached operation contexts, so custom methods are needed
for caller cancellation, deadlines and transaction propagation.

Runtime facade discovery supports Cargo-renamed dependencies. The macro support
paths remove the need to add registry, trace or helper dependencies solely for
generated code. Runtime-dependent contract tests live in the repository's
unpublished `tests/fixtures/macro_contracts/mongodb` package.

## Documentation and license

Full documentation and canonical application examples: [lilyrs.com](https://lilyrs.com).
Published API reference: [docs.rs/lily_mongodb_derive](https://docs.rs/lily_mongodb_derive).

Licensed under either [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
