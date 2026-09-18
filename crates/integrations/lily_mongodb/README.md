# lily_mongodb

MongoDB integration for Lilyrs: client lifecycle, typed collection and repository
adapters, operation contexts and explicit versioned migrations. Applications
can use generated collections directly, add the optional repository/service
layers, or use the driver collection handle for custom operations.

```toml
[dependencies]
lily_mongodb = "0.1.0"
lily_injection = "0.1.0"
serde = { version = "1", features = ["derive"] }
```

The umbrella path is `lilyrs::mongodb` with feature `mongodb`.
`MongoCollection`, `Repository`, `CrudService`, `MongoRepository`, bounded
query types and `BaseService` are all re-exported here. Additional derive,
repository and service implementation dependencies are unnecessary.

## A typed collection

```rust
use std::sync::Arc;
use lily_injection::Injectable;
use lily_mongodb::{bson::oid::ObjectId, Collection, DatabaseService, MongoCollection};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
struct User {
    #[serde(skip_serializing_if = "Option::is_none")]
    _id: Option<ObjectId>,
    name: String,
}

#[derive(Default, Injectable, MongoCollection)]
#[service(lifetime = "Singleton")]
#[collection("users")]
#[collection_type(User)]
struct Users {
    #[inject]
    db: Arc<DatabaseService>,
    collection: Option<Collection<User>>,
}
```

`MongoCollection` implements `ServiceTrait`; do not add it manually. It generates
typed CRUD methods and index migration steps. The optional `Repository` and
`CrudService` derives implement their respective data contracts; application
types still supply their DI lifecycle.

## Configuration and features

The default `single` feature registers `DatabaseService` as a singleton using
the validated `[database]` configuration. Startup connects and performs a
readiness ping. Named databases use `MongoFactory`:

```toml
lily_mongodb = { version = "0.1.0", default-features = false, features = ["factory"] }
```

`single` and `factory` are mutually exclusive. Factory collections inject
`mongo_factory: Arc<MongoFactory>`, declare `#[cell_name("name")]` and leave the
runtime `db` field unannotated. `factory-api` exposes factory composition support
without selecting its DI registration; `test-support` is for qualification code.

## Operations and migrations

Use `DatabaseService::operation_context` to carry the configured deadline and
caller cancellation. Attach a transaction explicitly when operations must share
one; generated `CrudService` convenience methods instead create detached contexts.
Validate transaction capability before admitting transactional workloads.

Collection index attributes only produce migration steps. Use
`MongoMigration::new` and `MongoMigrationRunner` to apply a deployment-owned,
versioned plan. `for_component` separates independently versioned storage from
the application's migration ledger. DI initialization does not execute DDL.
Transaction and migration-lease support requires MongoDB 4.2 or newer; deployment
topology must also support the transactions the application uses.

## Documentation and license

Full documentation and canonical application examples: [lilyrs.com](https://lilyrs.com).
Published API reference: [docs.rs/lily_mongodb](https://docs.rs/lily_mongodb).

Licensed under either [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
