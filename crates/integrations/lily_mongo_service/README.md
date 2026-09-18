# lily_mongo_service

DTO-facing CRUD contracts for Lilyrs's MongoDB service adapters. `BaseService`
is an optional conventional interface, not a base class required for every
application service.

Applications normally import `BaseService`, `BaseServiceError`,
`BaseServiceOperation` and the `CrudService` derive through `lily_mongodb` or
`lilyrs::mongodb`. A direct `lily_mongo_service = "0.1.0"` dependency can be used
for contract-only consumers.

## Using the contract

```rust
use lily_mongo_service::{BaseService, BaseServiceError};

struct UserDto;
struct UserEntity;

async fn load_user<S>(service: &S, id: &str) -> Result<UserDto, BaseServiceError>
where
    S: BaseService<UserDto, UserEntity>,
{
    service.find_by_id(id).await
}
```

Import the trait for method syntax on a generated service. DTOs cross its method
boundary; the entity parameter selects the persistence implementation. The
built-in `CrudService` derive uses `MongoRepository<Entity>`, ObjectId string IDs,
and `From` conversions in both directions between entity and DTO.

The derive implements CRUD, while `Injectable` and `ServiceTrait` separately
provide DI registration and lifecycle. Each generated CRUD call uses a detached
operation context. Write domain methods for shared transactions, cancellation,
deadlines or a different not-found policy. Optional gateway notifications are
best-effort post-write work and cannot roll back a completed database write.

This crate has no optional features and does not own a MongoDB client.

## Documentation and license

Full documentation and canonical application examples: [lilyrs.com](https://lilyrs.com).
Published API reference: [docs.rs/lily_mongo_service](https://docs.rs/lily_mongo_service).

Licensed under either [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
