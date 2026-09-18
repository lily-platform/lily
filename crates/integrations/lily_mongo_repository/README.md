# lily_mongo_repository

MongoDB-specific repository contracts used by Lilyrs's generated data layer.
These types preserve BSON semantics and bound query inputs; they are persistence
contracts rather than database-neutral controller DTOs.

Applications normally import these APIs from `lily_mongodb` or
`lilyrs::mongodb`. A direct `lily_mongo_repository = "0.1.0"` dependency is useful
when implementing the contract without the runtime facade.

## Public contracts

- `MongoRepository<T>` defines typed CRUD, bounded reads, counts and existence
  queries. `Repository` in `lily_mongodb` can generate a delegating implementation.
- `MongoDocumentId` validates ObjectId text; `MongoIdBatch` bounds ID lookups.
- `MongoFilter` rejects empty filters unless callers explicitly choose `all()`.
- `MongoPageRequest` and `MongoPage<T>` provide bounded pages.
- `MongoWriteBatch<T>` validates multi-document write inputs.
- `MongoOperationContext` carries cancellation, an optional deadline,
  transaction and expected revision.
- `MongoTransaction` shares an already-started session and provides explicit
  commit/abort operations; it does not implicitly make every repository call
  transactional.

The limits are 100 records per page, 100 IDs per lookup batch, 1,000 documents
per write batch and 64 KiB per BSON filter. Multi-step operations must explicitly
share their operation/transaction context. Prefer the context created by
`DatabaseService::operation_context` for the configured deadline; `detached()`
does not carry caller cancellation.

The optional repository layer is not required: a domain service can use a
`MongoCollection` adapter directly. Errors are exposed as `MongoRepositoryError`,
an alias of Lily's MongoDB error vocabulary. There are no optional features in
this contract crate, and it does not create a client or register DI services.

## Documentation and license

Full documentation and canonical application examples: [lilyrs.com](https://lilyrs.com).
Published API reference: [docs.rs/lily_mongo_repository](https://docs.rs/lily_mongo_repository).

Licensed under either [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
