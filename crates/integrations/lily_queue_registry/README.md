# Lily Queue Registry

Internal link-time ABI shared by Lily's queue handler macros and consumer
composition root. It contains the borrowed type-erased invocation ABI,
application-owned middleware/guard constructor and hook vtables, explicit
typed delivery outcome, exact `(queue, schema version, content kind)` dispatch
keys, input metadata, and the distributed slice used for discovery. Each
handler metadata record is one supported contract; the consumer derives its
immutable local dispatch table from these records and rejects exact duplicate
keys before broker admission. Metadata also carries the database-neutral
`DeliveryGuarantee` selected by the handler. The registry records intent only;
it neither chooses a storage backend nor claims broker-level exactly-once
delivery.

This is not an application extension point. Applications import
`queue_service` and `queue` from `lily_queue`, configure queues in Lily config,
and let `lily_consumer` perform discovery. Public Rust visibility here exists
only because generated code is compiled in the downstream application crate.
