# RabbitMQ Queue Topology v2 Migration

Topology `v2` replaces the single per-message-TTL retry queue with bounded
fixed-delay queues. It also declares explicit message-count and byte retention
limits with `reject-publish` on every framework-owned physical queue.

## Naming

For worker `orders` and queue `orders.created`:

```text
main exchange: orders
main queue: orders.created
retry exchange: orders.retry.v2
retry bucket: orders.created.retry.v2.<delay-ms>ms
dead-letter exchange: orders.dlx.v2
dead-letter queue: orders.created.dlq.v2
```

Custom dead-letter exchange/routing-key values remain caller-owned names. When
`retry_attempts = 0`, Lily declares no retry exchange or retry bucket.

## Required operator procedure

Lily never deletes, purges, drains, renames, or copies broker entities.

Before deploying topology v2 to a broker that contains v1 entities:

1. Stop publishers or otherwise quiesce the affected routing keys.
2. Drain the main, `.retry.v1`, and `.dlq.v1` queues according to the
   application's data policy.
3. Back up or export messages that must be retained.
4. Delete incompatible queues/exchanges through an operator-controlled change.
5. Deploy the v2 consumer and verify that every expected fixed-delay bucket is
   declared and bound.
6. Resume publishers only after consumer readiness is healthy.

The main queue keeps its public name. Adding or changing declaration-time
retention arguments on an existing queue can therefore produce a RabbitMQ
precondition failure. Lily reports that startup failure and does not attempt an
automatic destructive migration.

## Capacity calculation

`main_*` and `dead_letter_*` bounds each cover one physical queue.
`retry_bucket_*` bounds cover one physical delay bucket. Worst-case retry
capacity is therefore the configured per-bucket value multiplied by the number
of distinct delays. With `retry_jitter_ratio = 0.0` (the default), exponential
delays are exact and deduplicated at the cap. A finite explicit ratio within
`0.0..=0.5` adds at most the lower, nominal and upper predeclared delay for an
attempt; event ID plus retry attempt deterministically selects among them.
Lily permits at most 32 distinct buckets per logical queue and 4096 across one
compiled topology. Either overflow fails before broker I/O.

Jitter does not use RabbitMQ per-message expiration, a delayed-message plugin,
runtime queue creation or sleeping application tasks. Changing the jitter
ratio changes the declared physical bucket set and must therefore follow the
same operator-controlled topology migration procedure as any other immutable
queue declaration change.

Overflow is a fixed correctness invariant: `reject-publish`. A full main queue
NACKs the publisher confirm. A full retry/DLQ queue makes the confirmed handoff
fail; Lily requeues the original delivery and fails the supervised Consumer
runtime instead of dropping an old message or entering an unobserved loop.
