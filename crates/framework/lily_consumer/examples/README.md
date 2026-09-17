# Consumer examples

`basic_consumer.rs` is the canonical minimal worker. From the workspace root,
select its configuration explicitly:

```bash
LILY_CONFIG_PATH=crates/framework/lily_consumer/examples/lily.toml \
LILY_CONFIG_MODE=development \
cargo run -p lily_consumer --example basic_consumer
```

`multi_queue_consumer.rs` demonstrates several independent injectable handler
services. Its five queue definitions live in `lily.toml.multi`:

```bash
LILY_CONFIG_PATH=crates/framework/lily_consumer/examples/lily.toml.multi \
LILY_CONFIG_MODE=development \
cargo run -p lily_consumer --example multi_queue_consumer
```

The examples intentionally do not inspect the generated registry or manually
construct RabbitMQ engines. Handler discovery, per-delivery DI scopes and
shutdown belong to `Consumer`. Publishing test messages belongs to
`lily_queue_client` or another RabbitMQ publisher.

Important rules:

- `ConfigService` uses the selected explicit/default Lily config path; it does
  not search parent directories for a convenient file.
- Every configured physical queue needs at least one compiled handler. More
  than one may share it only when each `(schema_version, content_kind)` pair is
  unique; the process still opens one RabbitMQ consumer pipeline for the queue.
- Queue policy belongs to `[[rabbitmq.topology.queues]]`, not macro attributes.
- `concurrency`, `prefetch_count` and `delivery_buffer_capacity` are separate
  bounds.
- One process accepts at most 256 configured queues, 512 linked handlers and
  256 trace cells. Each retained descriptor is at most 1 KiB and the complete
  validated consumer plan retains at most 256 KiB of descriptor text.
- Handler errors are propagated to the runtime; examples do not discard
  `Consumer::run()` results.

See `CONSUMER_CONFIGURATION_GUIDE.md` and
`../../../integrations/lily_queue/QUEUE_TOPOLOGY_MIGRATION_V2.md` for the complete policy and
operator contract.
