# lily_asyncapi

`lily_asyncapi` is Lily's transport-neutral AsyncAPI 3.1 document foundation.
It owns the bounded configuration model, deterministic document projection,
Draft 7 schema registry, and immutable attach-once snapshot service shared by
the WebSocket and RabbitMQ composition roots.

Application code normally obtains an `AsyncApiService<K>` from Lily's
dependency-injection container and reads its immutable snapshot. Transport
crates opt in to this crate through their own `asyncapi` Cargo features; the
crate does not expose a general-purpose operation builder, HTTP endpoint, file
exporter, broker topology deployment, or network side effect.

The generated subset targets exactly AsyncAPI `3.1.0`. WebSocket and AMQP
bindings are supplied later by the respective transport adapters.
