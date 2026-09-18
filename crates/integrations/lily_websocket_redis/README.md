# lily_websocket_redis

Redis Pub/Sub adapter for Lily's transport-neutral WebSocket backplane SPI.

The crate is optional. Applications that do not depend on it carry no Redis
client or runtime cost.

```toml
[dependencies]
lily_websocket = "0.1.0"
lily_websocket_redis = "0.1.0"
```

With the facade, select both `websocket` and `websocket-redis` and import
`lilyrs::websocket` / `lilyrs::websocket_redis`. The `websocket-redis` feature
does not enable the facade's `websocket` feature automatically. The adapter
itself has no optional Cargo features.

```rust,ignore
use lily_websocket::{BackplaneRequirement, WsAppBuilder};
use lily_websocket_redis::RedisWebSocketBackplane;

let app = WsAppBuilder::new("127.0.0.1:8081")
    .backplane::<RedisWebSocketBackplane>(BackplaneRequirement::Required)
    .build()
    .await?;
```

The [WebSocket host setup](../../framework/lily_websocket/README.md) also
applies here, including the `LILY_CONFIG_PATH` / `LILY_CONFIG_MODE` bootstrap
pair. Supply Redis and its ACL configuration before building the app.

The adapter reads its only configuration authority from the application
`ConfigService`:

```toml
[websocket.backplane]
redis_url = "${secret:websocket.redis_url}"
use_tls = true
application_namespace = "orders-api"
environment_namespace = "production"
channel_namespace = "events"
publish_capacity = 64
ingress_capacity = 256
connection_timeout_millis = 5000
operation_timeout_millis = 2000
reconnect_initial_delay_millis = 100
reconnect_max_delay_millis = 5000
reconnect_jitter_ratio = 0.2
```

In production mode `use_tls = true`, a `rediss://` URL with an ACL
credential, and an authenticated Redis deployment are required. An optional
`custom_ca_bundle` must be an absolute canonical PEM certificate file. When
present it replaces, rather than extends, the adapter's public WebPKI trust
roots for that Redis connection.
Only `redis_url` may be an exact `${secret:...}` or `${file:...}` reference
resolved by the existing application-owned `SecretResolver`.
`custom_ca_bundle` is a direct filesystem path, not a secret placeholder. The
adapter never logs the URL, credentials, CA path, provider error text, payload
or private backplane frame.

The configured ACL identity is shared by the adapter's dedicated publisher
and subscriber connections. It must permit the RESP3 connection setup
(`HELLO` and `CLIENT SETINFO`), `SELECT` when the URL selects a non-zero
database, and the runtime commands `PING`, `PUBLISH`, `SUBSCRIBE` and
`UNSUBSCRIBE`. Channel access should be restricted to the exact private
channel pattern `&lily.websocket.v4.<environment>.<application>.<channel>`;
the adapter requires no Redis key permissions.

The `v4` segment is Lily's private routing-protocol boundary. Every node in
one logical deployment must run the same protocol generation and use the same
channel. Mixed protocol generations are intentionally isolated rather than
accepting a legacy global target; deploy this protocol
change as a coordinated cutover.

`BackplaneRequirement::Required` gates application readiness on both the
publisher and the first successful subscription. A runtime outage lowers
readiness without killing existing WebSocket connection loops or undoing
already completed node-local delivery. `Optional` converts startup failure to
an observable degraded local-only app. Both modes use bounded queues,
deadlines and exponential reconnect/resubscribe backoff. A publish admitted
during a known outage fails immediately and is not replayed after reconnect.
`publish_capacity` bounds queued requests in addition to the one Redis command
that may be in flight. `ingress_capacity` is applied independently to the
Redis-callback stage and the subscriber-to-Lily frame stage; saturation drops
the subscription and forces a fail-closed reconnect instead of blocking
transport health events.
Reconnect delay uses bounded downward jitter by default so a shared outage
does not make every node reconnect in lockstep. Set
`reconnect_jitter_ratio = 0.0` only when deterministic timing is explicitly
required.

The publisher is also probed while idle. A subscriber transport loss makes
the last publisher observation temporarily stale until an immediate bounded
probe succeeds, so required readiness cannot become healthy from a recovered
subscriber plus an unverified publisher connection.

Publisher and subscriber connections are dedicated to this adapter and never
reuse `lily_redis`. Redis infrastructure, ACLs, TLS endpoints, DNS/failover and
high availability remain deployment responsibilities.

One adapter instance uses one stable Redis URL. It does not implement native
Redis Cluster or Sentinel topology discovery and node routing. Deployments
that need Redis HA must expose a stable DNS/proxy/failover endpoint through
that URL.

The Redis client decodes a Pub/Sub bulk frame before Lily's adapter callback
can validate the payload. Lily therefore rejects empty or oversized payloads
before they enter the WebSocket backplane dispatcher, but it cannot claim a
pre-allocation memory bound against an untrusted Redis publisher. Keep the
channel private with Redis ACLs and TLS, and configure an appropriate Redis
`proto-max-bulk-len` for the deployment's trust boundary.

Redis Pub/Sub is online and non-durable. The adapter does not provide offline
delivery, history, replay, global recipient acknowledgements, exact global
ordering or exactly-once delivery. A publish acknowledgement means only that
Redis accepted the `PUBLISH` command.

## Documentation and license

Full documentation and canonical application examples: [lilyrs.com](https://lilyrs.com).
Published API reference: [docs.rs/lily_websocket_redis](https://docs.rs/lily_websocket_redis).

Licensed under either [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
