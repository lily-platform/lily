# Lily WebSocket example

`server.rs` is the canonical standalone server example. It demonstrates the
public composition path only: derive a struct controller, declare its action
methods, build `WsApp`, and start the configured listener.

```bash
cargo run -p lily_websocket --example server
```

Connect to `ws://127.0.0.1:8080/ws?namespace=chat` with the `lily.v2`
subprotocol, then send a strict Lily v2 JSON envelope for `chat:send`.
`server.rs` decodes its payload through `Payload<SendMessage>` and returns an
explicit `NoReply` outcome after forwarding the typed value. Global controller
state does not exist: registration descriptors are immutable and the live
controller is constructed once inside the built app.
