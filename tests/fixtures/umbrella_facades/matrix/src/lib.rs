//! An isolated Cargo graph for validating every public facade feature.

// These APIs must compile when each feature is selected in isolation.
#[cfg(feature = "cancellation")]
pub fn execution_was_cancelled(signal: &lily::cancellation::ExecutionCancellation) -> bool {
    signal.is_cancelled()
}

#[cfg(feature = "websocket-redis")]
pub use lily::websocket_redis::RedisWebSocketBackplane;

#[cfg(all(feature = "websocket", feature = "websocket-redis"))]
pub fn redis_backplane_builder() -> lily::websocket::WsAppBuilder {
    lily::websocket::WsAppBuilder::new("127.0.0.1:0")
        .backplane::<lily::websocket_redis::RedisWebSocketBackplane>(
            lily::websocket::BackplaneRequirement::Required,
        )
}
