//! An isolated Cargo graph for validating every public facade feature.

// These APIs must compile when each feature is selected in isolation.
#[cfg(feature = "cancellation")]
pub fn execution_was_cancelled(signal: &lilyrs::cancellation::ExecutionCancellation) -> bool {
    signal.is_cancelled()
}

#[cfg(feature = "websocket-redis")]
pub use lilyrs::websocket_redis::RedisWebSocketBackplane;

#[cfg(all(feature = "websocket", feature = "websocket-redis"))]
pub fn redis_backplane_builder() -> lilyrs::websocket::WsAppBuilder {
    lilyrs::websocket::WsAppBuilder::new("127.0.0.1:0")
        .backplane::<lilyrs::websocket_redis::RedisWebSocketBackplane>(
            lilyrs::websocket::BackplaneRequirement::Required,
        )
}
