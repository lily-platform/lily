#![doc = include_str!("../README.md")]

// Absolute paths work in generated code, integration tests and doctests alike.
extern crate self as lilyrs;

#[cfg(feature = "consumer")]
pub use lily_consumer as consumer;

#[cfg(feature = "http-api")]
pub use lily_http_api as http_api;

#[cfg(feature = "websocket")]
pub use lily_websocket as websocket;

#[cfg(feature = "websocket-redis")]
pub use lily_websocket_redis as websocket_redis;

#[cfg(feature = "__clickhouse")]
pub use lily_clickhouse as clickhouse;

#[cfg(feature = "__mongodb")]
pub use lily_mongodb as mongodb;

#[cfg(feature = "__postgresql")]
pub use lily_postgresql as postgresql;

#[cfg(feature = "queue")]
pub use lily_queue as queue;

#[cfg(feature = "__queue-client")]
pub use lily_queue_client as queue_client;

#[cfg(feature = "__redis")]
pub use lily_redis as redis;

#[cfg(any(feature = "trace", feature = "trace-console"))]
pub use lily_trace as trace;

#[cfg(feature = "config")]
pub use lily_config as config;

#[cfg(feature = "__websocket-client")]
pub use lily_websocket_client as websocket_client;

#[cfg(feature = "injection")]
pub use lily_injection as injection;

#[cfg(feature = "http-client")]
pub use lily_http_client as http_client;

#[cfg(feature = "error")]
pub use lily_error as error;

#[cfg(feature = "background-service")]
pub use lily_background_service as background_service;

#[cfg(feature = "cancellation")]
pub use lily_cancellation as cancellation;

/// Expansion support for derives re-exported by the application facades.
/// This module is an implementation detail, not an application import path.
#[doc(hidden)]
pub mod __private {
    #[cfg(feature = "__injection")]
    pub use lily_injection;
}
