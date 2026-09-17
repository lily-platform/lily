// =============================================================================
// Service Module - DI Integration Layer
// =============================================================================

#[cfg(any(feature = "single", feature = "factory"))]
pub(crate) mod websocket_client_service;

#[cfg(feature = "factory")]
pub(crate) mod websocket_client_factory;
