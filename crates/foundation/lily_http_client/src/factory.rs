//! HTTP Client Factory
//!
//! Factory for managing multiple HTTP clients with different configurations.
//! Supports client caching and configuration-based initialization.

use crate::client::{
    ClientConfig, HttpClient, ProtocolPreference, ValidatedClientProfile,
    MAX_ADDITIONAL_CA_BUNDLE_BYTES,
};
use crate::error::HttpClientError;
use lily_config::{
    ConfigService, HttpClientConfig, HttpClientFactoryConfig, HttpClientProtocol,
};
use lily_error::application::http_api::HttpApiError;
use lily_error::injection::InjectionError;
use lily_injection::{Injectable, ServiceTrait};
use std::collections::{HashMap, HashSet};
use std::path::{Component, Path};
use std::sync::{Arc, RwLock};
use std::time::Duration;

/// HTTP Client Factory for managing multiple HTTP clients
///
/// The factory creates and caches HTTP clients based on configuration.
/// Each client is identified by a prefix (e.g., "payment_api", "user_api").
///
/// # Examples
///
/// ```rust,no_run
/// use lily_http_client::LilyHttpClientFactory;
///
/// async fn send_payment_request(
///     factory: &LilyHttpClientFactory,
/// ) -> Result<(), Box<dyn std::error::Error>> {
///     // The application container initializes the factory from lily.toml.
///     let payment_client = factory.get("payment_api")?;
///     let request = payment_client.get("/transactions")?.build()?;
///     let response = payment_client.execute(request).await?;
///     println!("status={}", response.status().as_u16());
///     Ok(())
/// }
/// ```
#[derive(Injectable, Default)]
#[service(lifetime = "Singleton")]
pub struct LilyHttpClientFactory {
    #[inject]
    /// Configuration service for reading settings
    config_service: Arc<ConfigService>,

    /// Cached HTTP clients by prefix
    clients: Arc<RwLock<HashMap<String, Arc<HttpClient>>>>,
    /// Immutable, eagerly validated named profiles. Client transports remain lazy.
    profiles: Arc<RwLock<Option<HashMap<String, ValidatedClientProfile>>>>,
}

impl LilyHttpClientFactory {
    /// Get an HTTP client by prefix
    ///
    /// If the client doesn't exist in cache, it will be created from configuration.
    ///
    /// # Arguments
    ///
    /// * `prefix` - Client identifier (e.g., "payment_api", "user_api")
    ///
    /// # Returns
    ///
    /// Returns an `Arc<HttpClient>` for the specified prefix.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The prefix is not found in configuration
    /// - Client creation fails
    #[lily_trace::lily_trace(name = "http_client_factory.get", skip(self), env = "development")]
    pub fn get(&self, prefix: &str) -> Result<Arc<HttpClient>, HttpApiError> {
        lily_trace::prelude::debug!("Getting HTTP client for prefix: {}", prefix);
        // Check if client exists in cache
        {
            let clients = self.clients.read().map_err(|_| {
                HttpApiError::StateError("HTTP client cache lock is poisoned".to_string())
            })?;
            if let Some(client) = clients.get(prefix) {
                lily_trace::prelude::debug!("HTTP client found in cache for prefix: {}", prefix);
                return Ok(Arc::clone(client));
            }
        }

        lily_trace::prelude::debug!(
            "HTTP client not in cache, creating new client for prefix: {}",
            prefix
        );
        // Client not in cache, create it
        self.create_and_cache_client(prefix)
    }

    /// Create a client from configuration and cache it
    #[lily_trace::lily_trace(
        name = "http_client_factory.create_and_cache_client",
        skip(self),
        env = "development"
    )]
    fn create_and_cache_client(&self, prefix: &str) -> Result<Arc<HttpClient>, HttpApiError> {
        lily_trace::prelude::debug!("Creating and caching HTTP client for prefix: {}", prefix);
        let mut clients = self.clients.write().map_err(|_| {
            HttpApiError::StateError("HTTP client cache lock is poisoned".to_string())
        })?;
        if let Some(client) = clients.get(prefix) {
            return Ok(Arc::clone(client));
        }

        let profile = {
            let profiles = self.profiles.read().map_err(|_| {
                HttpApiError::StateError("HTTP client factory profile lock is poisoned".to_string())
            })?;
            let profiles = profiles
                .as_ref()
                .ok_or_else(|| {
                    HttpClientError::Configuration(
                        "Factory not initialized. Call initialize() first.".to_string(),
                    )
                })
                .map_err(|error| HttpApiError::BadRequest(error.to_string()))?;
            profiles
                .get(prefix)
                .cloned()
                .ok_or_else(|| {
                    HttpClientError::Configuration(format!(
                        "HTTP client configuration not found for prefix: {prefix}"
                    ))
                })
                .map_err(|error| HttpApiError::BadRequest(error.to_string()))?
        };

        lily_trace::prelude::debug!("Creating HTTP client instance");
        let http_client = Arc::new(profile.build_client().map_err(|error| {
            HttpApiError::ConfigurationError(format!(
                "invalid HTTP client configuration for '{prefix}': {error}"
            ))
        })?);
        clients.insert(prefix.to_string(), Arc::clone(&http_client));
        lily_trace::prelude::debug!("HTTP client cached for prefix: {}", prefix);

        lily_trace::prelude::info!(
            "HTTP client created and cached successfully for prefix: {}",
            prefix
        );
        Ok(http_client)
    }

    /// Build ClientConfig from factory defaults and client-specific config
    fn build_client_config(
        &self,
        factory_config: &HttpClientFactoryConfig,
        client_config: &HttpClientConfig,
    ) -> Result<ClientConfig, HttpApiError> {
        let mut headers = crate::header::HeaderMap::new();

        // Set user agent
        let user_agent = client_config
            .user_agent
            .as_ref()
            .unwrap_or(&factory_config.user_agent);
        headers
            .insert("User-Agent", user_agent)
            .map_err(|error| HttpApiError::ConfigurationError(error.to_string()))?;

        // Add custom headers from client config
        if let Some(custom_headers) = &client_config.headers {
            let mut normalized_names = HashSet::with_capacity(custom_headers.len());
            for (key, value) in custom_headers {
                if key.eq_ignore_ascii_case("user-agent") {
                    return Err(HttpApiError::ConfigurationError(
                        "configure User-Agent through the user_agent field".to_string(),
                    ));
                }
                if !normalized_names.insert(key.to_ascii_lowercase()) {
                    return Err(HttpApiError::ConfigurationError(
                        "default header names must be unique ignoring ASCII case".to_string(),
                    ));
                }
                headers
                    .insert(key, value)
                    .map_err(|error| HttpApiError::ConfigurationError(error.to_string()))?;
            }
        }

        let protocol = match client_config.protocol.unwrap_or(factory_config.protocol) {
            HttpClientProtocol::Auto => ProtocolPreference::Auto,
            HttpClientProtocol::Http1 => ProtocolPreference::Http1Only,
            HttpClientProtocol::Http2 => ProtocolPreference::Http2Only,
        };
        let config = ClientConfig {
            base_address: Some(client_config.base_address.clone()),
            connect_timeout: Duration::from_secs(
                client_config
                    .connect_timeout_secs
                    .unwrap_or(factory_config.connect_timeout_secs),
            ),
            request_timeout: Duration::from_secs(
                client_config
                    .request_timeout_secs
                    .unwrap_or(factory_config.request_timeout_secs),
            ),
            max_redirects: client_config
                .max_redirects
                .unwrap_or(factory_config.max_redirects),
            default_headers: headers,
            protocol,
            max_in_flight_requests: client_config
                .max_in_flight_requests
                .unwrap_or(factory_config.max_in_flight_requests),
            max_in_flight_requests_per_origin: client_config
                .max_in_flight_requests_per_origin
                .unwrap_or(factory_config.max_in_flight_requests_per_origin),
            max_request_body_bytes: client_config
                .max_request_body_bytes
                .unwrap_or(factory_config.max_request_body_bytes),
            max_response_body_bytes: client_config
                .max_response_body_bytes
                .unwrap_or(factory_config.max_response_body_bytes),
            max_header_count: client_config
                .max_header_count
                .unwrap_or(factory_config.max_header_count),
            max_header_bytes: client_config
                .max_header_bytes
                .unwrap_or(factory_config.max_header_bytes),
            pool_idle_timeout: Duration::from_secs(
                client_config
                    .pool_idle_timeout_secs
                    .unwrap_or(factory_config.pool_idle_timeout_secs),
            ),
            pool_max_idle_per_host: client_config
                .pool_max_idle_per_host
                .unwrap_or(factory_config.pool_max_idle_per_host),
            max_retained_origins: client_config
                .max_retained_origins
                .unwrap_or(factory_config.max_retained_origins),
            http2_initial_stream_window_bytes: client_config
                .http2_initial_stream_window_bytes
                .unwrap_or(factory_config.http2_initial_stream_window_bytes),
            http2_initial_connection_window_bytes: client_config
                .http2_initial_connection_window_bytes
                .unwrap_or(factory_config.http2_initial_connection_window_bytes),
            http2_max_frame_bytes: client_config
                .http2_max_frame_bytes
                .unwrap_or(factory_config.http2_max_frame_bytes),
            http2_keep_alive_interval: Duration::from_secs(
                client_config
                    .http2_keep_alive_interval_secs
                    .unwrap_or(factory_config.http2_keep_alive_interval_secs),
            ),
            http2_keep_alive_timeout: Duration::from_secs(
                client_config
                    .http2_keep_alive_timeout_secs
                    .unwrap_or(factory_config.http2_keep_alive_timeout_secs),
            ),
            retry_unstarted_requests: client_config
                .retry_unstarted_requests
                .unwrap_or(factory_config.retry_unstarted_requests),
        };
        config
            .validate()
            .map_err(|error| HttpApiError::ConfigurationError(error.to_string()))?;
        Ok(config)
    }

    /// Clear all cached clients
    pub fn clear_cache(&self) {
        let mut clients = self
            .clients
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        clients.clear();
    }

    /// Get the number of cached clients
    pub fn cached_count(&self) -> usize {
        let clients = self
            .clients
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        clients.len()
    }
}

async fn read_additional_ca_bundle(path: &Path) -> Result<Vec<u8>, HttpClientError> {
    if !path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        return Err(HttpClientError::Tls(
            "additional CA bundle path must be absolute and canonical".to_string(),
        ));
    }
    let metadata = tokio::fs::symlink_metadata(path).await.map_err(|_| {
        HttpClientError::Tls("additional CA bundle file is unavailable".to_string())
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(HttpClientError::Tls(
            "additional CA bundle path must reference a regular non-symlink file".to_string(),
        ));
    }
    if metadata.len() == 0 || metadata.len() > MAX_ADDITIONAL_CA_BUNDLE_BYTES as u64 {
        return Err(HttpClientError::Tls(
            "additional CA bundle is outside the supported size bound".to_string(),
        ));
    }
    let canonical = tokio::fs::canonicalize(path).await.map_err(|_| {
        HttpClientError::Tls("additional CA bundle file is unavailable".to_string())
    })?;
    if canonical != path {
        return Err(HttpClientError::Tls(
            "additional CA bundle path must be absolute and canonical".to_string(),
        ));
    }
    let bytes = tokio::fs::read(path).await.map_err(|_| {
        HttpClientError::Tls("additional CA bundle file is unavailable".to_string())
    })?;
    if bytes.is_empty() || bytes.len() > MAX_ADDITIONAL_CA_BUNDLE_BYTES {
        return Err(HttpClientError::Tls(
            "additional CA bundle is outside the supported size bound".to_string(),
        ));
    }
    Ok(bytes)
}

#[async_trait::async_trait]
impl ServiceTrait for LilyHttpClientFactory {
    #[lily_trace::lily_trace(
        name = "http_client_factory.initialize",
        skip(self),
        env = "development"
    )]
    async fn initialize(&mut self) -> std::result::Result<(), InjectionError> {
        lily_trace::prelude::info!("Initializing HTTP client factory");
        // Read http_client_factory configuration from ConfigService
        let lily_config = self.config_service.get_lily_config().await;

        if let Some(factory_config) = lily_config.http_client_factory {
            lily_trace::prelude::debug!(
                "Found HTTP client factory configuration with {} clients",
                factory_config.clients.len()
            );
            let mut validated_profiles = HashMap::with_capacity(factory_config.clients.len());
            for (name, client_config) in &factory_config.clients {
                let config = self
                    .build_client_config(&factory_config, client_config)
                    .map_err(|error| {
                        InjectionError::InitError(format!(
                            "invalid HTTP client configuration for '{name}': {error}"
                        ))
                    })?;
                let additional_ca = match client_config.additional_ca_bundle.as_deref() {
                    Some(path) => Some(read_additional_ca_bundle(path).await.map_err(|error| {
                        InjectionError::InitError(format!(
                            "invalid HTTP client trust configuration for '{name}': {error}"
                        ))
                    })?),
                    None => None,
                };
                let profile = ValidatedClientProfile::new(config, additional_ca.as_deref())
                    .map_err(|error| {
                        InjectionError::InitError(format!(
                            "invalid HTTP client configuration for '{name}': {error}"
                        ))
                    })?;
                validated_profiles.insert(name.clone(), profile);
            }

            let mut profiles = self.profiles.write().map_err(|_| {
                InjectionError::InitError(
                    "HTTP client factory profile lock is poisoned".to_string(),
                )
            })?;
            *profiles = Some(validated_profiles);
            lily_trace::prelude::info!("HTTP client factory initialized successfully");
            Ok(())
        } else {
            lily_trace::prelude::warn!(
                "No HTTP client factory configuration found, using defaults"
            );
            let mut profiles = self.profiles.write().map_err(|_| {
                InjectionError::InitError(
                    "HTTP client factory profile lock is poisoned".to_string(),
                )
            })?;
            *profiles = Some(HashMap::new());
            Ok(())
        }
    }

    #[lily_trace::lily_trace(name = "http_client_factory.dispose", skip(self), env = "development")]
    async fn dispose(&self) -> std::result::Result<(), InjectionError> {
        lily_trace::prelude::info!("Disposing HTTP client factory");
        // Clear all cached clients
        self.clear_cache();
        lily_trace::prelude::info!("HTTP client factory disposed successfully");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn named_client() -> HttpClientConfig {
        HttpClientConfig {
            base_address: "https://api.example.test".to_string(),
            connect_timeout_secs: None,
            request_timeout_secs: None,
            max_redirects: None,
            user_agent: None,
            protocol: None,
            max_in_flight_requests: None,
            max_in_flight_requests_per_origin: None,
            max_request_body_bytes: None,
            max_response_body_bytes: None,
            max_header_count: None,
            max_header_bytes: None,
            pool_idle_timeout_secs: None,
            pool_max_idle_per_host: None,
            max_retained_origins: None,
            http2_initial_stream_window_bytes: None,
            http2_initial_connection_window_bytes: None,
            http2_max_frame_bytes: None,
            http2_keep_alive_interval_secs: None,
            http2_keep_alive_timeout_secs: None,
            retry_unstarted_requests: None,
            additional_ca_bundle: None,
            headers: None,
        }
    }

    #[test]
    fn factory_config_maps_each_supported_field_once() {
        let factory = LilyHttpClientFactory::default();
        let defaults = HttpClientFactoryConfig {
            connect_timeout_secs: 7,
            request_timeout_secs: 17,
            max_redirects: 4,
            user_agent: "factory-agent/1.0".to_string(),
            protocol: HttpClientProtocol::Http2,
            max_in_flight_requests: 80,
            max_in_flight_requests_per_origin: 10,
            max_request_body_bytes: 1111,
            max_response_body_bytes: 2222,
            max_header_count: 40,
            max_header_bytes: 4000,
            pool_idle_timeout_secs: 44,
            pool_max_idle_per_host: 7,
            max_retained_origins: 8,
            http2_initial_stream_window_bytes: 64 * 1024,
            http2_initial_connection_window_bytes: 128 * 1024,
            http2_max_frame_bytes: 32 * 1024,
            http2_keep_alive_interval_secs: 21,
            http2_keep_alive_timeout_secs: 6,
            retry_unstarted_requests: false,
            clients: HashMap::new(),
        };
        let inherited = factory
            .build_client_config(&defaults, &named_client())
            .unwrap();
        assert_eq!(inherited.base_address(), Some("https://api.example.test"));
        assert_eq!(inherited.connect_timeout(), Duration::from_secs(7));
        assert_eq!(inherited.request_timeout(), Duration::from_secs(17));
        assert_eq!(inherited.max_redirects(), 4);
        assert_eq!(inherited.protocol(), ProtocolPreference::Http2Only);
        assert_eq!(inherited.max_in_flight_requests(), 80);
        assert_eq!(inherited.max_in_flight_requests_per_origin(), 10);
        assert_eq!(inherited.max_request_body_bytes(), 1111);
        assert_eq!(inherited.max_response_body_bytes(), 2222);
        assert_eq!(inherited.max_header_count(), 40);
        assert_eq!(inherited.max_header_bytes(), 4000);
        assert_eq!(inherited.pool_idle_timeout(), Duration::from_secs(44));
        assert_eq!(inherited.pool_max_idle_per_host(), 7);
        assert_eq!(inherited.max_retained_origins(), 8);
        assert_eq!(inherited.http2_initial_stream_window_bytes(), 64 * 1024);
        assert_eq!(
            inherited.http2_initial_connection_window_bytes(),
            128 * 1024
        );
        assert_eq!(inherited.http2_max_frame_bytes(), 32 * 1024);
        assert_eq!(
            inherited.http2_keep_alive_interval(),
            Duration::from_secs(21)
        );
        assert_eq!(inherited.http2_keep_alive_timeout(), Duration::from_secs(6));
        assert!(!inherited.retries_unstarted_requests());
        assert_eq!(
            inherited.default_headers().get("User-Agent"),
            Some("factory-agent/1.0")
        );
        assert!(HttpClient::try_with_config(inherited).is_ok());

        let mut named = named_client();
        named.connect_timeout_secs = Some(11);
        named.request_timeout_secs = Some(23);
        named.max_redirects = Some(2);
        named.user_agent = Some("inventory-agent/2.0".to_string());
        named.protocol = Some(HttpClientProtocol::Http1);
        named.max_in_flight_requests = Some(90);
        named.max_in_flight_requests_per_origin = Some(9);
        named.max_request_body_bytes = Some(3333);
        named.max_response_body_bytes = Some(4444);
        named.max_header_count = Some(50);
        named.max_header_bytes = Some(5000);
        named.pool_idle_timeout_secs = Some(55);
        named.pool_max_idle_per_host = Some(6);
        named.max_retained_origins = Some(7);
        named.http2_initial_stream_window_bytes = Some(96 * 1024);
        named.http2_initial_connection_window_bytes = Some(192 * 1024);
        named.http2_max_frame_bytes = Some(64 * 1024);
        named.http2_keep_alive_interval_secs = Some(22);
        named.http2_keep_alive_timeout_secs = Some(7);
        named.retry_unstarted_requests = Some(true);
        named.headers = Some(HashMap::from([(
            "X-Client-Scope".to_string(),
            "inventory".to_string(),
        )]));

        let overridden = factory.build_client_config(&defaults, &named).unwrap();

        assert_eq!(overridden.base_address(), Some("https://api.example.test"));
        assert_eq!(overridden.connect_timeout(), Duration::from_secs(11));
        assert_eq!(overridden.request_timeout(), Duration::from_secs(23));
        assert_eq!(overridden.max_redirects(), 2);
        assert_eq!(overridden.protocol(), ProtocolPreference::Http1Only);
        assert_eq!(overridden.max_in_flight_requests(), 90);
        assert_eq!(overridden.max_in_flight_requests_per_origin(), 9);
        assert_eq!(overridden.max_request_body_bytes(), 3333);
        assert_eq!(overridden.max_response_body_bytes(), 4444);
        assert_eq!(overridden.max_header_count(), 50);
        assert_eq!(overridden.max_header_bytes(), 5000);
        assert_eq!(overridden.pool_idle_timeout(), Duration::from_secs(55));
        assert_eq!(overridden.pool_max_idle_per_host(), 6);
        assert_eq!(overridden.max_retained_origins(), 7);
        assert_eq!(overridden.http2_initial_stream_window_bytes(), 96 * 1024);
        assert_eq!(
            overridden.http2_initial_connection_window_bytes(),
            192 * 1024
        );
        assert_eq!(overridden.http2_max_frame_bytes(), 64 * 1024);
        assert_eq!(
            overridden.http2_keep_alive_interval(),
            Duration::from_secs(22)
        );
        assert_eq!(
            overridden.http2_keep_alive_timeout(),
            Duration::from_secs(7)
        );
        assert!(overridden.retries_unstarted_requests());
        assert_eq!(
            overridden.default_headers().get("User-Agent"),
            Some("inventory-agent/2.0")
        );
        assert_eq!(
            overridden.default_headers().get("X-Client-Scope"),
            Some("inventory")
        );
        assert!(HttpClient::try_with_config(overridden).is_ok());
    }

    #[test]
    fn factory_canonical_defaults_match_direct_client_defaults() {
        let factory = LilyHttpClientFactory::default();
        let inherited = factory
            .build_client_config(&HttpClientFactoryConfig::default(), &named_client())
            .unwrap();
        let direct = ClientConfig::default();

        assert_eq!(inherited.connect_timeout(), direct.connect_timeout());
        assert_eq!(inherited.request_timeout(), direct.request_timeout());
        assert_eq!(inherited.max_redirects(), direct.max_redirects());
        assert_eq!(inherited.protocol(), direct.protocol());
        assert_eq!(
            inherited.default_headers().get("User-Agent"),
            direct.default_headers().get("User-Agent")
        );
        assert_eq!(
            inherited.max_in_flight_requests(),
            direct.max_in_flight_requests()
        );
        assert_eq!(
            inherited.max_in_flight_requests_per_origin(),
            direct.max_in_flight_requests_per_origin()
        );
        assert_eq!(
            inherited.max_request_body_bytes(),
            direct.max_request_body_bytes()
        );
        assert_eq!(
            inherited.max_response_body_bytes(),
            direct.max_response_body_bytes()
        );
        assert_eq!(inherited.max_header_count(), direct.max_header_count());
        assert_eq!(inherited.max_header_bytes(), direct.max_header_bytes());
        assert_eq!(inherited.pool_idle_timeout(), direct.pool_idle_timeout());
        assert_eq!(
            inherited.pool_max_idle_per_host(),
            direct.pool_max_idle_per_host()
        );
        assert_eq!(
            inherited.max_retained_origins(),
            direct.max_retained_origins()
        );
        assert_eq!(
            inherited.http2_initial_stream_window_bytes(),
            direct.http2_initial_stream_window_bytes()
        );
        assert_eq!(
            inherited.http2_initial_connection_window_bytes(),
            direct.http2_initial_connection_window_bytes()
        );
        assert_eq!(
            inherited.http2_max_frame_bytes(),
            direct.http2_max_frame_bytes()
        );
        assert_eq!(
            inherited.http2_keep_alive_interval(),
            direct.http2_keep_alive_interval()
        );
        assert_eq!(
            inherited.http2_keep_alive_timeout(),
            direct.http2_keep_alive_timeout()
        );
        assert_eq!(
            inherited.retries_unstarted_requests(),
            direct.retries_unstarted_requests()
        );
    }

    #[test]
    fn factory_config_rejects_invalid_default_headers() {
        let factory = LilyHttpClientFactory::default();
        let defaults = HttpClientFactoryConfig::default();
        let mut client = named_client();
        client.headers = Some(HashMap::from([(
            "x-safe".to_string(),
            "value\r\ninjected: true".to_string(),
        )]));

        assert!(factory.build_client_config(&defaults, &client).is_err());
    }

    #[test]
    fn factory_config_has_one_user_agent_source() {
        let factory = LilyHttpClientFactory::default();
        let defaults = HttpClientFactoryConfig::default();
        let mut client = named_client();
        client.headers = Some(HashMap::from([(
            "user-agent".to_string(),
            "ambiguous-agent/1.0".to_string(),
        )]));

        assert!(factory.build_client_config(&defaults, &client).is_err());
    }

    #[test]
    fn factory_rejects_oversized_and_transport_owned_default_headers() {
        let factory = LilyHttpClientFactory::default();
        let defaults = HttpClientFactoryConfig::default();
        let mut oversized = named_client();
        oversized.headers = Some(HashMap::from([(
            "x-oversized".to_string(),
            "x".repeat(64 * 1024),
        )]));
        assert!(factory.build_client_config(&defaults, &oversized).is_err());

        for name in [
            "connection",
            "keep-alive",
            "proxy-connection",
            "proxy-authenticate",
            "proxy-authorization",
            "te",
            "trailer",
            "transfer-encoding",
            "upgrade",
            "content-length",
            "host",
        ] {
            let mut client = named_client();
            client.headers = Some(HashMap::from([(
                name.to_string(),
                "configured".to_string(),
            )]));
            assert!(factory.build_client_config(&defaults, &client).is_err());
        }
    }

    #[test]
    fn factory_rejects_case_aliases_in_default_headers_deterministically() {
        let factory = LilyHttpClientFactory::default();
        let defaults = HttpClientFactoryConfig::default();
        let mut client = named_client();
        client.headers = Some(HashMap::from([
            ("X-Api-Key".to_string(), "first".to_string()),
            ("x-api-key".to_string(), "second".to_string()),
        ]));

        for _ in 0..32 {
            assert!(factory.build_client_config(&defaults, &client).is_err());
        }
    }

    #[tokio::test]
    async fn additional_ca_file_loading_is_bounded_canonical_and_redacted() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("private-ca.pem");
        tokio::fs::write(&path, b"not-a-certificate").await.unwrap();
        let path = tokio::fs::canonicalize(path).await.unwrap();

        let bytes = read_additional_ca_bundle(&path).await.unwrap();
        let parse_error = ValidatedClientProfile::new(ClientConfig::default(), Some(&bytes))
            .err()
            .expect("non-PEM material must be rejected by the shared trust parser");
        assert_eq!(parse_error.diagnostic_code(), "TLS_CA_BUNDLE_EMPTY");

        let relative = read_additional_ca_bundle(Path::new("private-ca.pem"))
            .await
            .unwrap_err();
        assert_eq!(relative.diagnostic_code(), "TLS_CA_PATH_INVALID");

        #[cfg(unix)]
        {
            let link = directory.path().join("private-ca-link.pem");
            std::os::unix::fs::symlink(&path, &link).unwrap();
            let error = read_additional_ca_bundle(&link).await.unwrap_err();
            assert_eq!(error.diagnostic_code(), "TLS_CA_PATH_NOT_REGULAR");
        }

        let display = relative.to_string();
        let debug = format!("{relative:?}");
        assert!(!display.contains("private-ca.pem"));
        assert!(!debug.contains("private-ca.pem"));
    }

    #[test]
    fn concurrent_factory_get_publishes_exactly_one_named_client() {
        let factory = Arc::new(LilyHttpClientFactory::default());
        let config = factory
            .build_client_config(&HttpClientFactoryConfig::default(), &named_client())
            .unwrap();
        let profile = ValidatedClientProfile::new(config, None).unwrap();
        *factory.profiles.write().unwrap() =
            Some(HashMap::from([("inventory".to_string(), profile)]));
        let barrier = Arc::new(std::sync::Barrier::new(16));
        let handles = (0..16)
            .map(|_| {
                let factory = Arc::clone(&factory);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    factory.get("inventory").unwrap()
                })
            })
            .collect::<Vec<_>>();
        let clients = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();

        assert_eq!(factory.cached_count(), 1);
        assert!(clients
            .iter()
            .all(|client| Arc::ptr_eq(client, &clients[0])));
    }
}
