use std::{fmt, time::Duration};

use crate::{
    client::{ProtocolPreference, MAX_CONNECT_TIMEOUT, MAX_REQUEST_TIMEOUT},
    error::{HttpClientError, Result},
};

/// Request configuration options
///
/// Contains various settings that control request behavior such as timeouts,
/// redirects, and other client-specific options.
#[derive(Clone)]
pub struct RequestConfig {
    /// Connection timeout
    pub(crate) connect_timeout: Option<Duration>,
    /// Read timeout for the entire request
    pub(crate) timeout: Option<Duration>,
    /// Maximum number of redirects to follow
    pub(crate) max_redirects: Option<u32>,
    /// Whether to follow redirects automatically
    pub(crate) follow_redirects: bool,
    /// Optional exact protocol policy. `Http2Only` is the explicit switch for
    /// h2c prior knowledge on plain HTTP URLs.
    pub(crate) protocol: Option<ProtocolPreference>,
}

impl fmt::Debug for RequestConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RequestConfig")
            .field("connect_timeout", &self.connect_timeout)
            .field("timeout", &self.timeout)
            .field("max_redirects", &self.max_redirects)
            .field("follow_redirects", &self.follow_redirects)
            .field("protocol", &self.protocol)
            .finish()
    }
}

impl Default for RequestConfig {
    fn default() -> Self {
        Self {
            connect_timeout: None,
            timeout: None,
            max_redirects: None,
            follow_redirects: true,
            protocol: None,
        }
    }
}

impl RequestConfig {
    /// Create a new default request configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the connection timeout
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    /// Set the overall request timeout
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Set the maximum number of redirects
    pub fn max_redirects(mut self, max: u32) -> Self {
        self.max_redirects = Some(max);
        self
    }

    /// Enable or disable automatic redirect following
    pub fn follow_redirects(mut self, follow: bool) -> Self {
        self.follow_redirects = follow;
        self
    }

    /// Select an exact HTTP protocol policy for this request.
    pub fn protocol(mut self, protocol: ProtocolPreference) -> Self {
        self.protocol = Some(protocol);
        self
    }

    /// Disable redirects
    pub fn no_redirects(mut self) -> Self {
        self.follow_redirects = false;
        self.max_redirects = Some(0);
        self
    }

    /// Create a configuration for production (strict settings)
    pub fn production() -> Self {
        Self {
            timeout: Some(Duration::from_secs(30)),
            connect_timeout: Some(Duration::from_secs(10)),
            max_redirects: Some(5),
            ..Default::default()
        }
    }

    /// Request-specific connect timeout, or `None` to inherit the client value.
    pub fn connect_timeout_override(&self) -> Option<Duration> {
        self.connect_timeout
    }

    /// Whole-request timeout, or `None` to inherit the client value.
    pub fn timeout_override(&self) -> Option<Duration> {
        self.timeout
    }

    /// Redirect cap, or `None` to inherit the client value.
    pub fn max_redirects_override(&self) -> Option<u32> {
        self.max_redirects
    }

    pub fn follows_redirects(&self) -> bool {
        self.follow_redirects
    }

    pub fn protocol_preference(&self) -> Option<ProtocolPreference> {
        self.protocol
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self
            .connect_timeout
            .is_some_and(|timeout| timeout.is_zero() || timeout > MAX_CONNECT_TIMEOUT)
        {
            return Err(HttpClientError::Configuration(format!(
                "connect timeout must be in 1ns..={} seconds",
                MAX_CONNECT_TIMEOUT.as_secs()
            )));
        }
        if self
            .timeout
            .is_some_and(|timeout| timeout.is_zero() || timeout > MAX_REQUEST_TIMEOUT)
        {
            return Err(HttpClientError::Configuration(format!(
                "request timeout must be in 1ns..={} seconds",
                MAX_REQUEST_TIMEOUT.as_secs()
            )));
        }
        if self.max_redirects.is_some_and(|max| max > 20) {
            return Err(HttpClientError::Configuration(
                "max_redirects must be <= 20".to_string(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = RequestConfig::default();

        assert_eq!(config.connect_timeout, None);
        assert_eq!(config.timeout, None);
        assert_eq!(config.max_redirects, None);
        assert!(config.follow_redirects);
        assert_eq!(config.protocol, None);
    }

    #[test]
    fn test_builder_pattern() {
        let config = RequestConfig::new()
            .timeout(Duration::from_secs(120))
            .no_redirects()
            .protocol(ProtocolPreference::Http1Only);

        assert_eq!(config.timeout, Some(Duration::from_secs(120)));
        assert!(!config.follow_redirects);
        assert_eq!(config.max_redirects, Some(0));
        assert_eq!(config.protocol, Some(ProtocolPreference::Http1Only));
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_preset_configs() {
        let prod_config = RequestConfig::production();
        assert_eq!(
            prod_config.connect_timeout_override(),
            Some(Duration::from_secs(10))
        );
        assert_eq!(prod_config.timeout, Some(Duration::from_secs(30)));
        assert_eq!(prod_config.max_redirects, Some(5));
        assert!(prod_config.validate().is_ok());
    }

    #[test]
    fn invalid_supported_overrides_fail_validation() {
        assert!(RequestConfig::new()
            .timeout(Duration::ZERO)
            .validate()
            .is_err());
        assert!(RequestConfig::new()
            .connect_timeout(MAX_CONNECT_TIMEOUT + Duration::from_nanos(1))
            .validate()
            .is_err());
        assert!(RequestConfig::new().max_redirects(21).validate().is_err());
    }

    #[test]
    fn request_override_limits_are_inclusive() {
        let config = RequestConfig::new()
            .connect_timeout(MAX_CONNECT_TIMEOUT)
            .timeout(MAX_REQUEST_TIMEOUT)
            .max_redirects(20);

        assert_eq!(config.connect_timeout_override(), Some(MAX_CONNECT_TIMEOUT));
        assert!(config.validate().is_ok());
        assert!(RequestConfig::new()
            .timeout(MAX_REQUEST_TIMEOUT + Duration::from_nanos(1))
            .validate()
            .is_err());
    }

    #[test]
    fn request_config_debug_output_reports_every_policy_field() {
        let output = format!(
            "{:?}",
            RequestConfig::new()
                .connect_timeout(Duration::from_secs(2))
                .timeout(Duration::from_secs(3))
                .max_redirects(4)
                .follow_redirects(false)
                .protocol(ProtocolPreference::Http2Only)
        );

        assert!(output.starts_with("RequestConfig"));
        for field in [
            "connect_timeout",
            "timeout",
            "max_redirects",
            "follow_redirects",
            "protocol",
        ] {
            assert!(output.contains(field), "missing debug field: {field}");
        }
    }
}
