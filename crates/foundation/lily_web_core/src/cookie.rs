//! Typed HTTP cookie primitives shared by Lily requests and responses.
//!
//! This module deliberately stops at the HTTP and secure-cookie boundary.
//! Applications own session identifiers, persistence, session rotation,
//! expiry policy, and the guards that turn a validated cookie into
//! request-local identity.

use std::fmt::{self, Write as _};
use std::time::Duration;

/// Maximum bytes accepted for a cookie name before it is retained.
pub const MAX_COOKIE_NAME_BYTES: usize = 256;

/// Maximum bytes accepted for one cookie value before it is retained.
pub const MAX_COOKIE_VALUE_BYTES: usize = 4096;

/// Maximum bytes accepted for a cookie path before it is retained.
pub const MAX_COOKIE_PATH_BYTES: usize = 1024;

/// Maximum bytes accepted for a cookie domain before it is retained.
pub const MAX_COOKIE_DOMAIN_BYTES: usize = 253;

/// Maximum serialized bytes for one `Set-Cookie` field value.
pub const MAX_SET_COOKIE_BYTES: usize = 4096;

/// Maximum number of cookie pairs retained by one request jar.
pub const MAX_REQUEST_COOKIE_PAIRS: usize = 128;

/// Maximum aggregate name/value bytes retained by one parsed request jar.
pub const MAX_REQUEST_COOKIE_RETAINED_BYTES: usize = 64 * 1024;

/// Minimum cryptographically random master-secret bytes accepted for a cookie key.
pub const MIN_COOKIE_KEY_BYTES: usize = 32;

/// Maximum number of previous keys tried while verifying a secure cookie.
pub const MAX_COOKIE_PREVIOUS_KEYS: usize = 3;

/// Absolute UTC timestamp used by the typed `Expires` response attribute.
pub type CookieExpires = ::cookie::time::OffsetDateTime;

/// Typed `SameSite` policy for a response cookie.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CookieSameSite {
    /// Send the cookie only in same-site contexts.
    Strict,
    /// Allow same-site requests and safe top-level cross-site navigation.
    Lax,
    /// Allow cross-site delivery; Lily also forces the `Secure` attribute.
    None,
}

impl From<CookieSameSite> for ::cookie::SameSite {
    fn from(value: CookieSameSite) -> Self {
        match value {
            CookieSameSite::Strict => Self::Strict,
            CookieSameSite::Lax => Self::Lax,
            CookieSameSite::None => Self::None,
        }
    }
}

/// A stable, secret-safe response-cookie construction error.
///
/// `Debug` and `Display` intentionally never include caller-provided cookie
/// names, values, paths, or domains.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ResponseCookieError {
    /// The name violates the cookie grammar.
    #[error("cookie name is invalid")]
    InvalidName,
    /// The name exceeded its byte bound.
    #[error("cookie name exceeds the {limit_bytes}-byte limit")]
    NameTooLong {
        /// Maximum accepted name length.
        limit_bytes: usize,
    },
    /// The value violates the cookie grammar.
    #[error("cookie value is invalid")]
    InvalidValue,
    /// The value exceeded its byte bound.
    #[error("cookie value exceeds the {limit_bytes}-byte limit")]
    ValueTooLong {
        /// Maximum accepted value length.
        limit_bytes: usize,
    },
    /// The path contains a forbidden byte or is not absolute.
    #[error("cookie path is invalid")]
    InvalidPath,
    /// The path exceeded its byte bound.
    #[error("cookie path exceeds the {limit_bytes}-byte limit")]
    PathTooLong {
        /// Maximum accepted path length.
        limit_bytes: usize,
    },
    /// The domain is not a valid cookie domain.
    #[error("cookie domain is invalid")]
    InvalidDomain,
    /// The domain exceeded its byte bound.
    #[error("cookie domain exceeds the {limit_bytes}-byte limit")]
    DomainTooLong {
        /// Maximum accepted domain length.
        limit_bytes: usize,
    },
    /// The supplied duration cannot be represented by the cookie codec.
    #[error("cookie max-age cannot be represented")]
    MaxAgeOutOfRange,
    /// The complete `Set-Cookie` field value exceeded its byte bound.
    #[error("serialized cookie exceeds the {limit_bytes}-byte limit")]
    SerializedTooLarge {
        /// Maximum serialized field-value length.
        limit_bytes: usize,
    },
    /// The validated cookie could not be serialized.
    #[error("cookie serialization failed")]
    SerializationFailed,
}

/// A typed response cookie.
///
/// The value is intentionally redacted from `Debug`. Values are serialized by
/// the `cookie` crate and are never percent-encoded implicitly by Lily.
#[derive(Clone, PartialEq, Eq)]
pub struct ResponseCookie {
    name: String,
    value: String,
    secure: bool,
    http_only: bool,
    same_site: Option<CookieSameSite>,
    path: Option<String>,
    domain: Option<String>,
    max_age_seconds: Option<i64>,
    expires: Option<CookieExpires>,
    partitioned: bool,
}

impl fmt::Debug for ResponseCookie {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResponseCookie")
            .field("name", &"<redacted>")
            .field("name_bytes", &self.name.len())
            .field("value", &"<redacted>")
            .field("value_bytes", &self.value.len())
            .field("secure", &self.secure)
            .field("http_only", &self.http_only)
            .field("same_site", &self.same_site)
            .field("path_configured", &self.path.is_some())
            .field("domain_configured", &self.domain.is_some())
            .field("max_age_configured", &self.max_age_seconds.is_some())
            .field("expires_configured", &self.expires.is_some())
            .field("partitioned", &self.partitioned)
            .finish()
    }
}

impl ResponseCookie {
    /// Creates a bounded cookie with no optional attributes.
    pub fn new(name: &str, value: &str) -> Result<Self, ResponseCookieError> {
        validate_name(name)?;
        validate_value(value)?;

        Ok(Self {
            name: name.to_owned(),
            value: value.to_owned(),
            secure: false,
            http_only: false,
            same_site: None,
            path: None,
            domain: None,
            max_age_seconds: None,
            expires: None,
            partitioned: false,
        })
    }

    #[must_use]
    /// Enables or disables `Secure`.
    ///
    /// `SameSite=None` and partitioned cookies remain secure even when
    /// `enabled` is `false`.
    pub fn secure(mut self, enabled: bool) -> Self {
        self.secure = enabled || self.partitioned || self.same_site == Some(CookieSameSite::None);
        self
    }

    #[must_use]
    /// Enables or disables `HttpOnly`.
    pub fn http_only(mut self, enabled: bool) -> Self {
        self.http_only = enabled;
        self
    }

    #[must_use]
    /// Sets the `SameSite` policy.
    pub fn same_site(mut self, policy: CookieSameSite) -> Self {
        self.same_site = Some(policy);
        if policy == CookieSameSite::None {
            self.secure = true;
        }
        self
    }

    /// Sets and validates the cookie path.
    pub fn path(mut self, path: &str) -> Result<Self, ResponseCookieError> {
        validate_path(path)?;
        self.path = Some(path.to_owned());
        Ok(self)
    }

    /// Sets and validates the cookie domain.
    pub fn domain(mut self, domain: &str) -> Result<Self, ResponseCookieError> {
        validate_domain(domain)?;
        self.domain = Some(domain.to_owned());
        Ok(self)
    }

    /// Sets a non-negative `Max-Age` duration.
    pub fn max_age(mut self, max_age: Duration) -> Result<Self, ResponseCookieError> {
        let seconds =
            i64::try_from(max_age.as_secs()).map_err(|_| ResponseCookieError::MaxAgeOutOfRange)?;
        self.max_age_seconds = Some(seconds);
        Ok(self)
    }

    /// Sets an absolute UTC expiry. This can coexist with `Max-Age`.
    #[must_use]
    pub fn expires(mut self, expires: CookieExpires) -> Self {
        self.expires = Some(expires);
        self
    }

    /// Marks this as a partitioned cookie. Partitioned cookies are always secure.
    #[must_use]
    pub fn partitioned(mut self, enabled: bool) -> Self {
        self.partitioned = enabled;
        if enabled {
            self.secure = true;
        }
        self
    }

    #[must_use]
    /// Returns the validated cookie name.
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    /// Returns the unencoded cookie value.
    pub fn value(&self) -> &str {
        &self.value
    }

    #[must_use]
    /// Reports whether `Secure` will be emitted.
    pub const fn is_secure(&self) -> bool {
        self.secure
    }

    #[must_use]
    /// Reports whether `HttpOnly` will be emitted.
    pub const fn is_http_only(&self) -> bool {
        self.http_only
    }

    #[must_use]
    /// Returns the configured `SameSite` policy.
    pub const fn same_site_policy(&self) -> Option<CookieSameSite> {
        self.same_site
    }

    #[must_use]
    /// Returns the configured path.
    pub fn configured_path(&self) -> Option<&str> {
        self.path.as_deref()
    }

    #[must_use]
    /// Returns the configured domain.
    pub fn configured_domain(&self) -> Option<&str> {
        self.domain.as_deref()
    }

    #[must_use]
    /// Returns the configured `Max-Age` duration.
    pub fn configured_max_age(&self) -> Option<Duration> {
        self.max_age_seconds
            .map(|seconds| Duration::from_secs(seconds as u64))
    }

    #[must_use]
    /// Returns the configured absolute expiry.
    pub const fn configured_expires(&self) -> Option<CookieExpires> {
        self.expires
    }

    #[must_use]
    /// Reports whether the `Partitioned` attribute will be emitted.
    pub const fn is_partitioned(&self) -> bool {
        self.partitioned
    }

    pub(crate) fn to_header_value(&self) -> Result<String, ResponseCookieError> {
        render_bounded(&self.to_cookie())
    }

    pub(crate) fn to_signed_header_value(
        &self,
        key_ring: &CookieKeyRing,
    ) -> Result<String, ResponseCookieError> {
        self.to_secure_header_value(key_ring, SecureCookieKind::Signed)
    }

    pub(crate) fn to_private_header_value(
        &self,
        key_ring: &CookieKeyRing,
    ) -> Result<String, ResponseCookieError> {
        self.to_secure_header_value(key_ring, SecureCookieKind::Private)
    }

    fn to_cookie(&self) -> ::cookie::Cookie<'static> {
        let mut cookie = ::cookie::Cookie::new(self.name.clone(), self.value.clone());
        if self.secure {
            cookie.set_secure(true);
        }
        if self.http_only {
            cookie.set_http_only(true);
        }
        if let Some(same_site) = self.same_site {
            cookie.set_same_site(::cookie::SameSite::from(same_site));
        }
        if let Some(path) = self.path.as_deref() {
            cookie.set_path(path.to_owned());
        }
        if let Some(domain) = self.domain.as_deref() {
            cookie.set_domain(domain.to_owned());
        }
        if let Some(seconds) = self.max_age_seconds {
            cookie.set_max_age(::cookie::time::Duration::seconds(seconds));
        }
        if let Some(expires) = self.expires {
            cookie.set_expires(expires);
        }
        if self.partitioned {
            cookie.set_partitioned(true);
        }

        cookie
    }

    fn to_secure_header_value(
        &self,
        key_ring: &CookieKeyRing,
        kind: SecureCookieKind,
    ) -> Result<String, ResponseCookieError> {
        let mut jar = ::cookie::CookieJar::new();
        let name = self.name.clone();
        match kind {
            SecureCookieKind::Signed => jar.signed_mut(key_ring.primary()).add(self.to_cookie()),
            SecureCookieKind::Private => jar.private_mut(key_ring.primary()).add(self.to_cookie()),
        }

        let cookie = jar
            .get(&name)
            .ok_or(ResponseCookieError::SerializationFailed)?;
        render_bounded(cookie)
    }
}

/// Scope information for explicitly removing a response cookie.
///
/// A browser identifies a stored cookie by name, domain, and path. Callers
/// should therefore supply the same domain/path used when the cookie was set.
#[derive(Clone, PartialEq, Eq)]
pub struct CookieRemoval {
    name: String,
    path: Option<String>,
    domain: Option<String>,
    partitioned: bool,
}

impl fmt::Debug for CookieRemoval {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CookieRemoval")
            .field("name", &"<redacted>")
            .field("name_bytes", &self.name.len())
            .field("path_configured", &self.path.is_some())
            .field("domain_configured", &self.domain.is_some())
            .field("partitioned", &self.partitioned)
            .finish()
    }
}

impl CookieRemoval {
    /// Creates a removal cookie for `name`.
    pub fn new(name: &str) -> Result<Self, ResponseCookieError> {
        validate_name(name)?;
        Ok(Self {
            name: name.to_owned(),
            path: None,
            domain: None,
            partitioned: false,
        })
    }

    /// Sets the path of the stored cookie to remove.
    pub fn path(mut self, path: &str) -> Result<Self, ResponseCookieError> {
        validate_path(path)?;
        self.path = Some(path.to_owned());
        Ok(self)
    }

    /// Sets the domain of the stored cookie to remove.
    pub fn domain(mut self, domain: &str) -> Result<Self, ResponseCookieError> {
        validate_domain(domain)?;
        self.domain = Some(domain.to_owned());
        Ok(self)
    }

    /// Matches the partitioned scope of the cookie being removed.
    #[must_use]
    pub fn partitioned(mut self, enabled: bool) -> Self {
        self.partitioned = enabled;
        self
    }

    #[must_use]
    /// Returns the configured removal path.
    pub fn configured_path(&self) -> Option<&str> {
        self.path.as_deref()
    }

    #[must_use]
    /// Returns the configured removal domain.
    pub fn configured_domain(&self) -> Option<&str> {
        self.domain.as_deref()
    }

    #[must_use]
    /// Reports whether the removal targets a partitioned cookie.
    pub const fn is_partitioned(&self) -> bool {
        self.partitioned
    }

    pub(crate) fn to_header_value(&self) -> Result<String, ResponseCookieError> {
        let mut cookie = ::cookie::Cookie::new(self.name.as_str(), "");
        if let Some(path) = self.path.as_deref() {
            cookie.set_path(path);
        }
        if let Some(domain) = self.domain.as_deref() {
            cookie.set_domain(domain);
        }
        if self.partitioned {
            cookie.set_partitioned(true);
        }
        cookie.make_removal();
        render_bounded(&cookie)
    }
}

/// A stable, secret-safe request-cookie lookup error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum RequestCookieError {
    /// The lookup name violates the cookie grammar.
    #[error("requested cookie name is invalid")]
    InvalidName,
    /// The lookup name exceeded its byte bound.
    #[error("requested cookie name exceeds the {limit_bytes}-byte limit")]
    NameTooLong {
        /// Maximum accepted lookup-name length.
        limit_bytes: usize,
    },
    /// At least one request cookie field could not be parsed.
    #[error("request Cookie header is malformed")]
    Malformed,
    /// More than one cookie used the requested name.
    #[error("request contains a duplicate cookie name")]
    Duplicate,
    #[error("request Cookie header exceeds the {limit}-pair limit")]
    /// The request declared more cookie pairs than the parser retains.
    TooManyPairs {
        /// Maximum retained pair count.
        limit: usize,
    },
    /// A request cookie value exceeded its bound.
    #[error("request cookie value exceeds the {limit_bytes}-byte limit")]
    ValueTooLong {
        /// Maximum accepted value length.
        limit_bytes: usize,
    },
    /// Aggregate retained cookie data exceeded its bound.
    #[error("request cookies exceed the {limit_bytes}-byte retained-data limit")]
    RetainedBytesExceeded {
        /// Maximum aggregate retained bytes.
        limit_bytes: usize,
    },
}

/// One borrowed request-cookie pair in declaration order.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct RequestCookie<'a> {
    name: &'a str,
    value: &'a str,
}

impl fmt::Debug for RequestCookie<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RequestCookie")
            .field("name", &"<redacted>")
            .field("name_bytes", &self.name.len())
            .field("value", &"<redacted>")
            .field("value_bytes", &self.value.len())
            .finish()
    }
}

impl<'a> RequestCookie<'a> {
    #[must_use]
    /// Returns the wire cookie name.
    pub const fn name(self) -> &'a str {
        self.name
    }

    #[must_use]
    /// Returns the wire value without percent-decoding.
    pub const fn value(self) -> &'a str {
        self.value
    }
}

#[derive(Clone, PartialEq, Eq)]
struct StoredRequestCookie {
    name: String,
    value: String,
}

/// Immutable, bounded view of every cookie declared by one request.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct RequestCookieJar {
    cookies: Vec<StoredRequestCookie>,
    retained_bytes: usize,
}

impl fmt::Debug for RequestCookieJar {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RequestCookieJar")
            .field("cookie_count", &self.cookies.len())
            .field("retained_bytes", &self.retained_bytes)
            .finish()
    }
}

impl RequestCookieJar {
    pub(crate) fn parse<'a>(
        field_values: impl IntoIterator<Item = &'a str>,
    ) -> Result<Self, RequestCookieError> {
        let mut cookies = Vec::new();
        let mut retained_bytes = 0usize;

        for field_value in field_values {
            if field_value.trim().is_empty()
                || field_value
                    .split(';')
                    .any(|segment| segment.trim().is_empty())
            {
                return Err(RequestCookieError::Malformed);
            }

            for parsed in ::cookie::Cookie::split_parse(field_value) {
                if cookies.len() == MAX_REQUEST_COOKIE_PAIRS {
                    return Err(RequestCookieError::TooManyPairs {
                        limit: MAX_REQUEST_COOKIE_PAIRS,
                    });
                }

                let parsed = parsed.map_err(|_| RequestCookieError::Malformed)?;
                validate_request_pair(parsed.name(), parsed.value())?;
                let pair_bytes = parsed
                    .name()
                    .len()
                    .checked_add(parsed.value().len())
                    .ok_or(RequestCookieError::RetainedBytesExceeded {
                        limit_bytes: MAX_REQUEST_COOKIE_RETAINED_BYTES,
                    })?;
                retained_bytes = retained_bytes.checked_add(pair_bytes).ok_or(
                    RequestCookieError::RetainedBytesExceeded {
                        limit_bytes: MAX_REQUEST_COOKIE_RETAINED_BYTES,
                    },
                )?;
                if retained_bytes > MAX_REQUEST_COOKIE_RETAINED_BYTES {
                    return Err(RequestCookieError::RetainedBytesExceeded {
                        limit_bytes: MAX_REQUEST_COOKIE_RETAINED_BYTES,
                    });
                }

                cookies.push(StoredRequestCookie {
                    name: parsed.name().to_owned(),
                    value: parsed.value().to_owned(),
                });
            }
        }

        Ok(Self {
            cookies,
            retained_bytes,
        })
    }

    /// Returns one wire value without percent-decoding it.
    ///
    /// A duplicate target name is an explicit error. Duplicates of unrelated
    /// names remain visible through [`Self::iter`] and do not alter this lookup.
    pub fn get(&self, name: &str) -> Result<Option<&str>, RequestCookieError> {
        validate_request_name(name)?;
        let mut matched = None;
        for cookie in &self.cookies {
            if cookie.name == name {
                if matched.is_some() {
                    return Err(RequestCookieError::Duplicate);
                }
                matched = Some(cookie.value.as_str());
            }
        }
        Ok(matched)
    }

    /// Iterates over all wire pairs in field/declaration order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = RequestCookie<'_>> + '_ {
        self.cookies.iter().map(|cookie| RequestCookie {
            name: cookie.name.as_str(),
            value: cookie.value.as_str(),
        })
    }

    #[must_use]
    /// Returns the number of retained cookie pairs.
    pub fn len(&self) -> usize {
        self.cookies.len()
    }

    #[must_use]
    /// Returns `true` when the request declared no cookies.
    pub fn is_empty(&self) -> bool {
        self.cookies.is_empty()
    }

    /// Creates an integrity/authenticity-verifying view.
    #[must_use]
    pub const fn signed<'a>(&'a self, key_ring: &'a CookieKeyRing) -> SignedRequestCookieJar<'a> {
        SignedRequestCookieJar {
            jar: self,
            key_ring,
        }
    }

    /// Creates an authenticated-confidentiality-verifying view.
    #[must_use]
    pub const fn private<'a>(&'a self, key_ring: &'a CookieKeyRing) -> PrivateRequestCookieJar<'a> {
        PrivateRequestCookieJar {
            jar: self,
            key_ring,
        }
    }
}

/// Secret-safe configuration error for secure-cookie keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum CookieKeyRingError {
    /// A secret does not contain enough key material.
    #[error("cookie key secret is shorter than the required {minimum_bytes} bytes")]
    SecretTooShort {
        /// Minimum accepted secret length.
        minimum_bytes: usize,
    },
    /// More previous keys were configured than the verification bound.
    #[error("cookie key ring exceeds the {limit}-previous-key limit")]
    TooManyPreviousKeys {
        /// Maximum previous-key count.
        limit: usize,
    },
    /// The primary or a previous key occurs more than once.
    #[error("cookie key ring contains a duplicate key")]
    DuplicateKey,
}

/// Application-supplied primary key plus a bounded previous-key verification set.
#[derive(Clone)]
pub struct CookieKeyRing {
    primary: ::cookie::Key,
    previous: Vec<::cookie::Key>,
}

impl fmt::Debug for CookieKeyRing {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CookieKeyRing")
            .field("primary", &"<redacted>")
            .field("previous_key_count", &self.previous.len())
            .finish()
    }
}

impl CookieKeyRing {
    /// Derives the primary cookie key from application-owned random secret bytes.
    pub fn new(primary_secret: &[u8]) -> Result<Self, CookieKeyRingError> {
        Ok(Self {
            primary: derive_cookie_key(primary_secret)?,
            previous: Vec::new(),
        })
    }

    /// Adds one previous secret in verification order.
    pub fn with_previous(mut self, previous_secret: &[u8]) -> Result<Self, CookieKeyRingError> {
        if self.previous.len() == MAX_COOKIE_PREVIOUS_KEYS {
            return Err(CookieKeyRingError::TooManyPreviousKeys {
                limit: MAX_COOKIE_PREVIOUS_KEYS,
            });
        }

        let previous = derive_cookie_key(previous_secret)?;
        if previous == self.primary || self.previous.iter().any(|key| key == &previous) {
            return Err(CookieKeyRingError::DuplicateKey);
        }
        self.previous.push(previous);
        Ok(self)
    }

    #[must_use]
    /// Returns the number of previous keys accepted for verification.
    pub fn previous_key_count(&self) -> usize {
        self.previous.len()
    }

    fn primary(&self) -> &::cookie::Key {
        &self.primary
    }

    fn keys(&self) -> impl Iterator<Item = (&::cookie::Key, bool)> {
        std::iter::once((&self.primary, false)).chain(self.previous.iter().map(|key| (key, true)))
    }
}

fn derive_cookie_key(secret: &[u8]) -> Result<::cookie::Key, CookieKeyRingError> {
    if secret.len() < MIN_COOKIE_KEY_BYTES {
        return Err(CookieKeyRingError::SecretTooShort {
            minimum_bytes: MIN_COOKIE_KEY_BYTES,
        });
    }
    Ok(::cookie::Key::derive_from(secret))
}

/// A verified/decrypted cookie value and its rotation signal.
#[derive(Clone, PartialEq, Eq)]
pub struct SecureCookieValue {
    value: String,
    needs_rotation: bool,
}

impl fmt::Debug for SecureCookieValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SecureCookieValue")
            .field("value", &"<redacted>")
            .field("value_bytes", &self.value.len())
            .field("needs_rotation", &self.needs_rotation)
            .finish()
    }
}

impl SecureCookieValue {
    #[must_use]
    /// Returns the verified or decrypted cookie value.
    pub fn value(&self) -> &str {
        &self.value
    }

    #[must_use]
    /// Reports whether the value was verified with a previous key.
    pub const fn needs_rotation(&self) -> bool {
        self.needs_rotation
    }
}

/// Secret-safe secure-cookie read error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum SecureCookieError {
    /// Request-cookie parsing or lookup failed.
    #[error(transparent)]
    Request(#[from] RequestCookieError),
    /// The signed or private payload did not authenticate.
    #[error("secure cookie payload failed authentication or decoding")]
    InvalidPayload,
}

/// Integrity/authenticity-verifying request-cookie view.
pub struct SignedRequestCookieJar<'a> {
    jar: &'a RequestCookieJar,
    key_ring: &'a CookieKeyRing,
}

impl fmt::Debug for SignedRequestCookieJar<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SignedRequestCookieJar")
            .field("cookie_count", &self.jar.len())
            .field("key_ring", &"<redacted>")
            .finish()
    }
}

impl SignedRequestCookieJar<'_> {
    /// Authenticates and returns the named signed cookie.
    pub fn get(&self, name: &str) -> Result<Option<SecureCookieValue>, SecureCookieError> {
        read_secure_cookie(self.jar, self.key_ring, name, SecureCookieKind::Signed)
    }
}

/// Authenticated-confidentiality-verifying request-cookie view.
pub struct PrivateRequestCookieJar<'a> {
    jar: &'a RequestCookieJar,
    key_ring: &'a CookieKeyRing,
}

impl fmt::Debug for PrivateRequestCookieJar<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PrivateRequestCookieJar")
            .field("cookie_count", &self.jar.len())
            .field("key_ring", &"<redacted>")
            .finish()
    }
}

impl PrivateRequestCookieJar<'_> {
    /// Authenticates, decrypts, and returns the named private cookie.
    pub fn get(&self, name: &str) -> Result<Option<SecureCookieValue>, SecureCookieError> {
        read_secure_cookie(self.jar, self.key_ring, name, SecureCookieKind::Private)
    }
}

#[derive(Clone, Copy)]
enum SecureCookieKind {
    Signed,
    Private,
}

fn read_secure_cookie(
    jar: &RequestCookieJar,
    key_ring: &CookieKeyRing,
    name: &str,
    kind: SecureCookieKind,
) -> Result<Option<SecureCookieValue>, SecureCookieError> {
    let Some(wire_value) = jar.get(name)? else {
        return Ok(None);
    };

    for (key, needs_rotation) in key_ring.keys() {
        let mut upstream = ::cookie::CookieJar::new();
        upstream.add_original(::cookie::Cookie::new(
            name.to_owned(),
            wire_value.to_owned(),
        ));
        let verified = match kind {
            SecureCookieKind::Signed => upstream.signed(key).get(name),
            SecureCookieKind::Private => upstream.private(key).get(name),
        };
        if let Some(cookie) = verified {
            validate_value(cookie.value()).map_err(|_| SecureCookieError::InvalidPayload)?;
            return Ok(Some(SecureCookieValue {
                value: cookie.value().to_owned(),
                needs_rotation,
            }));
        }
    }

    Err(SecureCookieError::InvalidPayload)
}

pub(crate) fn validate_request_name(name: &str) -> Result<(), RequestCookieError> {
    match validate_name(name) {
        Ok(()) => Ok(()),
        Err(ResponseCookieError::NameTooLong { limit_bytes }) => {
            Err(RequestCookieError::NameTooLong { limit_bytes })
        }
        Err(_) => Err(RequestCookieError::InvalidName),
    }
}

pub(crate) fn validate_request_pair(name: &str, value: &str) -> Result<(), RequestCookieError> {
    validate_name(name).map_err(|_| RequestCookieError::Malformed)?;
    if value.len() > MAX_COOKIE_VALUE_BYTES {
        return Err(RequestCookieError::ValueTooLong {
            limit_bytes: MAX_COOKIE_VALUE_BYTES,
        });
    }
    if !is_valid_cookie_value(value) {
        return Err(RequestCookieError::Malformed);
    }
    Ok(())
}

fn validate_name(name: &str) -> Result<(), ResponseCookieError> {
    if name.len() > MAX_COOKIE_NAME_BYTES {
        return Err(ResponseCookieError::NameTooLong {
            limit_bytes: MAX_COOKIE_NAME_BYTES,
        });
    }
    if name.is_empty() || !name.as_bytes().iter().copied().all(is_token_byte) {
        return Err(ResponseCookieError::InvalidName);
    }
    Ok(())
}

fn validate_value(value: &str) -> Result<(), ResponseCookieError> {
    if value.len() > MAX_COOKIE_VALUE_BYTES {
        return Err(ResponseCookieError::ValueTooLong {
            limit_bytes: MAX_COOKIE_VALUE_BYTES,
        });
    }
    if !is_valid_cookie_value(value) {
        return Err(ResponseCookieError::InvalidValue);
    }
    Ok(())
}

fn validate_path(path: &str) -> Result<(), ResponseCookieError> {
    if path.len() > MAX_COOKIE_PATH_BYTES {
        return Err(ResponseCookieError::PathTooLong {
            limit_bytes: MAX_COOKIE_PATH_BYTES,
        });
    }
    if !path.starts_with('/')
        || path
            .as_bytes()
            .iter()
            .any(|byte| *byte < 0x20 || *byte == 0x7f || *byte == b';')
    {
        return Err(ResponseCookieError::InvalidPath);
    }
    Ok(())
}

fn validate_domain(domain: &str) -> Result<(), ResponseCookieError> {
    if domain.len() > MAX_COOKIE_DOMAIN_BYTES {
        return Err(ResponseCookieError::DomainTooLong {
            limit_bytes: MAX_COOKIE_DOMAIN_BYTES,
        });
    }

    let normalized = domain.strip_prefix('.').unwrap_or(domain);
    if normalized.is_empty()
        || !domain.is_ascii()
        || domain.bytes().any(|byte| {
            byte.is_ascii_whitespace()
                || byte.is_ascii_control()
                || matches!(byte, b';' | b'/' | b'\\' | b':' | b'[' | b']')
        })
        || url::Host::parse(normalized).is_err()
    {
        return Err(ResponseCookieError::InvalidDomain);
    }
    Ok(())
}

fn is_valid_cookie_value(value: &str) -> bool {
    value.bytes().all(|byte| {
        matches!(
            byte,
            0x21 | 0x23..=0x2b | 0x2d..=0x3a | 0x3c..=0x5b | 0x5d..=0x7e
        )
    })
}

fn is_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}

fn render_bounded(cookie: &::cookie::Cookie<'_>) -> Result<String, ResponseCookieError> {
    let mut output = BoundedString::new(MAX_SET_COOKIE_BYTES);
    if write!(&mut output, "{cookie}").is_err() {
        return if output.exceeded {
            Err(ResponseCookieError::SerializedTooLarge {
                limit_bytes: MAX_SET_COOKIE_BYTES,
            })
        } else {
            Err(ResponseCookieError::SerializationFailed)
        };
    }
    Ok(output.value)
}

struct BoundedString {
    value: String,
    limit: usize,
    exceeded: bool,
}

impl BoundedString {
    fn new(limit: usize) -> Self {
        Self {
            value: String::with_capacity(limit.min(256)),
            limit,
            exceeded: false,
        }
    }
}

impl fmt::Write for BoundedString {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        let Some(next_len) = self.value.len().checked_add(value.len()) else {
            self.exceeded = true;
            return Err(fmt::Error);
        };
        if next_len > self.limit {
            self.exceeded = true;
            return Err(fmt::Error);
        }
        self.value.push_str(value);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PRIMARY_SECRET: [u8; MIN_COOKIE_KEY_BYTES] = [0x11; MIN_COOKIE_KEY_BYTES];
    const OLD_SECRET: [u8; MIN_COOKIE_KEY_BYTES] = [0x22; MIN_COOKIE_KEY_BYTES];

    fn request_jar_from_set_cookie(value: &str) -> RequestCookieJar {
        let parsed = ::cookie::Cookie::parse(value.to_owned()).unwrap();
        let request_value = format!("{}={}", parsed.name(), parsed.value());
        RequestCookieJar::parse([request_value.as_str()]).unwrap()
    }

    #[test]
    fn typed_cookie_uses_standard_codec_without_exposing_secrets_in_debug() {
        let cookie = ResponseCookie::new("session", "opaque-value")
            .unwrap()
            .secure(true)
            .http_only(true)
            .same_site(CookieSameSite::Lax)
            .path("/")
            .unwrap()
            .domain("example.com")
            .unwrap()
            .max_age(Duration::from_secs(300))
            .unwrap();

        let encoded = cookie.to_header_value().unwrap();
        assert!(encoded.starts_with("session=opaque-value"));
        for attribute in [
            "HttpOnly",
            "SameSite=Lax",
            "Secure",
            "Path=/",
            "Domain=example.com",
            "Max-Age=300",
        ] {
            assert!(
                encoded.contains(attribute),
                "missing {attribute}: {encoded}"
            );
        }

        let debug = format!("{cookie:?}");
        assert!(!debug.contains("session"));
        assert!(!debug.contains("opaque-value"));
    }

    #[test]
    fn same_site_none_is_rendered_with_secure_by_the_standard_codec() {
        let cookie = ResponseCookie::new("session", "opaque")
            .unwrap()
            .same_site(CookieSameSite::None)
            .secure(false);
        assert!(cookie.is_secure());
        let encoded = cookie.to_header_value().unwrap();
        assert!(encoded.contains("SameSite=None"));
        assert!(encoded.contains("Secure"));
    }

    #[test]
    fn expires_and_partitioned_are_typed_and_enforce_secure() {
        let expires = CookieExpires::from_unix_timestamp(1_893_456_000).unwrap();
        let cookie = ResponseCookie::new("session", "opaque")
            .unwrap()
            .secure(false)
            .partitioned(true)
            .secure(false)
            .max_age(Duration::from_secs(600))
            .unwrap()
            .expires(expires);

        assert!(cookie.is_secure());
        assert!(cookie.is_partitioned());
        assert_eq!(cookie.configured_expires(), Some(expires));
        let encoded = cookie.to_header_value().unwrap();
        let parsed = ::cookie::Cookie::parse(encoded).unwrap();
        assert_eq!(parsed.secure(), Some(true));
        assert_eq!(parsed.partitioned(), Some(true));
        assert_eq!(
            parsed.max_age(),
            Some(::cookie::time::Duration::seconds(600))
        );
        assert_eq!(parsed.expires_datetime(), Some(expires));
    }

    #[test]
    fn removal_is_explicit_and_preserves_cookie_scope() {
        let removal = CookieRemoval::new("session")
            .unwrap()
            .path("/")
            .unwrap()
            .domain("example.com")
            .unwrap();
        let encoded = removal.to_header_value().unwrap();

        assert!(encoded.starts_with("session="));
        assert!(encoded.contains("Path=/"));
        assert!(encoded.contains("Domain=example.com"));
        assert!(encoded.contains("Max-Age=0"));
        assert!(encoded.contains("Expires="));
    }

    #[test]
    fn partitioned_removal_preserves_partition_scope_and_secure_invariant() {
        let removal = CookieRemoval::new("session")
            .unwrap()
            .path("/")
            .unwrap()
            .partitioned(true);
        let encoded = removal.to_header_value().unwrap();
        let parsed = ::cookie::Cookie::parse(encoded).unwrap();

        assert_eq!(parsed.value(), "");
        assert_eq!(parsed.max_age(), Some(::cookie::time::Duration::ZERO));
        assert!(parsed.expires_datetime().is_some());
        assert_eq!(parsed.partitioned(), Some(true));
        assert_eq!(parsed.secure(), Some(true));
    }

    #[test]
    fn response_cookie_rejects_attribute_injection_and_oversized_input() {
        assert_eq!(
            ResponseCookie::new("session", "safe; HttpOnly").unwrap_err(),
            ResponseCookieError::InvalidValue
        );
        assert_eq!(
            ResponseCookie::new("session\r\n", "safe").unwrap_err(),
            ResponseCookieError::InvalidName
        );
        assert_eq!(
            ResponseCookie::new("session", "safe")
                .unwrap()
                .path("/safe; Domain=attacker.example")
                .unwrap_err(),
            ResponseCookieError::InvalidPath
        );
        assert_eq!(
            ResponseCookie::new("session", &"x".repeat(MAX_COOKIE_VALUE_BYTES + 1)).unwrap_err(),
            ResponseCookieError::ValueTooLong {
                limit_bytes: MAX_COOKIE_VALUE_BYTES
            }
        );
    }

    #[test]
    fn serialized_cookie_has_an_independent_browser_compatible_bound() {
        let cookie = ResponseCookie::new("session", &"x".repeat(MAX_COOKIE_VALUE_BYTES))
            .unwrap()
            .http_only(true);
        assert_eq!(
            cookie.to_header_value().unwrap_err(),
            ResponseCookieError::SerializedTooLarge {
                limit_bytes: MAX_SET_COOKIE_BYTES
            }
        );
    }

    #[test]
    fn request_cookie_jar_preserves_all_fields_duplicates_and_wire_order() {
        let jar = RequestCookieJar::parse([
            "theme=dark; session=opaque%2Ftoken",
            "session=second; locale=tr",
        ])
        .unwrap();

        let pairs = jar
            .iter()
            .map(|cookie| (cookie.name().to_owned(), cookie.value().to_owned()))
            .collect::<Vec<_>>();
        assert_eq!(
            pairs,
            vec![
                ("theme".to_string(), "dark".to_string()),
                ("session".to_string(), "opaque%2Ftoken".to_string()),
                ("session".to_string(), "second".to_string()),
                ("locale".to_string(), "tr".to_string()),
            ]
        );
        assert_eq!(jar.get("theme"), Ok(Some("dark")));
        assert_eq!(jar.get("session"), Err(RequestCookieError::Duplicate));

        let debug = format!("{jar:?} {:?}", jar.iter().next().unwrap());
        for secret in ["theme", "dark", "session", "opaque%2Ftoken"] {
            assert!(!debug.contains(secret));
        }
    }

    #[test]
    fn request_cookie_jar_rejects_malformed_pair_count_and_retained_bytes() {
        assert_eq!(
            RequestCookieJar::parse(["session=opaque; missing-pair"]),
            Err(RequestCookieError::Malformed)
        );

        let too_many = (0..=MAX_REQUEST_COOKIE_PAIRS)
            .map(|index| format!("c{index}=v"))
            .collect::<Vec<_>>()
            .join("; ");
        assert_eq!(
            RequestCookieJar::parse([too_many.as_str()]),
            Err(RequestCookieError::TooManyPairs {
                limit: MAX_REQUEST_COOKIE_PAIRS
            })
        );

        let large_value = "x".repeat(MAX_COOKIE_VALUE_BYTES);
        let retained = (0..17)
            .map(|index| format!("c{index}={large_value}"))
            .collect::<Vec<_>>()
            .join("; ");
        assert_eq!(
            RequestCookieJar::parse([retained.as_str()]),
            Err(RequestCookieError::RetainedBytesExceeded {
                limit_bytes: MAX_REQUEST_COOKIE_RETAINED_BYTES
            })
        );
    }

    #[test]
    fn cookie_key_ring_is_bounded_and_secret_safe() {
        assert_eq!(
            CookieKeyRing::new(&[0x33; MIN_COOKIE_KEY_BYTES - 1]).unwrap_err(),
            CookieKeyRingError::SecretTooShort {
                minimum_bytes: MIN_COOKIE_KEY_BYTES
            }
        );

        let mut ring = CookieKeyRing::new(&PRIMARY_SECRET).unwrap();
        assert_eq!(
            ring.clone().with_previous(&PRIMARY_SECRET).unwrap_err(),
            CookieKeyRingError::DuplicateKey
        );
        for byte in 2..=u8::try_from(MAX_COOKIE_PREVIOUS_KEYS + 1).unwrap() {
            ring = ring.with_previous(&[byte; MIN_COOKIE_KEY_BYTES]).unwrap();
        }
        assert_eq!(ring.previous_key_count(), MAX_COOKIE_PREVIOUS_KEYS);
        assert_eq!(
            ring.with_previous(&[0xfe; MIN_COOKIE_KEY_BYTES])
                .unwrap_err(),
            CookieKeyRingError::TooManyPreviousKeys {
                limit: MAX_COOKIE_PREVIOUS_KEYS
            }
        );

        let debug = format!("{:?}", CookieKeyRing::new(&PRIMARY_SECRET).unwrap());
        assert!(!debug.contains("17"));
        assert!(!debug.contains("11, 11"));
    }

    #[test]
    fn signed_cookie_verifies_tampering_and_previous_key_rotation() {
        let old_ring = CookieKeyRing::new(&OLD_SECRET).unwrap();
        let current_ring = CookieKeyRing::new(&PRIMARY_SECRET)
            .unwrap()
            .with_previous(&OLD_SECRET)
            .unwrap();
        let cookie = ResponseCookie::new("session", "old-value")
            .unwrap()
            .http_only(true);

        let old_wire = cookie.to_signed_header_value(&old_ring).unwrap();
        let old_jar = request_jar_from_set_cookie(&old_wire);
        let verified = old_jar
            .signed(&current_ring)
            .get("session")
            .unwrap()
            .unwrap();
        assert_eq!(verified.value(), "old-value");
        assert!(verified.needs_rotation());

        let reissued = ResponseCookie::new("session", verified.value())
            .unwrap()
            .to_signed_header_value(&current_ring)
            .unwrap();
        let current_jar = request_jar_from_set_cookie(&reissued);
        let current = current_jar
            .signed(&current_ring)
            .get("session")
            .unwrap()
            .unwrap();
        assert_eq!(current.value(), "old-value");
        assert!(!current.needs_rotation());

        let parsed = ::cookie::Cookie::parse(reissued).unwrap();
        let mut tampered = parsed.value().to_owned();
        tampered.push('x');
        let tampered_header = format!("session={tampered}");
        let tampered_jar = RequestCookieJar::parse([tampered_header.as_str()]).unwrap();
        assert_eq!(
            tampered_jar.signed(&current_ring).get("session"),
            Err(SecureCookieError::InvalidPayload)
        );
    }

    #[test]
    fn private_cookie_confidentiality_tampering_and_rotation_are_enforced() {
        let old_ring = CookieKeyRing::new(&OLD_SECRET).unwrap();
        let current_ring = CookieKeyRing::new(&PRIMARY_SECRET)
            .unwrap()
            .with_previous(&OLD_SECRET)
            .unwrap();
        let wire = ResponseCookie::new("session", "private-value")
            .unwrap()
            .to_private_header_value(&old_ring)
            .unwrap();
        assert!(!wire.contains("private-value"));

        let jar = request_jar_from_set_cookie(&wire);
        let verified = jar.private(&current_ring).get("session").unwrap().unwrap();
        assert_eq!(verified.value(), "private-value");
        assert!(verified.needs_rotation());

        let reissued = ResponseCookie::new("session", verified.value())
            .unwrap()
            .to_private_header_value(&current_ring)
            .unwrap();
        let current_jar = request_jar_from_set_cookie(&reissued);
        let current = current_jar
            .private(&current_ring)
            .get("session")
            .unwrap()
            .unwrap();
        assert_eq!(current.value(), "private-value");
        assert!(!current.needs_rotation());

        let parsed = ::cookie::Cookie::parse(wire).unwrap();
        let mut tampered = parsed.value().as_bytes().to_vec();
        let final_byte = tampered.last_mut().unwrap();
        *final_byte = if *final_byte == b'A' { b'B' } else { b'A' };
        let tampered = String::from_utf8(tampered).unwrap();
        let tampered_header = format!("session={tampered}");
        let tampered_jar = RequestCookieJar::parse([tampered_header.as_str()]).unwrap();
        let error = tampered_jar
            .private(&current_ring)
            .get("session")
            .unwrap_err();
        assert_eq!(error, SecureCookieError::InvalidPayload);
        let error_output = format!("{error:?} {error}");
        assert!(!error_output.contains("session"));
        assert!(!error_output.contains(&tampered));

        let debug = format!("{verified:?} {:?}", jar.private(&current_ring));
        assert!(!debug.contains("private-value"));
        assert!(!debug.contains("session"));
    }

    #[test]
    fn secure_cookie_duplicate_and_oversized_output_fail_closed() {
        let ring = CookieKeyRing::new(&PRIMARY_SECRET).unwrap();
        let signed = ResponseCookie::new("session", "value")
            .unwrap()
            .to_signed_header_value(&ring)
            .unwrap();
        let parsed = ::cookie::Cookie::parse(signed).unwrap();
        let duplicates = format!("session={}; session={}", parsed.value(), parsed.value());
        let jar = RequestCookieJar::parse([duplicates.as_str()]).unwrap();
        assert_eq!(
            jar.signed(&ring).get("session"),
            Err(SecureCookieError::Request(RequestCookieError::Duplicate))
        );

        let oversized =
            ResponseCookie::new("session", &"x".repeat(MAX_COOKIE_VALUE_BYTES)).unwrap();
        assert_eq!(
            oversized.to_signed_header_value(&ring).unwrap_err(),
            ResponseCookieError::SerializedTooLarge {
                limit_bytes: MAX_SET_COOKIE_BYTES
            }
        );
        assert_eq!(
            oversized.to_private_header_value(&ring).unwrap_err(),
            ResponseCookieError::SerializedTooLarge {
                limit_bytes: MAX_SET_COOKIE_BYTES
            }
        );
    }
}
