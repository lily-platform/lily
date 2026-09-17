//! Opt-in CSRF policy and Lily-private enforcement runtime.
//!
//! Applications compose global enforcement through `lily_http_api::AppBuilder`
//! or opt selected routes into `lily_http_api::CsrfGuard`. Tower and
//! RustCrypto implementation types deliberately remain private.

use crate::{MiddlewareConfigError, MiddlewareErrorCode};
use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, Mac};
use http::{
    HeaderName, HeaderValue, Method, Request as TowerRequest, Response as TowerResponse, Uri,
    header::{HOST, ORIGIN},
};
use lily_web_core::{
    CookieRemoval, CookieSameSite, FormData, Request, RequestCookieError, RequestExt,
    ResponseCookie, ResponseCookieError,
};
use rand::{RngCore, rngs::OsRng};
use sha2::Sha256;
use std::{
    convert::Infallible,
    fmt,
    future::{Ready, ready},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use subtle::ConstantTimeEq;
use tower_http::csrf::{CsrfLayer, ProtectionError, ProtectionErrorKind};
use tower_layer::Layer;
use tower_service::Service;
use url::Url;
use zeroize::Zeroizing;

/// Maximum exact trusted origins retained by one policy.
const MAX_CSRF_TRUSTED_ORIGINS: usize = 64;
/// Maximum exact method/path bypasses retained by one policy.
const MAX_CSRF_BYPASSES: usize = 32;
/// Maximum previous signed-token verification keys retained during rotation.
const MAX_CSRF_PREVIOUS_KEYS: usize = 4;
/// Maximum aggregate policy metadata retained in memory.
const MAX_CSRF_POLICY_BYTES: usize = 32 * 1024;
/// Maximum canonical trusted-origin byte length.
const MAX_CSRF_ORIGIN_BYTES: usize = 2 * 1024;
/// Maximum exact bypass-path byte length.
const MAX_CSRF_PATH_BYTES: usize = 2 * 1024;
/// Maximum encoded CSRF token byte length accepted at the request boundary.
const MAX_CSRF_TOKEN_BYTES: usize = 512;
/// Maximum opaque application session-binding byte length.
const MAX_CSRF_BINDING_BYTES: usize = 4 * 1024;
/// Maximum URL-encoded form body inspected for an opt-in token field.
const MAX_CSRF_FORM_BODY_BYTES: usize = 64 * 1024;
/// Minimum HMAC or store-key derivation secret length.
const MIN_CSRF_SECRET_BYTES: usize = 32;
/// Maximum retained CSRF secret length.
const MAX_CSRF_SECRET_BYTES: usize = 128;
/// Maximum lifetime accepted for an issued token.
const MAX_CSRF_TOKEN_TTL: Duration = Duration::from_secs(7 * 24 * 60 * 60);
/// Maximum deadline accepted for one application token-store operation.
pub const MAX_CSRF_STORE_TIMEOUT: Duration = Duration::from_secs(30);

const DEFAULT_CSRF_TOKEN_TTL: Duration = Duration::from_secs(60 * 60);
const DEFAULT_CSRF_STORE_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_CSRF_COOKIE_NAME: &str = "__Host-lily-csrf";
const DEFAULT_CSRF_HEADER_NAME: &str = "x-csrf-token";
const CSRF_TOKEN_VERSION: &str = "v1";
const CSRF_MAC_DOMAIN: &[u8] = b"lily.csrf.signed-double-submit.v1\0";
const CSRF_STORE_KEY_DOMAIN: &[u8] = b"lily.csrf.synchronizer.store-key.v1\0";
const CSRF_NONCE_BYTES: usize = 32;
const CSRF_MAC_BYTES: usize = 32;
/// Fixed width of the opaque key passed to an application token store.
pub const CSRF_STORE_KEY_BYTES: usize = 32;

type HmacSha256 = Hmac<Sha256>;

fn csrf_code(code: &'static str) -> MiddlewareErrorCode {
    MiddlewareErrorCode::new(code).expect("built-in CSRF codes are valid bounded labels")
}

fn csrf_error(code: &'static str) -> MiddlewareConfigError {
    MiddlewareConfigError::middleware(csrf_code(code))
}

/// Selects the enforcement performed before user middleware and route guards.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CsrfMode {
    /// Browser origin and Fetch Metadata checks only.
    CrossOrigin,
    /// Stateless HMAC-signed double-submit token enforcement.
    SignedDoubleSubmit,
    /// Cross-origin checks followed by signed double-submit verification.
    DefenseInDepth,
    /// Stateful, session-bound synchronizer-token enforcement.
    Synchronizer,
    /// Cross-origin checks followed by synchronizer-token verification.
    SynchronizerDefenseInDepth,
}

impl CsrfMode {
    const fn uses_cross_origin(self) -> bool {
        matches!(
            self,
            Self::CrossOrigin | Self::DefenseInDepth | Self::SynchronizerDefenseInDepth
        )
    }

    const fn uses_token(self) -> bool {
        !matches!(self, Self::CrossOrigin)
    }

    const fn uses_signed_token(self) -> bool {
        matches!(self, Self::SignedDoubleSubmit | Self::DefenseInDepth)
    }

    const fn uses_synchronizer_token(self) -> bool {
        matches!(self, Self::Synchronizer | Self::SynchronizerDefenseInDepth)
    }
}

/// Redacted application-owned key material used for HMAC signing.
#[derive(Clone, PartialEq, Eq)]
pub struct CsrfSecret(Zeroizing<Vec<u8>>);

impl CsrfSecret {
    /// Copies 32..=128 bytes of secret material into zeroizing storage.
    pub fn new(secret: impl AsRef<[u8]>) -> Result<Self, CsrfSecretError> {
        let secret = secret.as_ref();
        if secret.len() < MIN_CSRF_SECRET_BYTES {
            return Err(CsrfSecretError::TooShort {
                minimum_bytes: MIN_CSRF_SECRET_BYTES,
            });
        }
        if secret.len() > MAX_CSRF_SECRET_BYTES {
            return Err(CsrfSecretError::TooLong {
                maximum_bytes: MAX_CSRF_SECRET_BYTES,
            });
        }
        Ok(Self(Zeroizing::new(secret.to_vec())))
    }

    fn bytes(&self) -> &[u8] {
        self.0.as_slice()
    }
}

impl fmt::Debug for CsrfSecret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CsrfSecret")
            .field("value", &"<redacted>")
            .field("bytes", &self.0.len())
            .finish()
    }
}

/// Failure returned when CSRF secret material violates the bounded contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CsrfSecretError {
    /// Secret material is below the cryptographic minimum.
    #[error("CSRF secret is shorter than the {minimum_bytes}-byte minimum")]
    TooShort {
        /// Required minimum length.
        minimum_bytes: usize,
    },
    /// Secret material exceeds the retained-memory bound.
    #[error("CSRF secret exceeds the {maximum_bytes}-byte limit")]
    TooLong {
        /// Maximum accepted length.
        maximum_bytes: usize,
    },
}

/// Opaque per-session binding supplied by the application.
///
/// Applications using [`CsrfRequestLocalBinding`] construct this value only
/// after their session middleware has authenticated or otherwise validated the
/// current anonymous/authenticated session, then publish it through
/// [`Request::local_mut`]. Lily does not decide whether a session is valid.
#[derive(Clone, PartialEq, Eq)]
pub struct CsrfSessionId(Zeroizing<Vec<u8>>);

impl CsrfSessionId {
    /// Copies a 1..=4096-byte application session identity.
    pub fn new(value: impl AsRef<[u8]>) -> Result<Self, CsrfBindingError> {
        let value = value.as_ref();
        if value.is_empty() {
            return Err(CsrfBindingError::Empty);
        }
        if value.len() > MAX_CSRF_BINDING_BYTES {
            return Err(CsrfBindingError::TooLong {
                maximum_bytes: MAX_CSRF_BINDING_BYTES,
            });
        }
        Ok(Self(Zeroizing::new(value.to_vec())))
    }

    fn bytes(&self) -> &[u8] {
        self.0.as_slice()
    }
}

impl fmt::Debug for CsrfSessionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CsrfSessionId")
            .field("value", &"<redacted>")
            .field("bytes", &self.0.len())
            .finish()
    }
}

/// Failure returned while deriving an opaque CSRF session binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CsrfBindingError {
    /// The selected session identity is empty.
    #[error("CSRF session binding is empty")]
    Empty,
    /// The selected session identity exceeds its retained-memory bound.
    #[error("CSRF session binding exceeds the {maximum_bytes}-byte limit")]
    TooLong {
        /// Maximum accepted binding length.
        maximum_bytes: usize,
    },
    /// The selected cookie or request-local source is malformed.
    #[error("CSRF session binding source is malformed")]
    Malformed,
    /// More than one candidate session identity was supplied.
    #[error("CSRF session binding source is ambiguous")]
    Ambiguous,
}

/// Opaque, fixed-width storage key derived by Lily from the application
/// session binding. Raw session identifiers are never passed to a token store.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct CsrfStoreKey([u8; CSRF_STORE_KEY_BYTES]);

impl CsrfStoreKey {
    /// Returns the stable binary key an application store may persist or
    /// encode for its own backend.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; CSRF_STORE_KEY_BYTES] {
        &self.0
    }
}

impl fmt::Debug for CsrfStoreKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CsrfStoreKey")
            .field("value", &"<redacted>")
            .field("bytes", &CSRF_STORE_KEY_BYTES)
            .finish()
    }
}

/// Validated synchronizer-token record exchanged with an application store.
///
/// Its `Debug` output is redacted. Store adapters reconstruct values with
/// [`Self::from_parts`], which rejects non-canonical or oversized token data.
#[derive(Clone, PartialEq, Eq)]
pub struct StoredCsrfToken {
    token: CsrfToken,
    expires_at_unix_seconds: u64,
}

impl StoredCsrfToken {
    /// Reconstructs one validated record returned by an external store.
    pub fn from_parts(
        token: &str,
        expires_at_unix_seconds: u64,
    ) -> Result<Self, CsrfTokenStoreError> {
        if expires_at_unix_seconds == 0 || !valid_synchronizer_token(token) {
            return Err(CsrfTokenStoreError::CorruptData);
        }
        Ok(Self {
            token: CsrfToken(token.to_owned()),
            expires_at_unix_seconds,
        })
    }

    /// Returns the public token serialization without transferring ownership.
    #[must_use]
    pub fn token(&self) -> &str {
        self.token.as_str()
    }

    /// Returns the absolute Unix expiry used for store-side eviction.
    #[must_use]
    pub const fn expires_at_unix_seconds(&self) -> u64 {
        self.expires_at_unix_seconds
    }
}

impl fmt::Debug for StoredCsrfToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoredCsrfToken")
            .field("token", &"<redacted>")
            .field("expires_at_unix_seconds", &"<redacted>")
            .finish()
    }
}

/// Bounded token-store failure categories. Implementations must not embed
/// backend payloads, credentials, keys, or session values in these variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum CsrfTokenStoreError {
    /// The store or its backing service cannot currently serve requests.
    #[error("CSRF token store is unavailable")]
    Unavailable,
    /// The store exceeded its own operation deadline.
    #[error("CSRF token store operation timed out")]
    Timeout,
    /// The store cannot retain another token record.
    #[error("CSRF token store reached its capacity")]
    Capacity,
    /// The store returned a token record that failed Lily validation.
    #[error("CSRF token store returned corrupt data")]
    CorruptData,
    /// The store failed without a more precise safe category.
    #[error("CSRF token store operation failed")]
    Internal,
}

/// Application-supplied, storage-independent synchronizer-token contract.
///
/// `load_or_insert` must atomically return a still-live existing value or
/// install and return `candidate` when the key is absent/expired. `replace`
/// atomically overwrites the current value. `revoke` is idempotent. Lily wraps
/// every call in the policy's bounded timeout.
#[async_trait]
pub trait CsrfTokenStore: Send + Sync + 'static {
    /// Atomically returns a live record or inserts the supplied candidate.
    async fn load_or_insert(
        &self,
        key: &CsrfStoreKey,
        candidate: StoredCsrfToken,
        now_unix_seconds: u64,
    ) -> Result<StoredCsrfToken, CsrfTokenStoreError>;

    /// Loads the record associated with an opaque store key, when present.
    async fn load(
        &self,
        key: &CsrfStoreKey,
    ) -> Result<Option<StoredCsrfToken>, CsrfTokenStoreError>;

    /// Atomically replaces the record associated with an opaque store key.
    async fn replace(
        &self,
        key: &CsrfStoreKey,
        replacement: StoredCsrfToken,
    ) -> Result<(), CsrfTokenStoreError>;

    /// Idempotently removes the record associated with an opaque store key.
    async fn revoke(&self, key: &CsrfStoreKey) -> Result<(), CsrfTokenStoreError>;
}

/// Application hook corresponding to NestJS `getSessionIdentifier(req)`.
///
/// Implementations must return an opaque identifier that changes when the
/// authenticated or anonymous application session is rotated. Cookie-backed
/// applications can derive it from the immutable request boundary. Applications
/// that validate/load sessions before CSRF enforcement can instead publish an
/// exact [`CsrfSessionId`] to request-local state and use
/// [`CsrfRequestLocalBinding`].
pub trait CsrfSessionBinding: Send + Sync + 'static {
    /// Derives the current validated session identity, or reports its absence.
    fn session_id(&self, request: &Request) -> Result<Option<CsrfSessionId>, CsrfBindingError>;
}

/// Reads an exact, application-verified [`CsrfSessionId`] from request-local
/// state.
///
/// In global CSRF mode, the application's registered session middleware must
/// validate/load the session, insert the typed identifier with
/// `request.local_mut().insert(session_id)`, and only then call `next`. A
/// missing value returns `None`; token issuance and unsafe-request enforcement
/// convert that absence into their existing fail-closed binding errors.
///
/// This adapter never parses a raw identifier. Empty or oversized identifiers
/// cannot enter the map through [`CsrfSessionId::new`], and unrelated session
/// DTO types are intentionally ignored. The application remains responsible
/// for publishing a replacement identifier when its session rotates.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CsrfRequestLocalBinding;

impl CsrfRequestLocalBinding {
    /// Creates the stateless request-local binding adapter.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl CsrfSessionBinding for CsrfRequestLocalBinding {
    fn session_id(&self, request: &Request) -> Result<Option<CsrfSessionId>, CsrfBindingError> {
        Ok(request.local().get::<CsrfSessionId>().cloned())
    }
}

/// Ready-to-use binding that reads one exact, fail-closed session cookie.
#[derive(Clone, PartialEq, Eq)]
pub struct CsrfSessionCookieBinding {
    cookie_name: String,
}

impl CsrfSessionCookieBinding {
    /// Creates a binding for one syntactically valid cookie name.
    pub fn new(cookie_name: &str) -> Result<Self, CsrfBindingError> {
        lily_web_core::ResponseCookie::new(cookie_name, "binding-probe")
            .map_err(|_| CsrfBindingError::Malformed)?;
        Ok(Self {
            cookie_name: cookie_name.to_owned(),
        })
    }
}

impl fmt::Debug for CsrfSessionCookieBinding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CsrfSessionCookieBinding")
            .field("cookie_name", &"<redacted>")
            .field("cookie_name_bytes", &self.cookie_name.len())
            .finish()
    }
}

impl CsrfSessionBinding for CsrfSessionCookieBinding {
    fn session_id(&self, request: &Request) -> Result<Option<CsrfSessionId>, CsrfBindingError> {
        request
            .cookie(&self.cookie_name)
            .map_err(map_binding_cookie_error)?
            .map(CsrfSessionId::new)
            .transpose()
    }
}

fn map_binding_cookie_error(error: RequestCookieError) -> CsrfBindingError {
    match error {
        RequestCookieError::Duplicate => CsrfBindingError::Ambiguous,
        RequestCookieError::ValueTooLong { .. } => CsrfBindingError::TooLong {
            maximum_bytes: MAX_CSRF_BINDING_BYTES,
        },
        _ => CsrfBindingError::Malformed,
    }
}

/// Cookie attributes used for the signed token copy.
#[derive(Clone, PartialEq, Eq)]
pub struct CsrfCookiePolicy {
    name: String,
    secure: bool,
    http_only: bool,
    same_site: CookieSameSite,
    path: String,
}

impl CsrfCookiePolicy {
    /// Creates secure, HTTP-only, strict same-site cookie attributes.
    pub fn new(name: &str) -> Result<Self, MiddlewareConfigError> {
        ResponseCookie::new(name, "csrf-probe")
            .map_err(|_| csrf_error("CSRF_COOKIE_NAME_INVALID"))?;
        Ok(Self {
            name: name.to_owned(),
            secure: true,
            http_only: true,
            same_site: CookieSameSite::Strict,
            path: "/".to_string(),
        })
    }

    /// Enables or disables the cookie `Secure` attribute.
    #[must_use]
    pub fn secure(mut self, enabled: bool) -> Self {
        self.secure = enabled;
        self
    }

    /// Enables or disables the cookie `HttpOnly` attribute.
    #[must_use]
    pub fn http_only(mut self, enabled: bool) -> Self {
        self.http_only = enabled;
        self
    }

    /// Selects the cookie `SameSite` policy.
    #[must_use]
    pub fn same_site(mut self, same_site: CookieSameSite) -> Self {
        self.same_site = same_site;
        self
    }

    /// Selects and validates the cookie path.
    pub fn path(mut self, path: &str) -> Result<Self, MiddlewareConfigError> {
        ResponseCookie::new(&self.name, "csrf-probe")
            .map_err(|_| csrf_error("CSRF_COOKIE_INVALID"))?
            .path(path)
            .map_err(|_| csrf_error("CSRF_COOKIE_PATH_INVALID"))?;
        self.path = path.to_owned();
        Ok(self)
    }

    fn validate(&self, ttl: Duration) -> Result<(), MiddlewareConfigError> {
        if self.name.starts_with("__Host-") && (!self.secure || self.path != "/") {
            return Err(csrf_error("CSRF_HOST_COOKIE_INVALID"));
        }
        if self.same_site == CookieSameSite::None && !self.secure {
            return Err(csrf_error("CSRF_SAMESITE_NONE_INSECURE"));
        }
        self.response_cookie("csrf-probe", ttl)
            .map(|_| ())
            .map_err(|_| csrf_error("CSRF_COOKIE_INVALID"))
    }

    fn response_cookie(
        &self,
        value: &str,
        ttl: Duration,
    ) -> Result<ResponseCookie, ResponseCookieError> {
        ResponseCookie::new(&self.name, value)?
            .secure(self.secure)
            .http_only(self.http_only)
            .same_site(self.same_site)
            .path(&self.path)?
            .max_age(ttl)
    }

    fn removal(&self) -> Result<CookieRemoval, ResponseCookieError> {
        CookieRemoval::new(&self.name)?.path(&self.path)
    }
}

impl Default for CsrfCookiePolicy {
    fn default() -> Self {
        Self::new(DEFAULT_CSRF_COOKIE_NAME).expect("the built-in CSRF cookie name is valid")
    }
}

impl fmt::Debug for CsrfCookiePolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CsrfCookiePolicy")
            .field("name", &"<redacted>")
            .field("name_bytes", &self.name.len())
            .field("secure", &self.secure)
            .field("http_only", &self.http_only)
            .field("same_site", &self.same_site)
            .field("path", &"<redacted>")
            .field("path_bytes", &self.path.len())
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CsrfBypass {
    method: String,
    path: String,
}

#[derive(Clone)]
struct SignedDoubleSubmitPolicy {
    primary_key: CsrfSecret,
    previous_keys: Vec<CsrfSecret>,
    binding: Arc<dyn CsrfSessionBinding>,
    cookie: CsrfCookiePolicy,
    token_header: String,
    form_field: Option<String>,
    ttl: Duration,
}

impl fmt::Debug for SignedDoubleSubmitPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SignedDoubleSubmitPolicy")
            .field("primary_key", &"<redacted>")
            .field("previous_key_count", &self.previous_keys.len())
            .field("binding", &"<application-owned>")
            .field("cookie", &self.cookie)
            .field("token_header", &self.token_header)
            .field("form_field_configured", &self.form_field.is_some())
            .field("ttl", &self.ttl)
            .finish()
    }
}

#[derive(Clone)]
struct SynchronizerPolicy {
    key_derivation_secret: CsrfSecret,
    binding: Arc<dyn CsrfSessionBinding>,
    store: Arc<dyn CsrfTokenStore>,
    token_header: String,
    form_field: Option<String>,
    ttl: Duration,
    store_timeout: Duration,
}

impl fmt::Debug for SynchronizerPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SynchronizerPolicy")
            .field("key_derivation_secret", &"<redacted>")
            .field("binding", &"<application-owned>")
            .field("store", &"<application-owned>")
            .field("token_header", &self.token_header)
            .field("form_field_configured", &self.form_field.is_some())
            .field("ttl", &self.ttl)
            .field("store_timeout", &self.store_timeout)
            .finish()
    }
}

/// Immutable, bounded application-wide CSRF policy.
///
/// CSRF is opt-in and applies only to `GET`/`HEAD`/`OPTIONS`-unsafe methods.
/// CORS and `SameSite` cookie attributes are complementary controls, not a
/// replacement for this policy. Applications must not implement state changes
/// in safe-method handlers.
#[derive(Clone)]
pub struct CsrfPolicy {
    mode: CsrfMode,
    trusted_origins: Vec<String>,
    bypasses: Vec<CsrfBypass>,
    signed: Option<SignedDoubleSubmitPolicy>,
    synchronizer: Option<SynchronizerPolicy>,
    invalid: Option<MiddlewareErrorCode>,
}

impl CsrfPolicy {
    /// Uses the upstream `tower-http` browser-signal policy.
    ///
    /// Unsafe requests with neither `Sec-Fetch-Site` nor `Origin` are accepted
    /// as same-origin or non-browser traffic, matching upstream semantics. A
    /// reverse proxy must preserve the effective `Host` and must not strip
    /// browser `Origin`; deployments that cannot guarantee this should use
    /// [`Self::defense_in_depth`] or [`Self::signed_double_submit`].
    #[must_use]
    pub const fn cross_origin() -> Self {
        Self {
            mode: CsrfMode::CrossOrigin,
            trusted_origins: Vec::new(),
            bypasses: Vec::new(),
            signed: None,
            synchronizer: None,
            invalid: None,
        }
    }

    /// Requires a signed token bound to an application-owned session identity.
    ///
    /// This profile does not create, rotate, or invalidate that session.
    #[must_use]
    pub fn signed_double_submit<B>(primary_key: CsrfSecret, binding: B) -> Self
    where
        B: CsrfSessionBinding,
    {
        Self::with_signed(CsrfMode::SignedDoubleSubmit, primary_key, Arc::new(binding))
    }

    /// Runs cross-origin enforcement before signed-token verification.
    #[must_use]
    pub fn defense_in_depth<B>(primary_key: CsrfSecret, binding: B) -> Self
    where
        B: CsrfSessionBinding,
    {
        Self::with_signed(CsrfMode::DefenseInDepth, primary_key, Arc::new(binding))
    }

    fn with_signed(
        mode: CsrfMode,
        primary_key: CsrfSecret,
        binding: Arc<dyn CsrfSessionBinding>,
    ) -> Self {
        Self {
            mode,
            trusted_origins: Vec::new(),
            bypasses: Vec::new(),
            signed: Some(SignedDoubleSubmitPolicy {
                primary_key,
                previous_keys: Vec::new(),
                binding,
                cookie: CsrfCookiePolicy::default(),
                token_header: DEFAULT_CSRF_HEADER_NAME.to_string(),
                form_field: None,
                ttl: DEFAULT_CSRF_TOKEN_TTL,
            }),
            synchronizer: None,
            invalid: None,
        }
    }

    /// Requires a stateful synchronizer token bound to an application-owned
    /// session identity and persisted by the supplied store.
    #[must_use]
    pub fn synchronizer<B>(
        key_derivation_secret: CsrfSecret,
        binding: B,
        store: Arc<dyn CsrfTokenStore>,
    ) -> Self
    where
        B: CsrfSessionBinding,
    {
        Self::with_synchronizer(
            CsrfMode::Synchronizer,
            key_derivation_secret,
            Arc::new(binding),
            store,
        )
    }

    /// Runs cross-origin enforcement before stateful token verification.
    #[must_use]
    pub fn synchronizer_defense_in_depth<B>(
        key_derivation_secret: CsrfSecret,
        binding: B,
        store: Arc<dyn CsrfTokenStore>,
    ) -> Self
    where
        B: CsrfSessionBinding,
    {
        Self::with_synchronizer(
            CsrfMode::SynchronizerDefenseInDepth,
            key_derivation_secret,
            Arc::new(binding),
            store,
        )
    }

    fn with_synchronizer(
        mode: CsrfMode,
        key_derivation_secret: CsrfSecret,
        binding: Arc<dyn CsrfSessionBinding>,
        store: Arc<dyn CsrfTokenStore>,
    ) -> Self {
        Self {
            mode,
            trusted_origins: Vec::new(),
            bypasses: Vec::new(),
            signed: None,
            synchronizer: Some(SynchronizerPolicy {
                key_derivation_secret,
                binding,
                store,
                token_header: DEFAULT_CSRF_HEADER_NAME.to_string(),
                form_field: None,
                ttl: DEFAULT_CSRF_TOKEN_TTL,
                store_timeout: DEFAULT_CSRF_STORE_TIMEOUT,
            }),
            invalid: None,
        }
    }

    /// Replaces the exact trusted-origin list for a cross-origin profile.
    /// Calling this on the token-only profile is rejected as inert config. At
    /// most 64 canonical origins of at most 2 KiB each are retained, subject to
    /// the complete policy's 32 KiB metadata budget.
    #[must_use]
    pub fn trust_origins<I, S>(mut self, origins: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        if self.invalid.is_some() {
            return self;
        }
        if !self.mode.uses_cross_origin() {
            return self.with_error("CSRF_CROSS_ORIGIN_MODE_REQUIRED");
        }
        self.trusted_origins.clear();
        let mut retained_bytes = 0usize;
        for origin in origins {
            if self.trusted_origins.len() == MAX_CSRF_TRUSTED_ORIGINS {
                return self.with_error("CSRF_ORIGIN_LIMIT");
            }
            let origin = origin.as_ref();
            retained_bytes = retained_bytes.saturating_add(origin.len());
            if origin.is_empty()
                || origin.len() > MAX_CSRF_ORIGIN_BYTES
                || retained_bytes > MAX_CSRF_POLICY_BYTES
            {
                return self.with_error("CSRF_ORIGIN_LIMIT");
            }
            self.trusted_origins.push(origin.to_owned());
        }
        self
    }

    /// Exempts one exact method and URI path from both configured profiles.
    /// Exempt routes must provide their own request-authenticity mechanism. A
    /// policy retains at most 32 bypasses. Methods are uppercase HTTP tokens of
    /// at most 32 bytes; paths are exact URI paths of at most 2 KiB and cannot
    /// contain a query or fragment.
    #[must_use]
    pub fn bypass(mut self, method: &str, path: &str) -> Self {
        if self.invalid.is_some() {
            return self;
        }
        if self.bypasses.len() == MAX_CSRF_BYPASSES
            || method.is_empty()
            || method.len() > 32
            || method.bytes().any(|byte| byte.is_ascii_lowercase())
            || !valid_bypass_path(path)
        {
            return self.with_error("CSRF_BYPASS_INVALID");
        }
        let Ok(parsed_method) = Method::from_bytes(method.as_bytes()) else {
            return self.with_error("CSRF_BYPASS_INVALID");
        };
        self.bypasses.push(CsrfBypass {
            method: parsed_method.as_str().to_owned(),
            path: path.to_owned(),
        });
        self
    }

    /// Adds a prior signed-token key accepted only for verification.
    ///
    /// New tokens always use the primary key. Duplicate keys and more than
    /// four prior keys make the policy invalid.
    #[must_use]
    pub fn previous_verification_key(mut self, key: CsrfSecret) -> Self {
        let Some(signed) = self.signed.as_mut() else {
            return self.with_error("CSRF_SIGNED_MODE_REQUIRED");
        };
        if signed.previous_keys.len() == MAX_CSRF_PREVIOUS_KEYS {
            return self.with_error("CSRF_PREVIOUS_KEY_LIMIT");
        }
        if signed.primary_key == key || signed.previous_keys.contains(&key) {
            return self.with_error("CSRF_PREVIOUS_KEY_DUPLICATE");
        }
        signed.previous_keys.push(key);
        self
    }

    /// Replaces the cookie attributes used by signed double-submit mode.
    #[must_use]
    pub fn cookie_policy(mut self, cookie: CsrfCookiePolicy) -> Self {
        let Some(signed) = self.signed.as_mut() else {
            return self.with_error("CSRF_SIGNED_MODE_REQUIRED");
        };
        signed.cookie = cookie;
        self
    }

    /// Selects the request header carrying a token in either token mode.
    ///
    /// The name must be a valid, non-empty HTTP header name of at most 256
    /// bytes. Incoming encoded tokens are bounded to 512 bytes.
    #[must_use]
    pub fn token_header(mut self, name: &str) -> Self {
        if name.is_empty() || name.len() > 256 || HeaderName::from_bytes(name.as_bytes()).is_err() {
            return self.with_error("CSRF_TOKEN_HEADER_INVALID");
        }
        let name = name.to_ascii_lowercase();
        if let Some(signed) = self.signed.as_mut() {
            signed.token_header = name;
        } else if let Some(synchronizer) = self.synchronizer.as_mut() {
            synchronizer.token_header = name;
        } else {
            return self.with_error("CSRF_TOKEN_MODE_REQUIRED");
        }
        self
    }

    /// Enables URL-encoded form extraction in addition to the token header.
    /// Supplying both sources in one request is rejected as ambiguous.
    /// JSON and multipart requests continue to carry the token in the header.
    /// The field name is limited to 256 ASCII alphanumeric, `_`, `-`, or `.`
    /// bytes; Lily inspects at most 64 KiB of a URL-encoded form body.
    #[must_use]
    pub fn allow_form_field(mut self, name: &str) -> Self {
        if !valid_form_field_name(name) {
            return self.with_error("CSRF_FORM_FIELD_INVALID");
        }
        if let Some(signed) = self.signed.as_mut() {
            signed.form_field = Some(name.to_owned());
        } else if let Some(synchronizer) = self.synchronizer.as_mut() {
            synchronizer.form_field = Some(name.to_owned());
        } else {
            return self.with_error("CSRF_TOKEN_MODE_REQUIRED");
        }
        self
    }

    /// Selects the bounded lifetime for newly issued tokens.
    ///
    /// The lifetime must use whole-second precision in the inclusive range
    /// 1 second through 7 days.
    #[must_use]
    pub fn token_ttl(mut self, ttl: Duration) -> Self {
        if ttl.is_zero() || ttl > MAX_CSRF_TOKEN_TTL || ttl.subsec_nanos() != 0 {
            return self.with_error("CSRF_TOKEN_TTL_INVALID");
        }
        if let Some(signed) = self.signed.as_mut() {
            signed.ttl = ttl;
        } else if let Some(synchronizer) = self.synchronizer.as_mut() {
            synchronizer.ttl = ttl;
        } else {
            return self.with_error("CSRF_TOKEN_MODE_REQUIRED");
        }
        self
    }

    /// Bounds each call into an application synchronizer-token store.
    ///
    /// The timeout must be greater than zero and at most 30 seconds.
    #[must_use]
    pub fn store_timeout(mut self, timeout: Duration) -> Self {
        let Some(synchronizer) = self.synchronizer.as_mut() else {
            return self.with_error("CSRF_SYNCHRONIZER_MODE_REQUIRED");
        };
        if timeout.is_zero() || timeout > MAX_CSRF_STORE_TIMEOUT {
            return self.with_error("CSRF_STORE_TIMEOUT_INVALID");
        }
        synchronizer.store_timeout = timeout;
        self
    }

    /// Validates all retained policy metadata before application publication.
    pub fn validate(&self) -> Result<(), MiddlewareConfigError> {
        if let Some(code) = self.invalid {
            return Err(MiddlewareConfigError::middleware(code));
        }
        validate_origins(&self.trusted_origins)?;
        validate_bypasses(&self.bypasses)?;

        if self.mode.uses_signed_token() {
            let signed = self
                .signed
                .as_ref()
                .ok_or_else(|| csrf_error("CSRF_SIGNED_CONFIG_MISSING"))?;
            if signed.previous_keys.len() > MAX_CSRF_PREVIOUS_KEYS {
                return Err(csrf_error("CSRF_PREVIOUS_KEY_LIMIT"));
            }
            if HeaderName::from_bytes(signed.token_header.as_bytes()).is_err() {
                return Err(csrf_error("CSRF_TOKEN_HEADER_INVALID"));
            }
            if signed
                .form_field
                .as_deref()
                .is_some_and(|name| !valid_form_field_name(name))
            {
                return Err(csrf_error("CSRF_FORM_FIELD_INVALID"));
            }
            if signed.ttl.is_zero()
                || signed.ttl > MAX_CSRF_TOKEN_TTL
                || signed.ttl.subsec_nanos() != 0
            {
                return Err(csrf_error("CSRF_TOKEN_TTL_INVALID"));
            }
            signed.cookie.validate(signed.ttl)?;
        } else if self.signed.is_some() {
            return Err(csrf_error("CSRF_SIGNED_CONFIG_UNEXPECTED"));
        }

        if self.mode.uses_synchronizer_token() {
            let synchronizer = self
                .synchronizer
                .as_ref()
                .ok_or_else(|| csrf_error("CSRF_SYNCHRONIZER_CONFIG_MISSING"))?;
            if HeaderName::from_bytes(synchronizer.token_header.as_bytes()).is_err() {
                return Err(csrf_error("CSRF_TOKEN_HEADER_INVALID"));
            }
            if synchronizer
                .form_field
                .as_deref()
                .is_some_and(|name| !valid_form_field_name(name))
            {
                return Err(csrf_error("CSRF_FORM_FIELD_INVALID"));
            }
            if synchronizer.ttl.is_zero()
                || synchronizer.ttl > MAX_CSRF_TOKEN_TTL
                || synchronizer.ttl.subsec_nanos() != 0
            {
                return Err(csrf_error("CSRF_TOKEN_TTL_INVALID"));
            }
            if synchronizer.store_timeout.is_zero()
                || synchronizer.store_timeout > MAX_CSRF_STORE_TIMEOUT
            {
                return Err(csrf_error("CSRF_STORE_TIMEOUT_INVALID"));
            }
        } else if self.synchronizer.is_some() {
            return Err(csrf_error("CSRF_SYNCHRONIZER_CONFIG_UNEXPECTED"));
        }

        if self.mode.uses_token() != (self.signed.is_some() || self.synchronizer.is_some()) {
            return Err(csrf_error("CSRF_TOKEN_CONFIG_INVALID"));
        }

        let retained_bytes = self
            .trusted_origins
            .iter()
            .map(String::len)
            .chain(
                self.bypasses
                    .iter()
                    .map(|bypass| bypass.method.len().saturating_add(bypass.path.len())),
            )
            .sum::<usize>()
            .saturating_add(self.signed.as_ref().map_or(0, |signed| {
                signed.token_header.len()
                    + signed.form_field.as_ref().map_or(0, String::len)
                    + signed.cookie.name.len()
                    + signed.cookie.path.len()
            }))
            .saturating_add(self.synchronizer.as_ref().map_or(0, |synchronizer| {
                synchronizer.token_header.len()
                    + synchronizer.form_field.as_ref().map_or(0, String::len)
            }));
        if retained_bytes > MAX_CSRF_POLICY_BYTES {
            return Err(csrf_error("CSRF_POLICY_BYTES_EXCEEDED"));
        }
        Ok(())
    }

    /// Returns the enforcement profile selected by this policy.
    #[must_use]
    pub const fn mode(&self) -> CsrfMode {
        self.mode
    }

    fn with_error(mut self, code: &'static str) -> Self {
        self.invalid = Some(csrf_code(code));
        self
    }
}

impl fmt::Debug for CsrfPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CsrfPolicy")
            .field("mode", &self.mode)
            .field("trusted_origin_count", &self.trusted_origins.len())
            .field("bypass_count", &self.bypasses.len())
            .field("signed", &self.signed)
            .field("synchronizer", &self.synchronizer)
            .field("is_invalid", &self.invalid.is_some())
            .finish()
    }
}

fn valid_bypass_path(path: &str) -> bool {
    !path.is_empty()
        && path.len() <= MAX_CSRF_PATH_BYTES
        && path.starts_with('/')
        && !path.contains(['?', '#', '\r', '\n'])
        && path.parse::<Uri>().is_ok()
}

fn valid_form_field_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 256
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

fn validate_origins(origins: &[String]) -> Result<(), MiddlewareConfigError> {
    if origins.len() > MAX_CSRF_TRUSTED_ORIGINS {
        return Err(csrf_error("CSRF_ORIGIN_LIMIT"));
    }
    for (index, origin) in origins.iter().enumerate() {
        if origins[..index].contains(origin) {
            return Err(csrf_error("CSRF_ORIGIN_DUPLICATE"));
        }
        if origin.len() > MAX_CSRF_ORIGIN_BYTES {
            return Err(csrf_error("CSRF_ORIGIN_LIMIT"));
        }
        let parsed = Url::parse(origin).map_err(|_| csrf_error("CSRF_ORIGIN_INVALID"))?;
        if !matches!(parsed.scheme(), "http" | "https")
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
            || parsed.path() != "/"
            || parsed.origin().ascii_serialization() != *origin
            || HeaderValue::from_str(origin).is_err()
        {
            return Err(csrf_error("CSRF_ORIGIN_INVALID"));
        }
    }
    Ok(())
}

fn validate_bypasses(bypasses: &[CsrfBypass]) -> Result<(), MiddlewareConfigError> {
    if bypasses.len() > MAX_CSRF_BYPASSES {
        return Err(csrf_error("CSRF_BYPASS_LIMIT"));
    }
    for (index, bypass) in bypasses.iter().enumerate() {
        if Method::from_bytes(bypass.method.as_bytes()).is_err()
            || bypass.method.bytes().any(|byte| byte.is_ascii_lowercase())
            || !valid_bypass_path(&bypass.path)
        {
            return Err(csrf_error("CSRF_BYPASS_INVALID"));
        }
        if bypasses[..index].contains(bypass) {
            return Err(csrf_error("CSRF_BYPASS_DUPLICATE"));
        }
    }
    Ok(())
}

/// Public token intentionally returned to an application endpoint.
#[derive(Clone, PartialEq, Eq)]
pub struct CsrfToken(String);

impl CsrfToken {
    /// Returns the public token serialization for an application response.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for CsrfToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CsrfToken")
            .field("value", &"<redacted>")
            .field("bytes", &self.0.len())
            .finish()
    }
}

#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CsrfRuntimeRejection {
    CrossOrigin,
    CrossOriginLegacy,
    CrossOriginHeadersInvalid,
    BindingMissing,
    BindingInvalid,
    CookieMissing,
    CookieInvalid,
    TokenMissing,
    TokenAmbiguous,
    TokenMalformed,
    TokenExpired,
    TokenMismatch,
    SignatureInvalid,
    FormInvalid,
    ClockInvalid,
    StoreUnavailable,
    StoreTimeout,
    StoreInvalid,
}

impl CsrfRuntimeRejection {
    #[must_use]
    pub const fn diagnostic_code(self) -> &'static str {
        match self {
            Self::CrossOrigin => "CSRF_CROSS_ORIGIN",
            Self::CrossOriginLegacy => "CSRF_CROSS_ORIGIN_LEGACY",
            Self::CrossOriginHeadersInvalid => "CSRF_HEADERS_INVALID",
            Self::BindingMissing => "CSRF_BINDING_MISSING",
            Self::BindingInvalid => "CSRF_BINDING_INVALID",
            Self::CookieMissing => "CSRF_COOKIE_MISSING",
            Self::CookieInvalid => "CSRF_COOKIE_INVALID",
            Self::TokenMissing => "CSRF_TOKEN_MISSING",
            Self::TokenAmbiguous => "CSRF_TOKEN_AMBIGUOUS",
            Self::TokenMalformed => "CSRF_TOKEN_MALFORMED",
            Self::TokenExpired => "CSRF_TOKEN_EXPIRED",
            Self::TokenMismatch => "CSRF_TOKEN_MISMATCH",
            Self::SignatureInvalid => "CSRF_SIGNATURE_INVALID",
            Self::FormInvalid => "CSRF_FORM_INVALID",
            Self::ClockInvalid => "CSRF_CLOCK_INVALID",
            Self::StoreUnavailable => "CSRF_STORE_UNAVAILABLE",
            Self::StoreTimeout => "CSRF_STORE_TIMEOUT",
            Self::StoreInvalid => "CSRF_STORE_INVALID",
        }
    }
}

#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CsrfOperationError {
    TokenModeRequired,
    BindingMissing,
    BindingInvalid,
    ClockInvalid,
    RandomUnavailable,
    CookieInvalid,
    StoreUnavailable,
    StoreTimeout,
    StoreInvalid,
}

impl fmt::Display for CsrfOperationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::TokenModeRequired => "CSRF token mode is not configured",
            Self::BindingMissing => "CSRF session binding is unavailable",
            Self::BindingInvalid => "CSRF session binding is invalid",
            Self::ClockInvalid => "CSRF token clock is unavailable",
            Self::RandomUnavailable => "CSRF secure randomness is unavailable",
            Self::CookieInvalid => "CSRF cookie construction failed",
            Self::StoreUnavailable => "CSRF token store is unavailable",
            Self::StoreTimeout => "CSRF token store operation timed out",
            Self::StoreInvalid => "CSRF token store returned invalid data",
        })
    }
}

impl std::error::Error for CsrfOperationError {}

#[derive(Clone)]
struct CompiledSignedDoubleSubmit {
    keys: Vec<CsrfSecret>,
    binding: Arc<dyn CsrfSessionBinding>,
    cookie: CsrfCookiePolicy,
    token_header: String,
    form_field: Option<String>,
    ttl: Duration,
}

#[derive(Clone)]
struct CompiledSynchronizer {
    key_derivation_secret: CsrfSecret,
    binding: Arc<dyn CsrfSessionBinding>,
    store: Arc<dyn CsrfTokenStore>,
    token_header: String,
    form_field: Option<String>,
    ttl: Duration,
    store_timeout: Duration,
}

/// Immutable runtime compiled once during application build.
#[doc(hidden)]
#[derive(Clone)]
pub struct CompiledCsrfPolicy {
    mode: CsrfMode,
    bypasses: Arc<[CsrfBypass]>,
    cross_origin: Option<CsrfLayerAdapter>,
    signed: Option<CompiledSignedDoubleSubmit>,
    synchronizer: Option<CompiledSynchronizer>,
}

impl fmt::Debug for CompiledCsrfPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CompiledCsrfPolicy")
            .field("mode", &self.mode)
            .field("bypass_count", &self.bypasses.len())
            .field("cross_origin", &self.cross_origin.is_some())
            .field("signed", &self.signed.is_some())
            .field("synchronizer", &self.synchronizer.is_some())
            .finish()
    }
}

impl CompiledCsrfPolicy {
    pub fn try_new(policy: &CsrfPolicy) -> Result<Self, MiddlewareConfigError> {
        policy.validate()?;
        let cross_origin = policy
            .mode
            .uses_cross_origin()
            .then(|| CsrfLayerAdapter::try_new(policy))
            .transpose()?;
        let signed = policy.signed.as_ref().map(|signed| {
            let mut keys = Vec::with_capacity(1 + signed.previous_keys.len());
            keys.push(signed.primary_key.clone());
            keys.extend(signed.previous_keys.iter().cloned());
            CompiledSignedDoubleSubmit {
                keys,
                binding: Arc::clone(&signed.binding),
                cookie: signed.cookie.clone(),
                token_header: signed.token_header.clone(),
                form_field: signed.form_field.clone(),
                ttl: signed.ttl,
            }
        });
        let synchronizer = policy
            .synchronizer
            .as_ref()
            .map(|synchronizer| CompiledSynchronizer {
                key_derivation_secret: synchronizer.key_derivation_secret.clone(),
                binding: Arc::clone(&synchronizer.binding),
                store: Arc::clone(&synchronizer.store),
                token_header: synchronizer.token_header.clone(),
                form_field: synchronizer.form_field.clone(),
                ttl: synchronizer.ttl,
                store_timeout: synchronizer.store_timeout,
            });
        Ok(Self {
            mode: policy.mode,
            bypasses: policy.bypasses.clone().into(),
            cross_origin,
            signed,
            synchronizer,
        })
    }

    /// Compiles the route-guard profile without accepting an inert global
    /// bypass list. Route selection itself is the opt-in boundary in this
    /// mode.
    pub fn try_new_route_scoped(policy: &CsrfPolicy) -> Result<Self, MiddlewareConfigError> {
        if !policy.bypasses.is_empty() {
            return Err(csrf_error("CSRF_ROUTE_BYPASS_UNSUPPORTED"));
        }
        Self::try_new(policy)
    }

    pub async fn enforce(&self, request: &Request) -> Result<(), CsrfRuntimeRejection> {
        if safe_method(request.method()) || self.is_bypassed(request) {
            return Ok(());
        }
        if let Some(adapter) = &self.cross_origin {
            adapter.enforce(request).await?;
        }
        if let Some(signed) = &self.signed {
            signed.enforce(request, unix_seconds()?).await?;
        }
        if let Some(synchronizer) = &self.synchronizer {
            synchronizer.enforce(request, unix_seconds()?).await?;
        }
        Ok(())
    }

    pub async fn issue(
        &self,
        request: &Request,
    ) -> Result<(CsrfToken, Option<ResponseCookie>), CsrfOperationError> {
        let now = unix_seconds().map_err(|_| CsrfOperationError::ClockInvalid)?;
        if let Some(signed) = &self.signed {
            let (token, cookie) = signed.issue(request, now)?;
            return Ok((token, Some(cookie)));
        }
        if let Some(synchronizer) = &self.synchronizer {
            return synchronizer
                .issue(request, now)
                .await
                .map(|token| (token, None));
        }
        Err(CsrfOperationError::TokenModeRequired)
    }

    pub async fn rotate(
        &self,
        request: &Request,
    ) -> Result<(CsrfToken, Option<ResponseCookie>), CsrfOperationError> {
        let now = unix_seconds().map_err(|_| CsrfOperationError::ClockInvalid)?;
        if let Some(signed) = &self.signed {
            let (token, cookie) = signed.issue(request, now)?;
            return Ok((token, Some(cookie)));
        }
        if let Some(synchronizer) = &self.synchronizer {
            return synchronizer
                .rotate(request, now)
                .await
                .map(|token| (token, None));
        }
        Err(CsrfOperationError::TokenModeRequired)
    }

    pub async fn clear(
        &self,
        request: &Request,
    ) -> Result<Option<CookieRemoval>, CsrfOperationError> {
        if let Some(signed) = &self.signed {
            return signed
                .cookie
                .removal()
                .map(Some)
                .map_err(|_| CsrfOperationError::CookieInvalid);
        }
        if let Some(synchronizer) = &self.synchronizer {
            synchronizer.clear(request).await?;
            return Ok(None);
        }
        Err(CsrfOperationError::TokenModeRequired)
    }

    fn is_bypassed(&self, request: &Request) -> bool {
        if self.bypasses.is_empty() {
            return false;
        }
        let Ok(uri) = request.path().parse::<Uri>() else {
            return false;
        };
        self.bypasses
            .iter()
            .any(|bypass| bypass.method == request.method() && bypass.path == uri.path())
    }
}

impl CompiledSignedDoubleSubmit {
    fn binding(&self, request: &Request) -> Result<CsrfSessionId, CsrfOperationError> {
        self.binding
            .session_id(request)
            .map_err(|_| CsrfOperationError::BindingInvalid)?
            .ok_or(CsrfOperationError::BindingMissing)
    }

    fn issue(
        &self,
        request: &Request,
        now: u64,
    ) -> Result<(CsrfToken, ResponseCookie), CsrfOperationError> {
        let binding = self.binding(request)?;
        let expires = now
            .checked_add(self.ttl.as_secs())
            .ok_or(CsrfOperationError::ClockInvalid)?;
        let mut nonce = [0_u8; CSRF_NONCE_BYTES];
        OsRng
            .try_fill_bytes(&mut nonce)
            .map_err(|_| CsrfOperationError::RandomUnavailable)?;
        let mac = sign(self.keys[0].bytes(), binding.bytes(), expires, &nonce)
            .map_err(|_| CsrfOperationError::RandomUnavailable)?;
        let token = encode_token(expires, &nonce, &mac);
        let cookie = self
            .cookie
            .response_cookie(&token, self.ttl)
            .map_err(|_| CsrfOperationError::CookieInvalid)?;
        Ok((CsrfToken(token), cookie))
    }

    async fn enforce(&self, request: &Request, now: u64) -> Result<(), CsrfRuntimeRejection> {
        let binding = self
            .binding
            .session_id(request)
            .map_err(|_| CsrfRuntimeRejection::BindingInvalid)?
            .ok_or(CsrfRuntimeRejection::BindingMissing)?;
        let cookie = request
            .cookie(&self.cookie.name)
            .map_err(|_| CsrfRuntimeRejection::CookieInvalid)?
            .ok_or(CsrfRuntimeRejection::CookieMissing)?;
        if cookie.len() > MAX_CSRF_TOKEN_BYTES {
            return Err(CsrfRuntimeRejection::CookieInvalid);
        }

        let header_token = exact_header(request, &self.token_header)?.map(str::to_owned);
        let form_token = if let Some(field_name) = self.form_field.as_deref() {
            extract_form_token(request, field_name).await?
        } else {
            None
        };
        let request_token = match (header_token, form_token) {
            (Some(_), Some(_)) => return Err(CsrfRuntimeRejection::TokenAmbiguous),
            (Some(token), None) | (None, Some(token)) => token,
            (None, None) => return Err(CsrfRuntimeRejection::TokenMissing),
        };
        if request_token.len() > MAX_CSRF_TOKEN_BYTES {
            return Err(CsrfRuntimeRejection::TokenMalformed);
        }
        if cookie.len() != request_token.len()
            || !bool::from(cookie.as_bytes().ct_eq(request_token.as_bytes()))
        {
            return Err(CsrfRuntimeRejection::TokenMismatch);
        }

        let parsed = parse_token(&request_token)?;
        if parsed.expires <= now {
            return Err(CsrfRuntimeRejection::TokenExpired);
        }
        let mut valid = false;
        for key in &self.keys {
            let Ok(mut mac) = HmacSha256::new_from_slice(key.bytes()) else {
                continue;
            };
            update_mac(&mut mac, binding.bytes(), parsed.expires, &parsed.nonce);
            valid |= mac.verify_slice(&parsed.mac).is_ok();
        }
        if !valid {
            return Err(CsrfRuntimeRejection::SignatureInvalid);
        }
        Ok(())
    }
}

impl CompiledSynchronizer {
    fn binding(&self, request: &Request) -> Result<CsrfSessionId, CsrfOperationError> {
        self.binding
            .session_id(request)
            .map_err(|_| CsrfOperationError::BindingInvalid)?
            .ok_or(CsrfOperationError::BindingMissing)
    }

    fn store_key(&self, binding: &CsrfSessionId) -> Result<CsrfStoreKey, CsrfOperationError> {
        derive_store_key(self.key_derivation_secret.bytes(), binding.bytes())
            .map_err(|_| CsrfOperationError::StoreInvalid)
    }

    fn candidate(&self, now: u64) -> Result<StoredCsrfToken, CsrfOperationError> {
        let expires = now
            .checked_add(self.ttl.as_secs())
            .ok_or(CsrfOperationError::ClockInvalid)?;
        let mut nonce = [0_u8; CSRF_NONCE_BYTES];
        OsRng
            .try_fill_bytes(&mut nonce)
            .map_err(|_| CsrfOperationError::RandomUnavailable)?;
        StoredCsrfToken::from_parts(&URL_SAFE_NO_PAD.encode(nonce), expires)
            .map_err(|_| CsrfOperationError::StoreInvalid)
    }

    async fn issue(&self, request: &Request, now: u64) -> Result<CsrfToken, CsrfOperationError> {
        let binding = self.binding(request)?;
        let key = self.store_key(&binding)?;
        let candidate = self.candidate(now)?;
        let stored = self
            .store_call(self.store.load_or_insert(&key, candidate, now))
            .await?;
        if stored.expires_at_unix_seconds() <= now {
            return Err(CsrfOperationError::StoreInvalid);
        }
        Ok(stored.token)
    }

    async fn rotate(&self, request: &Request, now: u64) -> Result<CsrfToken, CsrfOperationError> {
        let binding = self.binding(request)?;
        let key = self.store_key(&binding)?;
        let replacement = self.candidate(now)?;
        self.store_call(self.store.replace(&key, replacement.clone()))
            .await?;
        Ok(replacement.token)
    }

    async fn clear(&self, request: &Request) -> Result<(), CsrfOperationError> {
        let binding = self.binding(request)?;
        let key = self.store_key(&binding)?;
        self.store_call(self.store.revoke(&key)).await
    }

    async fn enforce(&self, request: &Request, now: u64) -> Result<(), CsrfRuntimeRejection> {
        let binding = self
            .binding
            .session_id(request)
            .map_err(|_| CsrfRuntimeRejection::BindingInvalid)?
            .ok_or(CsrfRuntimeRejection::BindingMissing)?;
        let key = derive_store_key(self.key_derivation_secret.bytes(), binding.bytes())
            .map_err(|_| CsrfRuntimeRejection::StoreInvalid)?;
        let request_token =
            extract_request_token(request, &self.token_header, self.form_field.as_deref()).await?;
        if !valid_synchronizer_token(&request_token) {
            return Err(CsrfRuntimeRejection::TokenMalformed);
        }
        let stored = self
            .runtime_store_call(self.store.load(&key))
            .await?
            .ok_or(CsrfRuntimeRejection::TokenMissing)?;
        if stored.expires_at_unix_seconds() <= now {
            return Err(CsrfRuntimeRejection::TokenExpired);
        }
        if stored.token().len() != request_token.len()
            || !bool::from(stored.token().as_bytes().ct_eq(request_token.as_bytes()))
        {
            return Err(CsrfRuntimeRejection::TokenMismatch);
        }
        Ok(())
    }

    async fn store_call<T>(
        &self,
        operation: impl std::future::Future<Output = Result<T, CsrfTokenStoreError>>,
    ) -> Result<T, CsrfOperationError> {
        match tokio::time::timeout(self.store_timeout, operation).await {
            Err(_) | Ok(Err(CsrfTokenStoreError::Timeout)) => Err(CsrfOperationError::StoreTimeout),
            Ok(Err(CsrfTokenStoreError::CorruptData)) => Err(CsrfOperationError::StoreInvalid),
            Ok(Err(_)) => Err(CsrfOperationError::StoreUnavailable),
            Ok(Ok(value)) => Ok(value),
        }
    }

    async fn runtime_store_call<T>(
        &self,
        operation: impl std::future::Future<Output = Result<T, CsrfTokenStoreError>>,
    ) -> Result<T, CsrfRuntimeRejection> {
        match tokio::time::timeout(self.store_timeout, operation).await {
            Err(_) | Ok(Err(CsrfTokenStoreError::Timeout)) => {
                Err(CsrfRuntimeRejection::StoreTimeout)
            }
            Ok(Err(CsrfTokenStoreError::CorruptData)) => Err(CsrfRuntimeRejection::StoreInvalid),
            Ok(Err(_)) => Err(CsrfRuntimeRejection::StoreUnavailable),
            Ok(Ok(value)) => Ok(value),
        }
    }
}

async fn extract_request_token(
    request: &Request,
    token_header: &str,
    form_field: Option<&str>,
) -> Result<String, CsrfRuntimeRejection> {
    let header_token = exact_header(request, token_header)?.map(str::to_owned);
    let form_token = if let Some(field_name) = form_field {
        extract_form_token(request, field_name).await?
    } else {
        None
    };
    let token = match (header_token, form_token) {
        (Some(_), Some(_)) => return Err(CsrfRuntimeRejection::TokenAmbiguous),
        (Some(token), None) | (None, Some(token)) => token,
        (None, None) => return Err(CsrfRuntimeRejection::TokenMissing),
    };
    if token.len() > MAX_CSRF_TOKEN_BYTES {
        return Err(CsrfRuntimeRejection::TokenMalformed);
    }
    Ok(token)
}

fn valid_synchronizer_token(token: &str) -> bool {
    if token.is_empty() || token.len() > MAX_CSRF_TOKEN_BYTES {
        return false;
    }
    let Ok(decoded) = URL_SAFE_NO_PAD.decode(token) else {
        return false;
    };
    decoded.len() == CSRF_NONCE_BYTES && URL_SAFE_NO_PAD.encode(decoded) == token
}

fn derive_store_key(
    secret: &[u8],
    binding: &[u8],
) -> Result<CsrfStoreKey, hmac::digest::InvalidLength> {
    let mut mac = HmacSha256::new_from_slice(secret)?;
    mac.update(CSRF_STORE_KEY_DOMAIN);
    mac.update(&(binding.len() as u64).to_be_bytes());
    mac.update(binding);
    let mut key = [0_u8; CSRF_STORE_KEY_BYTES];
    key.copy_from_slice(&mac.finalize().into_bytes());
    Ok(CsrfStoreKey(key))
}

async fn extract_form_token(
    request: &Request,
    field_name: &str,
) -> Result<Option<String>, CsrfRuntimeRejection> {
    if !request.is_form() {
        return Ok(None);
    }
    request
        .buffer_body_bounded(MAX_CSRF_FORM_BODY_BYTES)
        .await
        .map_err(|_| CsrfRuntimeRejection::FormInvalid)?;
    let form = FormData::parse(request.raw_body_bytes().unwrap_or_default())
        .map_err(|_| CsrfRuntimeRejection::FormInvalid)?;
    let mut values = form.get_all(field_name);
    let value = values.next().map(str::to_owned);
    if values.next().is_some() {
        return Err(CsrfRuntimeRejection::TokenAmbiguous);
    }
    Ok(value)
}

fn exact_header<'a>(
    request: &'a Request,
    name: &str,
) -> Result<Option<&'a str>, CsrfRuntimeRejection> {
    let mut matches = request
        .header_cache()
        .iter()
        .filter(|header| header.name.eq_ignore_ascii_case(name));
    let value = matches.next().map(|header| header.value.as_str());
    if matches.next().is_some() {
        return Err(CsrfRuntimeRejection::TokenAmbiguous);
    }
    Ok(value)
}

struct ParsedToken {
    expires: u64,
    nonce: [u8; CSRF_NONCE_BYTES],
    mac: [u8; CSRF_MAC_BYTES],
}

fn parse_token(token: &str) -> Result<ParsedToken, CsrfRuntimeRejection> {
    if token.len() > MAX_CSRF_TOKEN_BYTES {
        return Err(CsrfRuntimeRejection::TokenMalformed);
    }
    let mut parts = token.split('.');
    let version = parts.next();
    let expires = parts.next();
    let nonce = parts.next();
    let mac = parts.next();
    if version != Some(CSRF_TOKEN_VERSION)
        || expires.is_none()
        || nonce.is_none()
        || mac.is_none()
        || parts.next().is_some()
    {
        return Err(CsrfRuntimeRejection::TokenMalformed);
    }
    let expires = expires
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or(CsrfRuntimeRejection::TokenMalformed)?;
    let nonce = decode_fixed::<CSRF_NONCE_BYTES>(nonce.unwrap())?;
    let mac = decode_fixed::<CSRF_MAC_BYTES>(mac.unwrap())?;
    Ok(ParsedToken {
        expires,
        nonce,
        mac,
    })
}

fn decode_fixed<const N: usize>(encoded: &str) -> Result<[u8; N], CsrfRuntimeRejection> {
    let decoded = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| CsrfRuntimeRejection::TokenMalformed)?;
    decoded
        .try_into()
        .map_err(|_| CsrfRuntimeRejection::TokenMalformed)
}

fn encode_token(
    expires: u64,
    nonce: &[u8; CSRF_NONCE_BYTES],
    mac: &[u8; CSRF_MAC_BYTES],
) -> String {
    format!(
        "{CSRF_TOKEN_VERSION}.{expires}.{}.{}",
        URL_SAFE_NO_PAD.encode(nonce),
        URL_SAFE_NO_PAD.encode(mac)
    )
}

fn sign(
    key: &[u8],
    binding: &[u8],
    expires: u64,
    nonce: &[u8; CSRF_NONCE_BYTES],
) -> Result<[u8; CSRF_MAC_BYTES], hmac::digest::InvalidLength> {
    let mut mac = HmacSha256::new_from_slice(key)?;
    update_mac(&mut mac, binding, expires, nonce);
    Ok(mac.finalize().into_bytes().into())
}

fn update_mac(mac: &mut HmacSha256, binding: &[u8], expires: u64, nonce: &[u8; CSRF_NONCE_BYTES]) {
    mac.update(CSRF_MAC_DOMAIN);
    mac.update(&(binding.len() as u64).to_be_bytes());
    mac.update(binding);
    mac.update(&expires.to_be_bytes());
    mac.update(nonce);
}

fn unix_seconds() -> Result<u64, CsrfRuntimeRejection> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| CsrfRuntimeRejection::ClockInvalid)
}

fn safe_method(method: &str) -> bool {
    matches!(method, "GET" | "HEAD" | "OPTIONS")
}

#[derive(Clone)]
struct AllowRequest;

impl Service<TowerRequest<()>> for AllowRequest {
    type Response = TowerResponse<()>;
    type Error = Infallible;
    type Future = Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        _context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, _request: TowerRequest<()>) -> Self::Future {
        ready(Ok(TowerResponse::new(())))
    }
}

#[derive(Clone)]
struct CsrfLayerAdapter {
    layer: CsrfLayer,
}

impl fmt::Debug for CsrfLayerAdapter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CsrfLayerAdapter")
            .finish_non_exhaustive()
    }
}

impl CsrfLayerAdapter {
    fn try_new(policy: &CsrfPolicy) -> Result<Self, MiddlewareConfigError> {
        let mut layer = CsrfLayer::new();
        for origin in &policy.trusted_origins {
            layer = layer
                .add_trusted_origin(origin)
                .map_err(|_| csrf_error("CSRF_ORIGIN_INVALID"))?;
        }
        Ok(Self { layer })
    }

    async fn enforce(&self, request: &Request) -> Result<(), CsrfRuntimeRejection> {
        let tower_request = tower_request_head(request)?;
        let mut service = self.layer.layer(AllowRequest);
        let response = service
            .call(tower_request)
            .await
            .expect("the CSRF probe service is infallible");
        if let Some(error) = response.extensions().get::<ProtectionError>() {
            return Err(match error.kind() {
                ProtectionErrorKind::CrossOriginRequest => CsrfRuntimeRejection::CrossOrigin,
                ProtectionErrorKind::CrossOriginRequestFromOldBrowser => {
                    CsrfRuntimeRejection::CrossOriginLegacy
                }
                _ => CsrfRuntimeRejection::CrossOrigin,
            });
        }
        Ok(())
    }
}

fn tower_request_head(request: &Request) -> Result<TowerRequest<()>, CsrfRuntimeRejection> {
    let method = Method::from_bytes(request.method().as_bytes())
        .map_err(|_| CsrfRuntimeRejection::CrossOriginHeadersInvalid)?;
    let uri = request
        .path()
        .parse::<Uri>()
        .map_err(|_| CsrfRuntimeRejection::CrossOriginHeadersInvalid)?;
    let mut tower_request = TowerRequest::builder()
        .method(method)
        .uri(uri)
        .body(())
        .map_err(|_| CsrfRuntimeRejection::CrossOriginHeadersInvalid)?;

    for name in [ORIGIN.as_str(), "sec-fetch-site", HOST.as_str()] {
        let mut values = request
            .header_cache()
            .iter()
            .filter(|header| header.name.eq_ignore_ascii_case(name));
        let value = values.next().map(|header| header.value.as_str());
        if values.next().is_some() {
            return Err(CsrfRuntimeRejection::CrossOriginHeadersInvalid);
        }
        if let Some(value) = value {
            let value = HeaderValue::from_str(value)
                .map_err(|_| CsrfRuntimeRejection::CrossOriginHeadersInvalid)?;
            tower_request.headers_mut().insert(
                HeaderName::from_bytes(name.as_bytes())
                    .expect("built-in CSRF header names are valid"),
                value,
            );
        }
    }
    Ok(tower_request)
}

#[cfg(test)]
mod tests {
    use super::*;
    use lily_core::RawHeader;
    use std::{
        collections::HashMap,
        sync::atomic::{AtomicU8, AtomicUsize, Ordering},
    };
    use tokio::sync::Mutex;

    fn secret(byte: u8) -> CsrfSecret {
        CsrfSecret::new([byte; 32]).unwrap()
    }

    #[derive(Default)]
    struct TestTokenStore {
        values: Mutex<HashMap<CsrfStoreKey, StoredCsrfToken>>,
        behavior: AtomicU8,
    }

    impl TestTokenStore {
        fn unavailable(&self) {
            self.behavior.store(1, Ordering::Relaxed);
        }

        fn delayed(&self) {
            self.behavior.store(2, Ordering::Relaxed);
        }

        async fn before_operation(&self) -> Result<(), CsrfTokenStoreError> {
            match self.behavior.load(Ordering::Relaxed) {
                1 => Err(CsrfTokenStoreError::Unavailable),
                2 => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    Ok(())
                }
                _ => Ok(()),
            }
        }
    }

    #[async_trait]
    impl CsrfTokenStore for TestTokenStore {
        async fn load_or_insert(
            &self,
            key: &CsrfStoreKey,
            candidate: StoredCsrfToken,
            now_unix_seconds: u64,
        ) -> Result<StoredCsrfToken, CsrfTokenStoreError> {
            self.before_operation().await?;
            let mut values = self.values.lock().await;
            if let Some(current) = values.get(key)
                && current.expires_at_unix_seconds() > now_unix_seconds
            {
                return Ok(current.clone());
            }
            values.insert(key.clone(), candidate.clone());
            Ok(candidate)
        }

        async fn load(
            &self,
            key: &CsrfStoreKey,
        ) -> Result<Option<StoredCsrfToken>, CsrfTokenStoreError> {
            self.before_operation().await?;
            Ok(self.values.lock().await.get(key).cloned())
        }

        async fn replace(
            &self,
            key: &CsrfStoreKey,
            replacement: StoredCsrfToken,
        ) -> Result<(), CsrfTokenStoreError> {
            self.before_operation().await?;
            self.values.lock().await.insert(key.clone(), replacement);
            Ok(())
        }

        async fn revoke(&self, key: &CsrfStoreKey) -> Result<(), CsrfTokenStoreError> {
            self.before_operation().await?;
            self.values.lock().await.remove(key);
            Ok(())
        }
    }

    async fn request(method: &str, path: &str, headers: &[(&str, &str)], body: &[u8]) -> Request {
        Request::from_transport_parts(
            method.to_string(),
            path.to_string(),
            headers
                .iter()
                .enumerate()
                .map(|(index, (name, value))| RawHeader {
                    name: (*name).to_string(),
                    value: (*value).to_string(),
                    line_number: index,
                    raw_line: String::new(),
                })
                .collect(),
            body,
        )
        .await
        .unwrap()
    }

    #[test]
    fn secret_and_binding_debug_are_redacted() {
        let secret_value = "internal-secret-value-32-bytes!!";
        let secret = CsrfSecret::new(secret_value).unwrap();
        let binding = CsrfSessionId::new("session-credential").unwrap();
        let output = format!("{secret:?} {binding:?}");
        assert!(!output.contains(secret_value));
        assert!(!output.contains("session-credential"));
    }

    #[tokio::test]
    async fn request_local_binding_reads_only_the_exact_verified_session_type() {
        #[derive(Debug)]
        struct ApplicationSession(&'static str);

        let binding = CsrfRequestLocalBinding::new();
        let mut request = request("GET", "/csrf", &[], b"").await;
        assert_eq!(binding.session_id(&request).unwrap(), None);

        request.local_mut().insert(ApplicationSession("session-a"));
        assert_eq!(binding.session_id(&request).unwrap(), None);
        assert_eq!(
            request.local().get::<ApplicationSession>().unwrap().0,
            "session-a"
        );

        let expected = CsrfSessionId::new("session-a").unwrap();
        request.local_mut().insert(expected.clone());
        assert_eq!(binding.session_id(&request).unwrap(), Some(expected));
    }

    #[tokio::test]
    async fn request_local_binding_is_fail_closed_for_missing_and_wrong_sessions() {
        let runtime = CompiledCsrfPolicy::try_new(&CsrfPolicy::signed_double_submit(
            secret(9),
            CsrfRequestLocalBinding::new(),
        ))
        .unwrap();

        let missing = request("GET", "/csrf", &[], b"").await;
        assert_eq!(
            runtime.issue(&missing).await,
            Err(CsrfOperationError::BindingMissing)
        );

        let mut issue_request = request("GET", "/csrf", &[], b"").await;
        issue_request
            .local_mut()
            .insert(CsrfSessionId::new("session-a").unwrap());
        let (token, cookie) = runtime.issue(&issue_request).await.unwrap();
        let cookie = cookie.unwrap();
        let cookie_header = format!("{}={}", cookie.name(), cookie.value());

        let mut valid = request(
            "POST",
            "/transfer",
            &[("Cookie", &cookie_header), ("X-CSRF-Token", token.as_str())],
            b"",
        )
        .await;
        valid
            .local_mut()
            .insert(CsrfSessionId::new("session-a").unwrap());
        assert_eq!(runtime.enforce(&valid).await, Ok(()));

        let missing = request(
            "POST",
            "/transfer",
            &[("Cookie", &cookie_header), ("X-CSRF-Token", token.as_str())],
            b"",
        )
        .await;
        assert_eq!(
            runtime.enforce(&missing).await,
            Err(CsrfRuntimeRejection::BindingMissing)
        );

        let mut wrong = request(
            "POST",
            "/transfer",
            &[("Cookie", &cookie_header), ("X-CSRF-Token", token.as_str())],
            b"",
        )
        .await;
        wrong
            .local_mut()
            .insert(CsrfSessionId::new("session-b").unwrap());
        assert_eq!(
            runtime.enforce(&wrong).await,
            Err(CsrfRuntimeRejection::SignatureInvalid)
        );
    }

    #[test]
    fn policy_rejects_noncanonical_origins_and_unsafe_host_cookie() {
        let binding = CsrfSessionCookieBinding::new("session").unwrap();
        let noncanonical = CsrfPolicy::defense_in_depth(secret(1), binding.clone())
            .trust_origins(["HTTPS://EXAMPLE.COM"]);
        assert_eq!(
            noncanonical.validate().unwrap_err().diagnostic_code(),
            "CSRF_ORIGIN_INVALID"
        );

        let cookie = CsrfCookiePolicy::default().secure(false);
        let insecure = CsrfPolicy::signed_double_submit(secret(1), binding).cookie_policy(cookie);
        assert_eq!(
            insecure.validate().unwrap_err().diagnostic_code(),
            "CSRF_HOST_COOKIE_INVALID"
        );
    }

    #[test]
    fn route_scoped_compilation_rejects_inert_global_bypass_configuration() {
        let policy = CsrfPolicy::cross_origin().bypass("POST", "/webhook");
        assert_eq!(
            CompiledCsrfPolicy::try_new_route_scoped(&policy)
                .unwrap_err()
                .diagnostic_code(),
            "CSRF_ROUTE_BYPASS_UNSUPPORTED"
        );
        assert!(CompiledCsrfPolicy::try_new_route_scoped(&CsrfPolicy::cross_origin()).is_ok());
    }

    #[test]
    fn policy_and_secret_limits_fail_with_bounded_startup_codes() {
        assert_eq!(
            CsrfSecret::new([0_u8; MIN_CSRF_SECRET_BYTES - 1]).unwrap_err(),
            CsrfSecretError::TooShort {
                minimum_bytes: MIN_CSRF_SECRET_BYTES
            }
        );
        assert_eq!(
            CsrfSessionId::new(Vec::<u8>::new()).unwrap_err(),
            CsrfBindingError::Empty
        );

        let binding = CsrfSessionCookieBinding::new("session").unwrap();
        let too_many_origins = CsrfPolicy::cross_origin().trust_origins(
            (0..=MAX_CSRF_TRUSTED_ORIGINS).map(|index| format!("https://{index}.example")),
        );
        assert_eq!(
            too_many_origins.validate().unwrap_err().diagnostic_code(),
            "CSRF_ORIGIN_LIMIT"
        );

        let mut too_many_keys = CsrfPolicy::signed_double_submit(secret(1), binding.clone());
        for byte in 2..=(MAX_CSRF_PREVIOUS_KEYS as u8 + 2) {
            too_many_keys = too_many_keys.previous_verification_key(secret(byte));
        }
        assert_eq!(
            too_many_keys.validate().unwrap_err().diagnostic_code(),
            "CSRF_PREVIOUS_KEY_LIMIT"
        );

        let invalid_header =
            CsrfPolicy::signed_double_submit(secret(1), binding.clone()).token_header("bad header");
        assert_eq!(
            invalid_header.validate().unwrap_err().diagnostic_code(),
            "CSRF_TOKEN_HEADER_INVALID"
        );
        let invalid_form =
            CsrfPolicy::signed_double_submit(secret(1), binding.clone()).allow_form_field("bad[]");
        assert_eq!(
            invalid_form.validate().unwrap_err().diagnostic_code(),
            "CSRF_FORM_FIELD_INVALID"
        );
        let invalid_ttl =
            CsrfPolicy::signed_double_submit(secret(1), binding).token_ttl(Duration::from_secs(0));
        assert_eq!(
            invalid_ttl.validate().unwrap_err().diagnostic_code(),
            "CSRF_TOKEN_TTL_INVALID"
        );

        let inert_origin = CsrfPolicy::signed_double_submit(
            secret(1),
            CsrfSessionCookieBinding::new("session").unwrap(),
        )
        .trust_origins(["https://frontend.example"]);
        assert_eq!(
            inert_origin.validate().unwrap_err().diagnostic_code(),
            "CSRF_CROSS_ORIGIN_MODE_REQUIRED"
        );

        let duplicate_bypass = CsrfPolicy::cross_origin()
            .bypass("POST", "/webhook")
            .bypass("POST", "/webhook");
        assert_eq!(
            duplicate_bypass.validate().unwrap_err().diagnostic_code(),
            "CSRF_BYPASS_DUPLICATE"
        );
    }

    #[tokio::test]
    async fn cross_origin_adapter_rejects_unsafe_cross_site_and_duplicate_headers() {
        let runtime = CompiledCsrfPolicy::try_new(&CsrfPolicy::cross_origin()).unwrap();
        let cross_site = request(
            "POST",
            "/transfer",
            &[("Host", "example.com"), ("Sec-Fetch-Site", "cross-site")],
            b"",
        )
        .await;
        assert_eq!(
            runtime.enforce(&cross_site).await,
            Err(CsrfRuntimeRejection::CrossOrigin)
        );

        let duplicate = request(
            "POST",
            "/transfer",
            &[
                ("Host", "example.com"),
                ("Origin", "https://example.com"),
                ("origin", "https://attacker.example"),
            ],
            b"",
        )
        .await;
        assert_eq!(
            runtime.enforce(&duplicate).await,
            Err(CsrfRuntimeRejection::CrossOriginHeadersInvalid)
        );

        let same_origin = request(
            "POST",
            "/transfer",
            &[("Host", "example.com"), ("Origin", "https://example.com")],
            b"",
        )
        .await;
        assert_eq!(runtime.enforce(&same_origin).await, Ok(()));

        let untrusted_legacy = request(
            "POST",
            "/transfer",
            &[
                ("Host", "example.com"),
                ("Origin", "https://attacker.example"),
            ],
            b"",
        )
        .await;
        assert_eq!(
            runtime.enforce(&untrusted_legacy).await,
            Err(CsrfRuntimeRejection::CrossOriginLegacy)
        );

        let trusted_runtime = CompiledCsrfPolicy::try_new(
            &CsrfPolicy::cross_origin().trust_origins(["https://frontend.example"]),
        )
        .unwrap();
        let trusted = request(
            "POST",
            "/transfer",
            &[
                ("Host", "api.example"),
                ("Origin", "https://frontend.example"),
            ],
            b"",
        )
        .await;
        assert_eq!(trusted_runtime.enforce(&trusted).await, Ok(()));

        let trusted_despite_fetch_metadata = request(
            "POST",
            "/transfer",
            &[
                ("Host", "api.example"),
                ("Origin", "https://frontend.example"),
                ("Sec-Fetch-Site", "cross-site"),
            ],
            b"",
        )
        .await;
        assert_eq!(
            trusted_runtime
                .enforce(&trusted_despite_fetch_metadata)
                .await,
            Ok(())
        );

        for duplicate_name in ["host", "sec-fetch-site"] {
            let duplicate = request(
                "POST",
                "/transfer",
                &[
                    ("Host", "example.com"),
                    (duplicate_name, "same-origin"),
                    (duplicate_name, "cross-site"),
                ],
                b"",
            )
            .await;
            assert_eq!(
                runtime.enforce(&duplicate).await,
                Err(CsrfRuntimeRejection::CrossOriginHeadersInvalid)
            );
        }

        // tower-http deliberately treats an unsafe request without either
        // browser signal as same-origin or non-browser traffic. Lily preserves
        // that documented semantic instead of inventing a stricter fork.
        let no_browser_signal = request("POST", "/transfer", &[("Host", "example.com")], b"").await;
        assert_eq!(runtime.enforce(&no_browser_signal).await, Ok(()));

        let safe_cross_site = request(
            "GET",
            "/transfer",
            &[("Host", "example.com"), ("Sec-Fetch-Site", "cross-site")],
            b"",
        )
        .await;
        assert_eq!(runtime.enforce(&safe_cross_site).await, Ok(()));
    }

    #[tokio::test]
    async fn signed_token_is_bound_to_cookie_session_and_rejects_old_session() {
        let policy = CsrfPolicy::signed_double_submit(
            secret(7),
            CsrfSessionCookieBinding::new("session").unwrap(),
        );
        let runtime = CompiledCsrfPolicy::try_new(&policy).unwrap();
        let issue_request = request("GET", "/csrf", &[("Cookie", "session=session-a")], b"").await;
        let (token, cookie) = runtime.issue(&issue_request).await.unwrap();
        let cookie = cookie.unwrap();

        let valid_cookie = format!("session=session-a; {}={}", cookie.name(), cookie.value());
        let valid = request(
            "POST",
            "/transfer",
            &[("Cookie", &valid_cookie), ("X-CSRF-Token", token.as_str())],
            b"",
        )
        .await;
        assert_eq!(runtime.enforce(&valid).await, Ok(()));

        let wrong_cookie = format!("session=session-b; {}={}", cookie.name(), cookie.value());
        let wrong_session = request(
            "POST",
            "/transfer",
            &[("Cookie", &wrong_cookie), ("X-CSRF-Token", token.as_str())],
            b"",
        )
        .await;
        assert_eq!(
            runtime.enforce(&wrong_session).await,
            Err(CsrfRuntimeRejection::SignatureInvalid)
        );
    }

    #[tokio::test]
    async fn form_source_is_bounded_and_ambiguous_with_header() {
        let policy = CsrfPolicy::signed_double_submit(
            secret(9),
            CsrfSessionCookieBinding::new("session").unwrap(),
        )
        .allow_form_field("_csrf");
        let runtime = CompiledCsrfPolicy::try_new(&policy).unwrap();
        let issue_request = request("GET", "/csrf", &[("Cookie", "session=session-a")], b"").await;
        let (token, cookie) = runtime.issue(&issue_request).await.unwrap();
        let cookie = cookie.unwrap();
        let cookies = format!("session=session-a; {}={}", cookie.name(), cookie.value());
        let body = format!("_csrf={}", token.as_str());
        let ambiguous = request(
            "POST",
            "/transfer",
            &[
                ("Cookie", &cookies),
                ("Content-Type", "application/x-www-form-urlencoded"),
                ("X-CSRF-Token", token.as_str()),
            ],
            body.as_bytes(),
        )
        .await;
        assert_eq!(
            runtime.enforce(&ambiguous).await,
            Err(CsrfRuntimeRejection::TokenAmbiguous)
        );

        let valid_form = request(
            "POST",
            "/transfer",
            &[
                ("Cookie", &cookies),
                ("Content-Type", "application/x-www-form-urlencoded"),
            ],
            body.as_bytes(),
        )
        .await;
        assert_eq!(runtime.enforce(&valid_form).await, Ok(()));

        let duplicate_form = request(
            "POST",
            "/transfer",
            &[
                ("Cookie", &cookies),
                ("Content-Type", "application/x-www-form-urlencoded"),
            ],
            format!("_csrf={0}&_csrf={0}", token.as_str()).as_bytes(),
        )
        .await;
        assert_eq!(
            runtime.enforce(&duplicate_form).await,
            Err(CsrfRuntimeRejection::TokenAmbiguous)
        );

        let oversized = request(
            "POST",
            "/transfer",
            &[
                ("Cookie", &cookies),
                ("Content-Type", "application/x-www-form-urlencoded"),
            ],
            &vec![b'a'; MAX_CSRF_FORM_BODY_BYTES + 1],
        )
        .await;
        assert_eq!(
            runtime.enforce(&oversized).await,
            Err(CsrfRuntimeRejection::FormInvalid)
        );
    }

    #[tokio::test]
    async fn token_sources_fail_closed_for_missing_malformed_and_duplicate_values() {
        let runtime = CompiledCsrfPolicy::try_new(&CsrfPolicy::signed_double_submit(
            secret(3),
            CsrfSessionCookieBinding::new("session").unwrap(),
        ))
        .unwrap();
        let issue_request = request("GET", "/csrf", &[("Cookie", "session=session-a")], b"").await;
        let (token, cookie) = runtime.issue(&issue_request).await.unwrap();
        let cookie = cookie.unwrap();
        let cookies = format!("session=session-a; {}={}", cookie.name(), cookie.value());

        let missing = request("POST", "/transfer", &[("Cookie", &cookies)], b"").await;
        assert_eq!(
            runtime.enforce(&missing).await,
            Err(CsrfRuntimeRejection::TokenMissing)
        );

        let malformed_cookies = "session=session-a; __Host-lily-csrf=not-a-token";
        let malformed = request(
            "POST",
            "/transfer",
            &[
                ("Cookie", malformed_cookies),
                ("X-CSRF-Token", "not-a-token"),
            ],
            b"",
        )
        .await;
        assert_eq!(
            runtime.enforce(&malformed).await,
            Err(CsrfRuntimeRejection::TokenMalformed)
        );

        let duplicate = request(
            "POST",
            "/transfer",
            &[
                ("Cookie", &cookies),
                ("X-CSRF-Token", token.as_str()),
                ("x-csrf-token", token.as_str()),
            ],
            b"",
        )
        .await;
        assert_eq!(
            runtime.enforce(&duplicate).await,
            Err(CsrfRuntimeRejection::TokenAmbiguous)
        );
    }

    #[tokio::test]
    async fn expiry_rotation_and_previous_key_window_have_explicit_semantics() {
        let binding = CsrfSessionCookieBinding::new("session").unwrap();
        let old_runtime = CompiledCsrfPolicy::try_new(&CsrfPolicy::signed_double_submit(
            secret(1),
            binding.clone(),
        ))
        .unwrap();
        let issue_request = request("GET", "/csrf", &[("Cookie", "session=session-a")], b"").await;
        let old_signed = old_runtime.signed.as_ref().unwrap();
        let (old_token, old_cookie) = old_signed.issue(&issue_request, 100).unwrap();
        let old_cookies = format!(
            "session=session-a; {}={}",
            old_cookie.name(),
            old_cookie.value()
        );
        let old_request = request(
            "POST",
            "/transfer",
            &[
                ("Cookie", &old_cookies),
                ("X-CSRF-Token", old_token.as_str()),
            ],
            b"",
        )
        .await;
        let expires_at = 100 + DEFAULT_CSRF_TOKEN_TTL.as_secs();
        assert_eq!(
            old_signed.enforce(&old_request, expires_at - 1).await,
            Ok(())
        );
        assert_eq!(
            old_signed.enforce(&old_request, expires_at).await,
            Err(CsrfRuntimeRejection::TokenExpired),
            "signed tokens must be rejected at their exact expiry boundary"
        );

        let rotated_runtime = CompiledCsrfPolicy::try_new(
            &CsrfPolicy::signed_double_submit(secret(2), binding.clone())
                .previous_verification_key(secret(1)),
        )
        .unwrap();
        assert_eq!(
            rotated_runtime
                .signed
                .as_ref()
                .unwrap()
                .enforce(&old_request, 101)
                .await,
            Ok(())
        );

        let without_previous =
            CompiledCsrfPolicy::try_new(&CsrfPolicy::signed_double_submit(secret(2), binding))
                .unwrap();
        assert_eq!(
            without_previous
                .signed
                .as_ref()
                .unwrap()
                .enforce(&old_request, 101)
                .await,
            Err(CsrfRuntimeRejection::SignatureInvalid)
        );

        let (new_token, new_cookie) = rotated_runtime
            .signed
            .as_ref()
            .unwrap()
            .issue(&issue_request, 101)
            .unwrap();
        assert_ne!(old_token, new_token);
        let mixed_cookies = format!(
            "session=session-a; {}={}",
            new_cookie.name(),
            new_cookie.value()
        );
        let mixed = request(
            "POST",
            "/transfer",
            &[
                ("Cookie", &mixed_cookies),
                ("X-CSRF-Token", old_token.as_str()),
            ],
            b"",
        )
        .await;
        assert_eq!(
            rotated_runtime
                .signed
                .as_ref()
                .unwrap()
                .enforce(&mixed, 101)
                .await,
            Err(CsrfRuntimeRejection::TokenMismatch)
        );
    }

    #[tokio::test]
    async fn synchronizer_issue_is_stable_then_rotate_and_revoke_invalidate() {
        let store = Arc::new(TestTokenStore::default());
        let policy = CsrfPolicy::synchronizer(
            secret(6),
            CsrfSessionCookieBinding::new("session").unwrap(),
            store.clone(),
        );
        let runtime = CompiledCsrfPolicy::try_new(&policy).unwrap();
        let second_instance = CompiledCsrfPolicy::try_new(&policy).unwrap();
        let issue_request = request("GET", "/csrf", &[("Cookie", "session=session-a")], b"").await;

        let (first, second, third, fourth) = tokio::join!(
            runtime.issue(&issue_request),
            runtime.issue(&issue_request),
            runtime.issue(&issue_request),
            runtime.issue(&issue_request),
        );
        let issued = [
            first.unwrap(),
            second.unwrap(),
            third.unwrap(),
            fourth.unwrap(),
        ];
        assert!(issued.iter().all(|(_, cookie)| cookie.is_none()));
        assert!(
            issued
                .iter()
                .all(|(token, _)| token.as_str() == issued[0].0.as_str())
        );
        assert_eq!(store.values.lock().await.len(), 1);

        let old_token = issued[0].0.clone();
        let valid = request(
            "POST",
            "/transfer",
            &[
                ("Cookie", "session=session-a"),
                ("X-CSRF-Token", old_token.as_str()),
            ],
            b"",
        )
        .await;
        assert_eq!(runtime.enforce(&valid).await, Ok(()));
        assert_eq!(second_instance.enforce(&valid).await, Ok(()));
        let wrong_session = request(
            "POST",
            "/transfer",
            &[
                ("Cookie", "session=session-b"),
                ("X-CSRF-Token", old_token.as_str()),
            ],
            b"",
        )
        .await;
        assert_eq!(
            runtime.enforce(&wrong_session).await,
            Err(CsrfRuntimeRejection::TokenMissing)
        );

        let (new_token, cookie) = runtime.rotate(&issue_request).await.unwrap();
        assert!(cookie.is_none());
        assert_ne!(old_token, new_token);
        assert_eq!(
            runtime.enforce(&valid).await,
            Err(CsrfRuntimeRejection::TokenMismatch)
        );
        let rotated = request(
            "POST",
            "/transfer",
            &[
                ("Cookie", "session=session-a"),
                ("X-CSRF-Token", new_token.as_str()),
            ],
            b"",
        )
        .await;
        assert_eq!(runtime.enforce(&rotated).await, Ok(()));

        assert_eq!(runtime.clear(&issue_request).await, Ok(None));
        assert_eq!(runtime.clear(&issue_request).await, Ok(None));
        assert_eq!(
            runtime.enforce(&rotated).await,
            Err(CsrfRuntimeRejection::TokenMissing)
        );
    }

    #[tokio::test]
    async fn synchronizer_accepts_opt_in_form_but_rejects_ambiguous_sources() {
        let store = Arc::new(TestTokenStore::default());
        let runtime = CompiledCsrfPolicy::try_new(
            &CsrfPolicy::synchronizer(
                secret(9),
                CsrfSessionCookieBinding::new("session").unwrap(),
                store,
            )
            .allow_form_field("_csrf"),
        )
        .unwrap();
        let issue_request = request("GET", "/csrf", &[("Cookie", "session=session-a")], b"").await;
        let (token, cookie) = runtime.issue(&issue_request).await.unwrap();
        assert!(cookie.is_none());
        let body = format!("_csrf={}", token.as_str());
        let form = request(
            "POST",
            "/transfer",
            &[
                ("Cookie", "session=session-a"),
                ("Content-Type", "application/x-www-form-urlencoded"),
            ],
            body.as_bytes(),
        )
        .await;
        assert_eq!(runtime.enforce(&form).await, Ok(()));

        let ambiguous = request(
            "POST",
            "/transfer",
            &[
                ("Cookie", "session=session-a"),
                ("Content-Type", "application/x-www-form-urlencoded"),
                ("X-CSRF-Token", token.as_str()),
            ],
            body.as_bytes(),
        )
        .await;
        assert_eq!(
            runtime.enforce(&ambiguous).await,
            Err(CsrfRuntimeRejection::TokenAmbiguous)
        );
    }

    #[tokio::test]
    async fn synchronizer_expiry_unavailability_and_timeout_fail_closed() {
        let store = Arc::new(TestTokenStore::default());
        let policy = CsrfPolicy::synchronizer(
            secret(7),
            CsrfSessionCookieBinding::new("session").unwrap(),
            store.clone(),
        )
        .store_timeout(Duration::from_millis(10));
        let runtime = CompiledCsrfPolicy::try_new(&policy).unwrap();
        let issue_request = request("GET", "/csrf", &[("Cookie", "session=session-a")], b"").await;
        let (token, cookie) = runtime.issue(&issue_request).await.unwrap();
        assert!(cookie.is_none());
        let protected = request(
            "POST",
            "/transfer",
            &[
                ("Cookie", "session=session-a"),
                ("X-CSRF-Token", token.as_str()),
            ],
            b"",
        )
        .await;

        let expired = StoredCsrfToken::from_parts(token.as_str(), 1).unwrap();
        *store.values.lock().await.values_mut().next().unwrap() = expired;
        assert_eq!(
            runtime.enforce(&protected).await,
            Err(CsrfRuntimeRejection::TokenExpired)
        );

        let (replacement, _) = runtime.issue(&issue_request).await.unwrap();
        assert_ne!(replacement, token);
        assert_eq!(
            runtime.enforce(&protected).await,
            Err(CsrfRuntimeRejection::TokenMismatch)
        );

        store.unavailable();
        assert_eq!(
            runtime.enforce(&protected).await,
            Err(CsrfRuntimeRejection::StoreUnavailable)
        );
        assert_eq!(
            runtime.issue(&issue_request).await,
            Err(CsrfOperationError::StoreUnavailable)
        );

        store.delayed();
        assert_eq!(
            runtime.enforce(&protected).await,
            Err(CsrfRuntimeRejection::StoreTimeout)
        );
        assert_eq!(
            runtime.issue(&issue_request).await,
            Err(CsrfOperationError::StoreTimeout)
        );
    }

    #[test]
    fn synchronizer_public_values_and_policy_limits_are_bounded_and_redacted() {
        let store = Arc::new(TestTokenStore::default());
        let binding = CsrfSessionCookieBinding::new("session").unwrap();
        let invalid_timeout = CsrfPolicy::synchronizer(secret(8), binding.clone(), store.clone())
            .store_timeout(Duration::ZERO);
        assert_eq!(
            invalid_timeout.validate().unwrap_err().diagnostic_code(),
            "CSRF_STORE_TIMEOUT_INVALID"
        );
        let inert_timeout = CsrfPolicy::signed_double_submit(secret(8), binding)
            .store_timeout(Duration::from_secs(1));
        assert_eq!(
            inert_timeout.validate().unwrap_err().diagnostic_code(),
            "CSRF_SYNCHRONIZER_MODE_REQUIRED"
        );
        assert_eq!(
            StoredCsrfToken::from_parts("not-canonical", 1),
            Err(CsrfTokenStoreError::CorruptData)
        );

        let binding = CsrfSessionId::new("raw-session-id").unwrap();
        let key = derive_store_key(secret(8).bytes(), binding.bytes()).unwrap();
        let token = StoredCsrfToken::from_parts(&URL_SAFE_NO_PAD.encode([3_u8; 32]), 42).unwrap();
        let debug = format!("{key:?} {token:?}");
        assert!(!debug.contains("raw-session-id"));
        assert!(!debug.contains(token.token()));
        assert_ne!(key.as_bytes().as_slice(), binding.bytes());
    }

    #[derive(Clone)]
    struct CountingBinding(Arc<AtomicUsize>);

    impl CsrfSessionBinding for CountingBinding {
        fn session_id(
            &self,
            _request: &Request,
        ) -> Result<Option<CsrfSessionId>, CsrfBindingError> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(Some(CsrfSessionId::new("session-a").unwrap()))
        }
    }

    #[tokio::test]
    async fn defense_in_depth_rejects_cross_origin_before_binding_or_token_work() {
        let calls = Arc::new(AtomicUsize::new(0));
        let runtime = CompiledCsrfPolicy::try_new(&CsrfPolicy::defense_in_depth(
            secret(4),
            CountingBinding(Arc::clone(&calls)),
        ))
        .unwrap();
        let cross_site = request(
            "POST",
            "/transfer",
            &[("Host", "example.com"), ("Sec-Fetch-Site", "cross-site")],
            b"",
        )
        .await;
        assert_eq!(
            runtime.enforce(&cross_site).await,
            Err(CsrfRuntimeRejection::CrossOrigin)
        );
        assert_eq!(calls.load(Ordering::Relaxed), 0);

        let store = Arc::new(TestTokenStore::default());
        let synchronizer = CompiledCsrfPolicy::try_new(&CsrfPolicy::synchronizer_defense_in_depth(
            secret(5),
            CountingBinding(Arc::clone(&calls)),
            store.clone(),
        ))
        .unwrap();
        assert_eq!(
            synchronizer.enforce(&cross_site).await,
            Err(CsrfRuntimeRejection::CrossOrigin)
        );
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        assert!(store.values.lock().await.is_empty());
    }
}
