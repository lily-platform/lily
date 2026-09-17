//! Bounded OpenAPI document configuration and immutable publication service.
//!
//! OpenAPI remains explicitly application-owned: [`crate::AppBuilder`]
//! creates a document only when configured, and applications choose whether
//! and where to expose [`OpenApiJson`].

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

use lily_error::application::http_api::HttpApiError;
use lily_web_core::ResponseBuilder;
use utoipa::openapi::info::InfoBuilder;
use utoipa::openapi::security::{
    ApiKey, ApiKeyValue, HttpAuthScheme, HttpBuilder, OpenIdConnect, SecurityScheme,
};
use utoipa::openapi::server::ServerBuilder;
use utoipa::openapi::tag::TagBuilder;
use utoipa::openapi::{Components, OpenApi, OpenApiBuilder, Paths};

const MAX_DOCUMENT_TITLE_BYTES: usize = 128;
const MAX_DOCUMENT_VERSION_BYTES: usize = 64;
const MAX_DOCUMENT_DESCRIPTION_BYTES: usize = 8_192;
const MAX_SERVERS: usize = 16;
const MAX_SERVER_URL_BYTES: usize = 2_048;
const MAX_SERVER_DESCRIPTION_BYTES: usize = 2_048;
const MAX_TAGS: usize = 64;
const MAX_TAG_NAME_BYTES: usize = 64;
const MAX_TAG_DESCRIPTION_BYTES: usize = 2_048;
const MAX_SECURITY_SCHEMES: usize = 32;
const MAX_SECURITY_SCHEME_NAME_BYTES: usize = 64;
const MAX_SECURITY_PARAMETER_NAME_BYTES: usize = 128;
const MAX_BEARER_FORMAT_BYTES: usize = 64;
const MAX_SECURITY_DESCRIPTION_BYTES: usize = 2_048;
const MAX_OPENID_CONNECT_URL_BYTES: usize = 2_048;

/// Location used to transport an OpenAPI API key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenApiApiKeyLocation {
    /// Transport the key in an HTTP request header.
    Header,
    /// Transport the key in a query parameter.
    Query,
    /// Transport the key in a request cookie.
    Cookie,
}

/// One explicitly named OpenAPI security scheme.
#[derive(Clone)]
pub struct OpenApiSecurityScheme {
    name: String,
    kind: OpenApiSecuritySchemeKind,
    scheme: SecurityScheme,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OpenApiSecuritySchemeKind {
    Bearer,
    OpenIdConnect,
    ApiKey,
}

impl std::fmt::Debug for OpenApiSecurityScheme {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OpenApiSecurityScheme")
            .field("name", &self.name)
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

impl OpenApiSecurityScheme {
    /// Define HTTP bearer authentication.
    pub fn bearer(
        name: impl Into<String>,
        bearer_format: Option<&str>,
        description: Option<&str>,
    ) -> Result<Self, OpenApiSecurityConfigError> {
        let name = validate_scheme_name(name.into())?;
        let bearer_format = validate_optional_value(
            "bearer format",
            bearer_format.map(str::to_owned),
            MAX_BEARER_FORMAT_BYTES,
        )?;
        let description = validate_optional_value(
            "security description",
            description.map(str::to_owned),
            MAX_SECURITY_DESCRIPTION_BYTES,
        )?;
        let mut builder = HttpBuilder::new().scheme(HttpAuthScheme::Bearer);
        if let Some(format) = bearer_format {
            builder = builder.bearer_format(format);
        }
        let scheme = SecurityScheme::Http(builder.description(description).build());
        Ok(Self {
            name,
            kind: OpenApiSecuritySchemeKind::Bearer,
            scheme,
        })
    }

    /// Define an OpenID Connect discovery scheme.
    ///
    /// `discovery_url` must be an absolute `http` or `https` URL. HTTPS is
    /// recommended for production; HTTP remains explicitly supported for
    /// local development and trusted private networks.
    pub fn open_id_connect(
        name: impl Into<String>,
        discovery_url: impl Into<String>,
        description: Option<&str>,
    ) -> Result<Self, OpenApiSecurityConfigError> {
        let name = validate_scheme_name(name.into())?;
        let discovery_url = discovery_url.into();
        validate_openid_connect_url(&discovery_url)?;
        let description = validate_optional_value(
            "security description",
            description.map(str::to_owned),
            MAX_SECURITY_DESCRIPTION_BYTES,
        )?;
        let oidc = description.map_or_else(
            || OpenIdConnect::new(discovery_url.clone()),
            |description| OpenIdConnect::with_description(discovery_url.clone(), description),
        );
        Ok(Self {
            name,
            kind: OpenApiSecuritySchemeKind::OpenIdConnect,
            scheme: SecurityScheme::OpenIdConnect(oidc),
        })
    }

    /// Define an API key transported in a header, query parameter or cookie.
    pub fn api_key(
        name: impl Into<String>,
        location: OpenApiApiKeyLocation,
        parameter_name: impl Into<String>,
        description: Option<&str>,
    ) -> Result<Self, OpenApiSecurityConfigError> {
        let name = validate_scheme_name(name.into())?;
        let parameter_name = validate_parameter_name(location, parameter_name.into())?;
        let description = validate_optional_value(
            "security description",
            description.map(str::to_owned),
            MAX_SECURITY_DESCRIPTION_BYTES,
        )?;
        let value = description.map_or_else(
            || ApiKeyValue::new(parameter_name.clone()),
            |description| ApiKeyValue::with_description(parameter_name.clone(), description),
        );
        let api_key = match location {
            OpenApiApiKeyLocation::Header => ApiKey::Header(value),
            OpenApiApiKeyLocation::Query => ApiKey::Query(value),
            OpenApiApiKeyLocation::Cookie => ApiKey::Cookie(value),
        };
        Ok(Self {
            name,
            kind: OpenApiSecuritySchemeKind::ApiKey,
            scheme: SecurityScheme::ApiKey(api_key),
        })
    }

    /// Registered component name referenced by controller/action metadata.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Clone)]
struct OpenApiServerConfig {
    url: String,
    description: Option<String>,
}

#[derive(Debug, Clone)]
struct OpenApiTagConfig {
    name: String,
    description: Option<String>,
}

/// Bounded OpenAPI configuration prepared before document publication.
#[derive(Debug, Clone)]
pub struct OpenApiConfig {
    title: String,
    version: String,
    description: Option<String>,
    servers: BTreeMap<String, OpenApiServerConfig>,
    tags: BTreeMap<String, OpenApiTagConfig>,
    security_schemes: BTreeMap<String, OpenApiSecurityScheme>,
}

impl OpenApiConfig {
    /// Create the required OpenAPI info metadata.
    pub fn new(
        title: impl Into<String>,
        version: impl Into<String>,
    ) -> Result<Self, OpenApiConfigError> {
        Ok(Self {
            title: validate_document_value("title", title.into(), MAX_DOCUMENT_TITLE_BYTES)?,
            version: validate_document_value(
                "version",
                version.into(),
                MAX_DOCUMENT_VERSION_BYTES,
            )?,
            description: None,
            servers: BTreeMap::new(),
            tags: BTreeMap::new(),
            security_schemes: BTreeMap::new(),
        })
    }

    /// Add the optional document description.
    pub fn description(
        &mut self,
        description: impl Into<String>,
    ) -> Result<&mut Self, OpenApiConfigError> {
        self.description = Some(validate_document_value(
            "description",
            description.into(),
            MAX_DOCUMENT_DESCRIPTION_BYTES,
        )?);
        Ok(self)
    }

    /// Register one unique absolute HTTP(S) or root-relative server URL.
    pub fn register_server(
        &mut self,
        url: impl Into<String>,
        description: Option<&str>,
    ) -> Result<&mut Self, OpenApiConfigError> {
        if self.servers.len() >= MAX_SERVERS {
            return Err(OpenApiConfigError::TooManyServers {
                maximum: MAX_SERVERS,
            });
        }
        let url = validate_server_url(url.into())?;
        if self.servers.contains_key(&url) {
            return Err(OpenApiConfigError::DuplicateServerUrl);
        }
        let description = validate_optional_document_value(
            "server description",
            description.map(str::to_owned),
            MAX_SERVER_DESCRIPTION_BYTES,
        )?;
        self.servers
            .insert(url.clone(), OpenApiServerConfig { url, description });
        Ok(self)
    }

    /// Register one unique document tag and its optional description.
    pub fn register_tag(
        &mut self,
        name: impl Into<String>,
        description: Option<&str>,
    ) -> Result<&mut Self, OpenApiConfigError> {
        if self.tags.len() >= MAX_TAGS {
            return Err(OpenApiConfigError::TooManyTags { maximum: MAX_TAGS });
        }
        let name = validate_document_value("tag name", name.into(), MAX_TAG_NAME_BYTES)?;
        if self.tags.contains_key(&name) {
            return Err(OpenApiConfigError::DuplicateTagName { name });
        }
        let description = validate_optional_document_value(
            "tag description",
            description.map(str::to_owned),
            MAX_TAG_DESCRIPTION_BYTES,
        )?;
        self.tags
            .insert(name.clone(), OpenApiTagConfig { name, description });
        Ok(self)
    }

    /// Register one unique, explicitly named security scheme.
    pub fn register_security_scheme(
        &mut self,
        scheme: OpenApiSecurityScheme,
    ) -> Result<&mut Self, OpenApiSecurityConfigError> {
        if self.security_schemes.contains_key(&scheme.name) {
            return Err(OpenApiSecurityConfigError::DuplicateSchemeName { name: scheme.name });
        }
        if self.security_schemes.len() >= MAX_SECURITY_SCHEMES {
            return Err(OpenApiSecurityConfigError::TooManySchemes {
                maximum: MAX_SECURITY_SCHEMES,
            });
        }
        self.security_schemes.insert(scheme.name.clone(), scheme);
        Ok(self)
    }

    pub(crate) fn security_schemes(&self) -> impl Iterator<Item = (&str, &SecurityScheme)> {
        self.security_schemes
            .values()
            .map(|scheme| (scheme.name.as_str(), &scheme.scheme))
    }

    pub(crate) fn accepts_scopes(&self, name: &str) -> Option<bool> {
        self.security_schemes
            .get(name)
            .map(|scheme| scheme.kind == OpenApiSecuritySchemeKind::OpenIdConnect)
    }

    pub(crate) fn build_document(&self, paths: Paths, components: Components) -> OpenApi {
        let info = InfoBuilder::new()
            .title(self.title.clone())
            .version(self.version.clone())
            .description(self.description.clone())
            .build();
        let servers = (!self.servers.is_empty()).then(|| {
            self.servers
                .values()
                .map(|server| {
                    ServerBuilder::new()
                        .url(server.url.clone())
                        .description(server.description.clone())
                        .build()
                })
                .collect::<Vec<_>>()
        });
        let tags = (!self.tags.is_empty()).then(|| {
            self.tags
                .values()
                .map(|tag| {
                    TagBuilder::new()
                        .name(tag.name.clone())
                        .description(tag.description.clone())
                        .build()
                })
                .collect::<Vec<_>>()
        });

        OpenApiBuilder::new()
            .info(info)
            .servers(servers)
            .paths(paths)
            .components(Some(components))
            .tags(tags)
            .build()
    }
}

/// Invalid bounded OpenAPI document metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpenApiConfigError {
    /// A required document value was empty, untrimmed, or contained controls.
    InvalidValue {
        /// Stable name of the rejected metadata field.
        field: &'static str,
    },
    /// A document value exceeded its byte bound.
    ValueTooLong {
        /// Stable name of the rejected metadata field.
        field: &'static str,
        /// Maximum accepted UTF-8 byte length.
        maximum: usize,
    },
    /// A server URL was not an accepted HTTP(S) or root-relative URL.
    InvalidServerUrl,
    /// The same normalized server URL was registered more than once.
    DuplicateServerUrl,
    /// The bounded number of document servers was exceeded.
    TooManyServers {
        /// Maximum number of servers accepted by one document.
        maximum: usize,
    },
    /// The same tag name was registered more than once.
    DuplicateTagName {
        /// Duplicated tag name.
        name: String,
    },
    /// The bounded number of document tags was exceeded.
    TooManyTags {
        /// Maximum number of tags accepted by one document.
        maximum: usize,
    },
}

impl std::fmt::Display for OpenApiConfigError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidValue { field } => {
                write!(formatter, "OpenAPI {field} is invalid")
            }
            Self::ValueTooLong { field, maximum } => {
                write!(formatter, "OpenAPI {field} exceeds {maximum} bytes")
            }
            Self::InvalidServerUrl => formatter.write_str(
                "OpenAPI server URL must be absolute HTTP(S) or root-relative without credentials or a fragment",
            ),
            Self::DuplicateServerUrl => {
                formatter.write_str("duplicate OpenAPI server URL")
            }
            Self::TooManyServers { maximum } => write!(
                formatter,
                "OpenAPI server count exceeds the bounded maximum of {maximum}"
            ),
            Self::DuplicateTagName { name } => {
                write!(formatter, "duplicate OpenAPI tag name '{name}'")
            }
            Self::TooManyTags { maximum } => write!(
                formatter,
                "OpenAPI tag count exceeds the bounded maximum of {maximum}"
            ),
        }
    }
}

impl std::error::Error for OpenApiConfigError {}

struct OpenApiDocumentState {
    document: OpenApi,
    canonical_json: Arc<[u8]>,
}

/// Immutable snapshot of one application's generated OpenAPI document.
#[derive(Clone)]
pub struct OpenApiSnapshot {
    state: Arc<OpenApiDocumentState>,
}

impl std::fmt::Debug for OpenApiSnapshot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OpenApiSnapshot")
            .field("canonical_json_bytes", &self.state.canonical_json.len())
            .finish_non_exhaustive()
    }
}

impl OpenApiSnapshot {
    /// Borrow the immutable OpenAPI 3.1 model.
    #[must_use]
    pub fn document(&self) -> &OpenApi {
        &self.state.document
    }

    /// Borrow the byte-stable JSON generated once during application build.
    #[must_use]
    pub fn canonical_json(&self) -> &[u8] {
        &self.state.canonical_json
    }

    /// Create the typed JSON response used by an application-owned endpoint.
    #[must_use]
    pub fn json(&self) -> OpenApiJson {
        ResponseBuilder::new().json_bytes(self.state.canonical_json.as_ref().to_vec())
    }
}

/// Read-only, attach-once OpenAPI document handle resolved through Lily DI.
pub struct OpenApiService {
    state: OnceLock<Arc<OpenApiDocumentState>>,
}

impl std::fmt::Debug for OpenApiService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OpenApiService")
            .field("initialized", &self.state.get().is_some())
            .finish()
    }
}

impl OpenApiService {
    pub(crate) const fn new() -> Self {
        Self {
            state: OnceLock::new(),
        }
    }

    pub(crate) fn prepare(
        document: OpenApi,
    ) -> Result<OpenApiPreparedDocument, OpenApiServiceError> {
        let canonical_json =
            serde_json::to_vec(&document).map_err(|_| OpenApiServiceError::SerializationFailed)?;
        Ok(OpenApiPreparedDocument(Arc::new(OpenApiDocumentState {
            document,
            canonical_json: canonical_json.into(),
        })))
    }

    pub(crate) fn attach(
        &self,
        prepared: OpenApiPreparedDocument,
    ) -> Result<(), OpenApiServiceError> {
        self.state
            .set(prepared.0)
            .map_err(|_| OpenApiServiceError::AlreadyInitialized)
    }

    /// Return the immutable generated document after application build.
    pub fn snapshot(&self) -> Result<OpenApiSnapshot, OpenApiServiceError> {
        self.state
            .get()
            .cloned()
            .map(|state| OpenApiSnapshot { state })
            .ok_or(OpenApiServiceError::NotInitialized)
    }

    /// Convenience response for an application-owned JSON endpoint.
    pub fn json(&self) -> Result<OpenApiJson, OpenApiServiceError> {
        self.snapshot().map(|snapshot| snapshot.json())
    }
}

pub(crate) struct OpenApiPreparedDocument(Arc<OpenApiDocumentState>);

/// OpenAPI service initialization failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenApiServiceError {
    /// The application build has not attached a generated document yet.
    NotInitialized,
    /// A second document was attached to the immutable service.
    AlreadyInitialized,
    /// Lily could not serialize the generated document to canonical JSON.
    SerializationFailed,
}

impl std::fmt::Display for OpenApiServiceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotInitialized => formatter.write_str("OpenAPI document is not initialized"),
            Self::AlreadyInitialized => {
                formatter.write_str("OpenAPI document is already initialized")
            }
            Self::SerializationFailed => {
                formatter.write_str("OpenAPI document JSON serialization failed")
            }
        }
    }
}

impl std::error::Error for OpenApiServiceError {}

impl From<OpenApiServiceError> for HttpApiError {
    fn from(error: OpenApiServiceError) -> Self {
        Self::InitializationError(error.to_string())
    }
}

/// Canonical OpenAPI JSON response returned by an application-owned action.
///
/// This alias retains Lily's bounded, atomic [`ResponseBuilder`] commit path.
pub type OpenApiJson = ResponseBuilder;

/// Invalid bounded security configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpenApiSecurityConfigError {
    /// A security component name was empty, untrimmed, or contained controls.
    InvalidSchemeName,
    /// The same security component name was registered more than once.
    DuplicateSchemeName {
        /// Duplicated security scheme name.
        name: String,
    },
    /// The bounded number of security schemes was exceeded.
    TooManySchemes {
        /// Maximum number of schemes accepted by one document.
        maximum: usize,
    },
    /// An API-key parameter name was invalid for its transport location.
    InvalidParameterName {
        /// Header, query, or cookie location of the invalid name.
        location: OpenApiApiKeyLocation,
    },
    /// The OpenID Connect discovery URL was not a valid absolute HTTP(S) URL.
    InvalidOpenIdConnectUrl,
    /// A security metadata value exceeded its byte bound.
    ValueTooLong {
        /// Stable name of the rejected metadata field.
        field: &'static str,
        /// Maximum accepted UTF-8 byte length.
        maximum: usize,
    },
}

impl std::fmt::Display for OpenApiSecurityConfigError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidSchemeName => {
                formatter.write_str("OpenAPI security scheme name is invalid")
            }
            Self::DuplicateSchemeName { name } => {
                write!(formatter, "duplicate OpenAPI security scheme name '{name}'")
            }
            Self::TooManySchemes { maximum } => write!(
                formatter,
                "OpenAPI security scheme count exceeds the bounded maximum of {maximum}"
            ),
            Self::InvalidParameterName { location } => write!(
                formatter,
                "OpenAPI API key parameter name is invalid for {location:?} transport"
            ),
            Self::InvalidOpenIdConnectUrl => {
                formatter.write_str("OpenAPI OpenID Connect discovery URL is invalid")
            }
            Self::ValueTooLong { field, maximum } => {
                write!(formatter, "OpenAPI {field} exceeds {maximum} bytes")
            }
        }
    }
}

impl std::error::Error for OpenApiSecurityConfigError {}

/// Security reference validation failure produced from accepted routes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpenApiSecurityValidationError {
    /// An operation referenced a security scheme absent from the document.
    UnknownScheme {
        /// Missing security scheme name.
        name: String,
        /// HTTP method of the affected operation.
        method: String,
        /// OpenAPI path of the affected operation.
        path: String,
        /// Rust handler name that declared the reference.
        handler: String,
    },
    /// An operation assigned OAuth-style scopes to a non-scoped scheme.
    SchemeTypeMismatch {
        /// Referenced security scheme name.
        name: String,
        /// HTTP method of the affected operation.
        method: String,
        /// OpenAPI path of the affected operation.
        path: String,
        /// Rust handler name that declared the requirement.
        handler: String,
    },
    /// A route-generated component used the name of a configured scheme.
    ComponentCollision {
        /// Colliding OpenAPI component name.
        name: String,
    },
    /// An operation contained malformed security requirement metadata.
    InvalidRequirement {
        /// HTTP method of the affected operation.
        method: String,
        /// OpenAPI path of the affected operation.
        path: String,
        /// Rust handler name that declared the requirement.
        handler: String,
    },
}

impl std::fmt::Display for OpenApiSecurityValidationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownScheme { name, method, path, handler } => write!(
                formatter,
                "OpenAPI operation {method} {path} ('{handler}') references unknown security scheme '{name}'"
            ),
            Self::SchemeTypeMismatch { name, method, path, handler } => write!(
                formatter,
                "OpenAPI operation {method} {path} ('{handler}') assigns scopes to non-scoped security scheme '{name}'"
            ),
            Self::ComponentCollision { name } => write!(
                formatter,
                "OpenAPI security scheme component '{name}' conflicts with route metadata"
            ),
            Self::InvalidRequirement { method, path, handler } => write!(
                formatter,
                "OpenAPI operation {method} {path} ('{handler}') contains an invalid security requirement"
            ),
        }
    }
}

impl std::error::Error for OpenApiSecurityValidationError {}

fn validate_document_value(
    field: &'static str,
    value: String,
    maximum: usize,
) -> Result<String, OpenApiConfigError> {
    if value.is_empty() || value.trim() != value || value.chars().any(char::is_control) {
        return Err(OpenApiConfigError::InvalidValue { field });
    }
    if value.len() > maximum {
        return Err(OpenApiConfigError::ValueTooLong { field, maximum });
    }
    Ok(value)
}

fn validate_optional_document_value(
    field: &'static str,
    value: Option<String>,
    maximum: usize,
) -> Result<Option<String>, OpenApiConfigError> {
    value
        .map(|value| validate_document_value(field, value, maximum))
        .transpose()
}

fn validate_server_url(url: String) -> Result<String, OpenApiConfigError> {
    if url.is_empty()
        || url.len() > MAX_SERVER_URL_BYTES
        || url.trim() != url
        || url.chars().any(char::is_control)
        || url.contains(['{', '}'])
    {
        return Err(OpenApiConfigError::InvalidServerUrl);
    }

    let parsed = if url.starts_with('/') {
        let base = url::Url::parse("https://lily.invalid/")
            .expect("the framework-owned OpenAPI server base URL is valid");
        base.join(&url)
    } else {
        url::Url::parse(&url)
    }
    .map_err(|_| OpenApiConfigError::InvalidServerUrl)?;
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.fragment().is_some()
    {
        return Err(OpenApiConfigError::InvalidServerUrl);
    }
    Ok(url)
}

fn validate_scheme_name(name: String) -> Result<String, OpenApiSecurityConfigError> {
    if name.is_empty()
        || name.len() > MAX_SECURITY_SCHEME_NAME_BYTES
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(OpenApiSecurityConfigError::InvalidSchemeName);
    }
    Ok(name)
}

fn validate_parameter_name(
    location: OpenApiApiKeyLocation,
    name: String,
) -> Result<String, OpenApiSecurityConfigError> {
    let valid_length = !name.is_empty() && name.len() <= MAX_SECURITY_PARAMETER_NAME_BYTES;
    let valid = match location {
        OpenApiApiKeyLocation::Header => http::HeaderName::from_bytes(name.as_bytes()).is_ok(),
        OpenApiApiKeyLocation::Query | OpenApiApiKeyLocation::Cookie => name.bytes().all(is_token),
    };
    if !valid_length || !valid {
        return Err(OpenApiSecurityConfigError::InvalidParameterName { location });
    }
    Ok(name)
}

fn validate_openid_connect_url(url: &str) -> Result<(), OpenApiSecurityConfigError> {
    if url.is_empty() || url.len() > MAX_OPENID_CONNECT_URL_BYTES {
        return Err(OpenApiSecurityConfigError::InvalidOpenIdConnectUrl);
    }
    let parsed =
        url::Url::parse(url).map_err(|_| OpenApiSecurityConfigError::InvalidOpenIdConnectUrl)?;
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.fragment().is_some()
    {
        return Err(OpenApiSecurityConfigError::InvalidOpenIdConnectUrl);
    }
    Ok(())
}

fn validate_optional_value(
    field: &'static str,
    value: Option<String>,
    maximum: usize,
) -> Result<Option<String>, OpenApiSecurityConfigError> {
    if value.as_ref().is_some_and(|value| value.len() > maximum) {
        return Err(OpenApiSecurityConfigError::ValueTooLong { field, maximum });
    }
    Ok(value)
}

const fn is_token(byte: u8) -> bool {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn document_metadata_is_bounded_unique_and_deterministic() {
        assert!(matches!(
            OpenApiConfig::new(" ", "1.0.0"),
            Err(OpenApiConfigError::InvalidValue { field: "title" })
        ));
        let mut config = OpenApiConfig::new("Lily API", "1.0.0").unwrap();
        config
            .description("Bounded API document")
            .unwrap()
            .register_server("https://z.example.test", Some("Z"))
            .unwrap()
            .register_server("/api/v1", Some("Relative"))
            .unwrap()
            .register_tag("Zeta", None)
            .unwrap()
            .register_tag("Alpha", Some("First"))
            .unwrap();

        assert!(matches!(
            config.register_server("/api/v1", None),
            Err(OpenApiConfigError::DuplicateServerUrl)
        ));
        assert!(matches!(
            config.register_tag("Alpha", None),
            Err(OpenApiConfigError::DuplicateTagName { .. })
        ));
        assert!(matches!(
            config.register_server("ftp://example.test", None),
            Err(OpenApiConfigError::InvalidServerUrl)
        ));

        let document = config.build_document(Paths::new(), Components::new());
        let value = serde_json::to_value(document).unwrap();
        assert_eq!(value["servers"][0]["url"], "/api/v1");
        assert_eq!(value["servers"][1]["url"], "https://z.example.test");
        assert_eq!(value["tags"][0]["name"], "Alpha");
        assert_eq!(value["tags"][1]["name"], "Zeta");
    }

    #[test]
    fn openid_connect_discovery_supports_absolute_http_and_https_urls() {
        for url in [
            "http://127.0.0.1:8080/.well-known/openid-configuration",
            "https://identity.example.test/.well-known/openid-configuration",
        ] {
            assert!(OpenApiSecurityScheme::open_id_connect("oidc", url, None).is_ok());
        }

        assert!(matches!(
            OpenApiSecurityScheme::open_id_connect(
                "oidc",
                "ftp://identity.example.test/.well-known/openid-configuration",
                None,
            ),
            Err(OpenApiSecurityConfigError::InvalidOpenIdConnectUrl)
        ));
    }

    #[test]
    fn service_is_fail_closed_before_one_immutable_attachment() {
        let config = OpenApiConfig::new("Lily API", "1.0.0").unwrap();
        let service = OpenApiService::new();
        assert!(matches!(
            service.snapshot(),
            Err(OpenApiServiceError::NotInitialized)
        ));

        let first = OpenApiService::prepare(config.build_document(Paths::new(), Components::new()))
            .unwrap();
        service.attach(first).unwrap();
        let snapshot = service.snapshot().unwrap();
        assert!(snapshot.document().openapi == utoipa::openapi::OpenApiVersion::Version31);
        assert_eq!(
            snapshot.canonical_json(),
            serde_json::to_vec(snapshot.document()).unwrap()
        );

        let second =
            OpenApiService::prepare(config.build_document(Paths::new(), Components::new()))
                .unwrap();
        assert!(matches!(
            service.attach(second),
            Err(OpenApiServiceError::AlreadyInitialized)
        ));
    }
}
