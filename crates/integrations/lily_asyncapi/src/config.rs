use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Map, Value};

use crate::{
    AsyncApiBuildError,
    limits::{
        API_KEY_PARAMETER_BYTES, BEARER_FORMAT_BYTES, DOCUMENT_DESCRIPTION_BYTES,
        DOCUMENT_TAG_COUNT, DOCUMENT_TITLE_BYTES, DOCUMENT_VERSION_BYTES, EFFECTIVE_SECURITY_COUNT,
        HTTP_SCHEME_BYTES, OAUTH_FLOW_COUNT, OAUTH_SCOPE_COUNT, REQUIRED_SCOPE_COUNT,
        SCOPE_DESCRIPTION_BYTES, SCOPE_NAME_BYTES, SECURITY_DESCRIPTION_BYTES,
        SECURITY_SCHEME_COUNT, SERVER_COUNT, SERVER_DESCRIPTION_BYTES,
        SERVER_PROTOCOL_VERSION_BYTES, TAG_DESCRIPTION_BYTES, TAG_NAME_BYTES,
    },
    model::{AsyncApiInfo, Reference, ServerObject, TagObject},
    validation::{
        TextKind, normalize_absolute_http_url, normalize_server_host, normalize_server_pathname,
        validate_count, validate_required, validate_required_text, validate_security_name,
        validate_server_name,
    },
};

/// Application-owned, transport-neutral AsyncAPI document configuration.
///
/// This type only describes public documentation metadata. It deliberately
/// cannot carry broker credentials, TLS key material, server variables or
/// arbitrary JSON extensions.
#[derive(Clone, Debug)]
pub struct AsyncApiConfig {
    title: String,
    version: String,
    description: Option<String>,
    servers: BTreeMap<String, AsyncApiServer>,
    tags: BTreeMap<String, AsyncApiTag>,
    security_schemes: BTreeMap<String, AsyncApiSecurityScheme>,
}

impl AsyncApiConfig {
    /// Creates the required AsyncAPI Info Object metadata.
    pub fn new(
        title: impl AsRef<str>,
        version: impl AsRef<str>,
    ) -> Result<Self, AsyncApiBuildError> {
        let title = title.as_ref();
        let version = version.as_ref();
        validate_required_text("config.title", title, DOCUMENT_TITLE_BYTES, TextKind::Token)?;
        validate_required_text(
            "config.version",
            version,
            DOCUMENT_VERSION_BYTES,
            TextKind::Token,
        )?;

        Ok(Self {
            title: title.to_owned(),
            version: version.to_owned(),
            description: None,
            servers: BTreeMap::new(),
            tags: BTreeMap::new(),
            security_schemes: BTreeMap::new(),
        })
    }

    /// Sets the bounded human-readable API description.
    pub fn with_description(
        mut self,
        description: impl AsRef<str>,
    ) -> Result<Self, AsyncApiBuildError> {
        let description = description.as_ref();
        validate_required_text(
            "config.description",
            description,
            DOCUMENT_DESCRIPTION_BYTES,
            TextKind::Description,
        )?;
        self.description = Some(description.to_owned());
        Ok(self)
    }

    /// Registers one explicitly advertised public server.
    pub fn add_server(&mut self, server: AsyncApiServer) -> Result<(), AsyncApiBuildError> {
        if self.servers.contains_key(server.name()) {
            return Err(AsyncApiBuildError::duplicate("server", server.name()));
        }
        validate_count("config.servers", self.servers.len() + 1, SERVER_COUNT)?;
        self.servers.insert(server.name.clone(), server);
        Ok(())
    }

    /// Registers one document-level tag.
    pub fn add_tag(&mut self, tag: AsyncApiTag) -> Result<(), AsyncApiBuildError> {
        if self.tags.contains_key(tag.name()) {
            return Err(AsyncApiBuildError::duplicate("tag", tag.name()));
        }
        validate_count("config.tags", self.tags.len() + 1, DOCUMENT_TAG_COUNT)?;
        self.tags.insert(tag.name.clone(), tag);
        Ok(())
    }

    /// Registers one named AsyncAPI 3.1 Security Scheme Object.
    pub fn add_security_scheme(
        &mut self,
        scheme: AsyncApiSecurityScheme,
    ) -> Result<(), AsyncApiBuildError> {
        if self.security_schemes.contains_key(scheme.name()) {
            return Err(AsyncApiBuildError::duplicate(
                "security scheme",
                scheme.name(),
            ));
        }
        validate_count(
            "config.security_schemes",
            self.security_schemes.len() + 1,
            SECURITY_SCHEME_COUNT,
        )?;
        self.security_schemes.insert(scheme.name.clone(), scheme);
        Ok(())
    }

    pub(crate) fn validate_references(&self) -> Result<(), AsyncApiBuildError> {
        for server in self.servers.values() {
            for security_name in &server.security {
                if !self.security_schemes.contains_key(security_name) {
                    return Err(AsyncApiBuildError::Reference {
                        owner: format!("server:{}", server.name),
                        target: security_name.clone(),
                        detail: "security scheme is not registered in AsyncApiConfig".to_owned(),
                    });
                }
            }
        }
        Ok(())
    }

    pub(crate) fn info(&self) -> AsyncApiInfo {
        AsyncApiInfo {
            title: self.title.clone(),
            version: self.version.clone(),
            description: self.description.clone(),
            tags: self.tags.values().map(AsyncApiTag::to_object).collect(),
        }
    }

    pub(crate) fn server_objects(
        &self,
    ) -> Result<BTreeMap<String, ServerObject>, AsyncApiBuildError> {
        self.validate_references()?;
        Ok(self
            .servers
            .iter()
            .map(|(name, server)| (name.clone(), server.to_object()))
            .collect())
    }

    pub(crate) fn security_scheme_objects(&self) -> BTreeMap<String, Value> {
        self.security_schemes
            .iter()
            .map(|(name, scheme)| (name.clone(), scheme.to_value()))
            .collect()
    }

    pub(crate) fn has_servers(&self) -> bool {
        !self.servers.is_empty()
    }

    pub(crate) fn has_security_scheme(&self, name: &str) -> bool {
        self.security_schemes.contains_key(name)
    }

    pub(crate) fn amqp_server_names(&self) -> Result<Vec<String>, AsyncApiBuildError> {
        let names = self
            .servers
            .values()
            .filter(|server| {
                matches!(
                    server.protocol,
                    AsyncApiServerProtocol::Amqp | AsyncApiServerProtocol::Amqps
                )
            })
            .map(|server| server.name.clone())
            .collect::<Vec<_>>();
        if names.is_empty() {
            return Err(AsyncApiBuildError::validation(
                "config.servers",
                "a Consumer AsyncAPI document requires at least one AMQP or AMQPS server",
            ));
        }
        Ok(names)
    }
}

/// One of the four public transport protocols supported by Lily's first
/// AsyncAPI projection contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum AsyncApiServerProtocol {
    /// Unencrypted WebSocket transport.
    Ws,
    /// TLS WebSocket transport.
    Wss,
    /// Unencrypted AMQP transport.
    Amqp,
    /// TLS AMQP transport.
    Amqps,
}

impl AsyncApiServerProtocol {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Ws => "ws",
            Self::Wss => "wss",
            Self::Amqp => "amqp",
            Self::Amqps => "amqps",
        }
    }
}

/// An explicitly advertised, secret-safe AsyncAPI Server Object.
#[derive(Clone, Debug)]
pub struct AsyncApiServer {
    name: String,
    host: String,
    protocol: AsyncApiServerProtocol,
    pathname: Option<String>,
    protocol_version: Option<String>,
    description: Option<String>,
    security: BTreeSet<String>,
}

impl AsyncApiServer {
    /// Creates a server with a concrete host and no URI-template variables.
    pub fn new(
        name: impl AsRef<str>,
        host: impl AsRef<str>,
        protocol: AsyncApiServerProtocol,
    ) -> Result<Self, AsyncApiBuildError> {
        let name = name.as_ref();
        validate_server_name("server.name", name)?;
        let host = normalize_server_host("server.host", host.as_ref())?;
        Ok(Self {
            name: name.to_owned(),
            host,
            protocol,
            pathname: None,
            protocol_version: None,
            description: None,
            security: BTreeSet::new(),
        })
    }

    /// Sets a concrete public transport pathname.
    pub fn with_pathname(mut self, pathname: impl AsRef<str>) -> Result<Self, AsyncApiBuildError> {
        self.pathname = Some(normalize_server_pathname(
            "server.pathname",
            pathname.as_ref(),
        )?);
        Ok(self)
    }

    /// Sets the advertised protocol implementation version.
    pub fn with_protocol_version(
        mut self,
        version: impl AsRef<str>,
    ) -> Result<Self, AsyncApiBuildError> {
        let version = version.as_ref();
        validate_required(
            "server.protocol_version",
            version,
            SERVER_PROTOCOL_VERSION_BYTES,
        )?;
        self.protocol_version = Some(version.to_owned());
        Ok(self)
    }

    /// Sets a bounded public server description.
    pub fn with_description(
        mut self,
        description: impl AsRef<str>,
    ) -> Result<Self, AsyncApiBuildError> {
        let description = description.as_ref();
        validate_required_text(
            "server.description",
            description,
            SERVER_DESCRIPTION_BYTES,
            TextKind::Description,
        )?;
        self.description = Some(description.to_owned());
        Ok(self)
    }

    /// Adds one security alternative by its registered scheme name.
    pub fn add_security(
        &mut self,
        security_scheme: impl AsRef<str>,
    ) -> Result<(), AsyncApiBuildError> {
        let security_scheme = security_scheme.as_ref();
        validate_security_name("server.security", security_scheme)?;
        if self.security.contains(security_scheme) {
            return Err(AsyncApiBuildError::duplicate(
                "server security reference",
                security_scheme,
            ));
        }
        validate_count(
            "server.security",
            self.security.len() + 1,
            EFFECTIVE_SECURITY_COUNT,
        )?;
        self.security.insert(security_scheme.to_owned());
        Ok(())
    }

    /// Returns the stable document key for this server.
    pub fn name(&self) -> &str {
        &self.name
    }

    fn to_object(&self) -> ServerObject {
        ServerObject {
            host: self.host.clone(),
            protocol: self.protocol.as_str().to_owned(),
            pathname: self.pathname.clone(),
            protocol_version: self.protocol_version.clone(),
            description: self.description.clone(),
            security: self
                .security
                .iter()
                .map(|name| Reference::new(format!("#/components/securitySchemes/{name}")))
                .collect(),
        }
    }
}

/// A validated document-level AsyncAPI tag.
#[derive(Clone, Debug)]
pub struct AsyncApiTag {
    name: String,
    description: Option<String>,
}

impl AsyncApiTag {
    /// Creates a tag with a stable bounded name.
    pub fn new(name: impl AsRef<str>) -> Result<Self, AsyncApiBuildError> {
        let name = name.as_ref();
        validate_required("tag.name", name, TAG_NAME_BYTES)?;
        Ok(Self {
            name: name.to_owned(),
            description: None,
        })
    }

    /// Sets a bounded public tag description.
    pub fn with_description(
        mut self,
        description: impl AsRef<str>,
    ) -> Result<Self, AsyncApiBuildError> {
        let description = description.as_ref();
        validate_required_text(
            "tag.description",
            description,
            TAG_DESCRIPTION_BYTES,
            TextKind::Description,
        )?;
        self.description = Some(description.to_owned());
        Ok(self)
    }

    /// Returns the exact tag name used by document references.
    pub fn name(&self) -> &str {
        &self.name
    }

    fn to_object(&self) -> TagObject {
        TagObject {
            name: self.name.clone(),
            description: self.description.clone(),
        }
    }
}

/// Location for AsyncAPI's SASL-oriented `apiKey` security scheme.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum AsyncApiApiKeyLocation {
    /// The key is supplied as the SASL username.
    User,
    /// The key is supplied as the SASL password.
    Password,
}

impl AsyncApiApiKeyLocation {
    const fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Password => "password",
        }
    }
}

/// Location for AsyncAPI's HTTP API-key security scheme.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum AsyncApiHttpApiKeyLocation {
    /// HTTP header.
    Header,
    /// URL query parameter.
    Query,
    /// HTTP cookie.
    Cookie,
}

impl AsyncApiHttpApiKeyLocation {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Header => "header",
            Self::Query => "query",
            Self::Cookie => "cookie",
        }
    }
}

/// One official OAuth 2 flow, with the URLs required by that exact flow.
#[derive(Clone, Debug)]
pub struct AsyncApiOAuthFlow {
    kind: OAuthFlowKind,
    authorization_url: Option<String>,
    token_url: Option<String>,
    refresh_url: Option<String>,
    available_scopes: BTreeMap<String, String>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum OAuthFlowKind {
    AuthorizationCode,
    ClientCredentials,
    Implicit,
    Password,
}

impl OAuthFlowKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::AuthorizationCode => "authorizationCode",
            Self::ClientCredentials => "clientCredentials",
            Self::Implicit => "implicit",
            Self::Password => "password",
        }
    }
}

impl AsyncApiOAuthFlow {
    /// Creates an OAuth implicit flow.
    pub fn implicit(authorization_url: impl AsRef<str>) -> Result<Self, AsyncApiBuildError> {
        Self::new(
            OAuthFlowKind::Implicit,
            Some(authorization_url.as_ref()),
            None,
        )
    }

    /// Creates an OAuth resource-owner password flow.
    pub fn password(token_url: impl AsRef<str>) -> Result<Self, AsyncApiBuildError> {
        Self::new(OAuthFlowKind::Password, None, Some(token_url.as_ref()))
    }

    /// Creates an OAuth client-credentials flow.
    pub fn client_credentials(token_url: impl AsRef<str>) -> Result<Self, AsyncApiBuildError> {
        Self::new(
            OAuthFlowKind::ClientCredentials,
            None,
            Some(token_url.as_ref()),
        )
    }

    /// Creates an OAuth authorization-code flow.
    pub fn authorization_code(
        authorization_url: impl AsRef<str>,
        token_url: impl AsRef<str>,
    ) -> Result<Self, AsyncApiBuildError> {
        Self::new(
            OAuthFlowKind::AuthorizationCode,
            Some(authorization_url.as_ref()),
            Some(token_url.as_ref()),
        )
    }

    fn new(
        kind: OAuthFlowKind,
        authorization_url: Option<&str>,
        token_url: Option<&str>,
    ) -> Result<Self, AsyncApiBuildError> {
        let authorization_url = authorization_url
            .map(|url| normalize_absolute_http_url("oauth.authorization_url", url))
            .transpose()?;
        let token_url = token_url
            .map(|url| normalize_absolute_http_url("oauth.token_url", url))
            .transpose()?;
        Ok(Self {
            kind,
            authorization_url,
            token_url,
            refresh_url: None,
            available_scopes: BTreeMap::new(),
        })
    }

    /// Sets the optional absolute refresh-token URL.
    pub fn with_refresh_url(
        mut self,
        refresh_url: impl AsRef<str>,
    ) -> Result<Self, AsyncApiBuildError> {
        self.refresh_url = Some(normalize_absolute_http_url(
            "oauth.refresh_url",
            refresh_url.as_ref(),
        )?);
        Ok(self)
    }

    /// Adds one available scope and its public description.
    pub fn add_available_scope(
        &mut self,
        name: impl AsRef<str>,
        description: impl AsRef<str>,
    ) -> Result<(), AsyncApiBuildError> {
        let name = name.as_ref();
        let description = description.as_ref();
        validate_required("oauth.scope.name", name, SCOPE_NAME_BYTES)?;
        validate_required_text(
            "oauth.scope.description",
            description,
            SCOPE_DESCRIPTION_BYTES,
            TextKind::Description,
        )?;
        if self.available_scopes.contains_key(name) {
            return Err(AsyncApiBuildError::duplicate("OAuth scope", name));
        }
        validate_count(
            "oauth.available_scopes",
            self.available_scopes.len() + 1,
            OAUTH_SCOPE_COUNT,
        )?;
        self.available_scopes
            .insert(name.to_owned(), description.to_owned());
        Ok(())
    }

    fn to_value(&self) -> Value {
        let mut object = Map::new();
        if let Some(url) = &self.authorization_url {
            object.insert("authorizationUrl".to_owned(), Value::String(url.clone()));
        }
        if let Some(url) = &self.token_url {
            object.insert("tokenUrl".to_owned(), Value::String(url.clone()));
        }
        if let Some(url) = &self.refresh_url {
            object.insert("refreshUrl".to_owned(), Value::String(url.clone()));
        }
        object.insert(
            "availableScopes".to_owned(),
            Value::Object(
                self.available_scopes
                    .iter()
                    .map(|(name, description)| (name.clone(), Value::String(description.clone())))
                    .collect(),
            ),
        );
        Value::Object(object)
    }
}

/// A duplicate-free set of official OAuth 2 flows.
#[derive(Clone, Debug, Default)]
pub struct AsyncApiOAuthFlows {
    flows: BTreeMap<OAuthFlowKind, AsyncApiOAuthFlow>,
}

impl AsyncApiOAuthFlows {
    /// Creates an empty flow set. At least one flow must be added before it is
    /// accepted by [`AsyncApiSecurityScheme::oauth2`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds an official OAuth flow and rejects duplicate flow kinds.
    pub fn add_flow(&mut self, flow: AsyncApiOAuthFlow) -> Result<(), AsyncApiBuildError> {
        if self.flows.contains_key(&flow.kind) {
            return Err(AsyncApiBuildError::duplicate(
                "OAuth flow",
                flow.kind.as_str(),
            ));
        }
        validate_count("oauth.flows", self.flows.len() + 1, OAUTH_FLOW_COUNT)?;
        self.flows.insert(flow.kind, flow);
        Ok(())
    }

    fn validate_nonempty(&self) -> Result<(), AsyncApiBuildError> {
        if self.flows.is_empty() {
            return Err(AsyncApiBuildError::validation(
                "oauth.flows",
                "must contain at least one OAuth flow",
            ));
        }
        Ok(())
    }

    fn available_scope_names(&self) -> BTreeSet<&str> {
        self.flows
            .values()
            .flat_map(|flow| flow.available_scopes.keys().map(String::as_str))
            .collect()
    }

    fn to_value(&self) -> Value {
        Value::Object(
            self.flows
                .iter()
                .map(|(kind, flow)| (kind.as_str().to_owned(), flow.to_value()))
                .collect(),
        )
    }
}

/// One named, typed AsyncAPI 3.1 Security Scheme Object.
#[derive(Clone, Debug)]
pub struct AsyncApiSecurityScheme {
    name: String,
    description: Option<String>,
    kind: SecuritySchemeKind,
}

#[derive(Clone, Debug)]
enum SecuritySchemeKind {
    UserPassword,
    ApiKey(AsyncApiApiKeyLocation),
    X509,
    SymmetricEncryption,
    AsymmetricEncryption,
    HttpApiKey {
        parameter_name: String,
        location: AsyncApiHttpApiKeyLocation,
    },
    Http {
        scheme: String,
        bearer_format: Option<String>,
    },
    OAuth2 {
        flows: AsyncApiOAuthFlows,
        required_scopes: Vec<String>,
    },
    OpenIdConnect {
        discovery_url: String,
        required_scopes: Vec<String>,
    },
    Plain,
    ScramSha256,
    ScramSha512,
    Gssapi,
}

impl AsyncApiSecurityScheme {
    /// Creates a `userPassword` scheme.
    pub fn user_password(name: impl AsRef<str>) -> Result<Self, AsyncApiBuildError> {
        Self::new(name.as_ref(), SecuritySchemeKind::UserPassword)
    }

    /// Creates an `apiKey` scheme for the selected SASL credential location.
    pub fn api_key(
        name: impl AsRef<str>,
        location: AsyncApiApiKeyLocation,
    ) -> Result<Self, AsyncApiBuildError> {
        Self::new(name.as_ref(), SecuritySchemeKind::ApiKey(location))
    }

    /// Creates an `X509` scheme.
    pub fn x509(name: impl AsRef<str>) -> Result<Self, AsyncApiBuildError> {
        Self::new(name.as_ref(), SecuritySchemeKind::X509)
    }

    /// Creates a `symmetricEncryption` scheme.
    pub fn symmetric_encryption(name: impl AsRef<str>) -> Result<Self, AsyncApiBuildError> {
        Self::new(name.as_ref(), SecuritySchemeKind::SymmetricEncryption)
    }

    /// Creates an `asymmetricEncryption` scheme.
    pub fn asymmetric_encryption(name: impl AsRef<str>) -> Result<Self, AsyncApiBuildError> {
        Self::new(name.as_ref(), SecuritySchemeKind::AsymmetricEncryption)
    }

    /// Creates an HTTP API-key scheme.
    pub fn http_api_key(
        name: impl AsRef<str>,
        parameter_name: impl AsRef<str>,
        location: AsyncApiHttpApiKeyLocation,
    ) -> Result<Self, AsyncApiBuildError> {
        let parameter_name = parameter_name.as_ref();
        validate_required(
            "security.http_api_key.name",
            parameter_name,
            API_KEY_PARAMETER_BYTES,
        )?;
        Self::new(
            name.as_ref(),
            SecuritySchemeKind::HttpApiKey {
                parameter_name: parameter_name.to_owned(),
                location,
            },
        )
    }

    /// Creates an HTTP authentication scheme.
    ///
    /// `scheme` follows the HTTP authentication token grammar. A bearer format
    /// can be attached separately only when this value is `bearer`.
    pub fn http(
        name: impl AsRef<str>,
        scheme: impl AsRef<str>,
    ) -> Result<Self, AsyncApiBuildError> {
        let scheme = scheme.as_ref();
        validate_http_token("security.http.scheme", scheme, HTTP_SCHEME_BYTES)?;
        Self::new(
            name.as_ref(),
            SecuritySchemeKind::Http {
                scheme: scheme.to_ascii_lowercase(),
                bearer_format: None,
            },
        )
    }

    /// Adds the documentation-only bearer-token format hint.
    pub fn with_bearer_format(
        mut self,
        bearer_format: impl AsRef<str>,
    ) -> Result<Self, AsyncApiBuildError> {
        let SecuritySchemeKind::Http {
            scheme,
            bearer_format: current,
        } = &mut self.kind
        else {
            return Err(AsyncApiBuildError::validation(
                "security.http.bearer_format",
                "is only valid for an HTTP security scheme",
            ));
        };
        if scheme != "bearer" {
            return Err(AsyncApiBuildError::validation(
                "security.http.bearer_format",
                "is only valid when the HTTP scheme is `bearer`",
            ));
        }
        let bearer_format = bearer_format.as_ref();
        validate_required(
            "security.http.bearer_format",
            bearer_format,
            BEARER_FORMAT_BYTES,
        )?;
        *current = Some(bearer_format.to_owned());
        Ok(self)
    }

    /// Creates an OAuth 2 scheme and validates required scopes against the
    /// union of scopes advertised by its flows.
    pub fn oauth2<I, S>(
        name: impl AsRef<str>,
        flows: AsyncApiOAuthFlows,
        required_scopes: I,
    ) -> Result<Self, AsyncApiBuildError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        flows.validate_nonempty()?;
        let required_scopes = collect_required_scopes(required_scopes)?;
        let available = flows.available_scope_names();
        if let Some(scope) = required_scopes
            .iter()
            .find(|scope| !available.contains(scope.as_str()))
        {
            return Err(AsyncApiBuildError::validation(
                "security.oauth2.scopes",
                format!("required scope `{scope}` is not advertised by any configured flow"),
            ));
        }
        Self::new(
            name.as_ref(),
            SecuritySchemeKind::OAuth2 {
                flows,
                required_scopes,
            },
        )
    }

    /// Creates an OpenID Connect scheme without performing network discovery.
    pub fn open_id_connect<I, S>(
        name: impl AsRef<str>,
        discovery_url: impl AsRef<str>,
        required_scopes: I,
    ) -> Result<Self, AsyncApiBuildError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let discovery_url =
            normalize_absolute_http_url("security.open_id_connect.url", discovery_url.as_ref())?;
        Self::new(
            name.as_ref(),
            SecuritySchemeKind::OpenIdConnect {
                discovery_url,
                required_scopes: collect_required_scopes(required_scopes)?,
            },
        )
    }

    /// Creates a SASL PLAIN scheme.
    pub fn plain(name: impl AsRef<str>) -> Result<Self, AsyncApiBuildError> {
        Self::new(name.as_ref(), SecuritySchemeKind::Plain)
    }

    /// Creates a SASL SCRAM-SHA-256 scheme.
    pub fn scram_sha256(name: impl AsRef<str>) -> Result<Self, AsyncApiBuildError> {
        Self::new(name.as_ref(), SecuritySchemeKind::ScramSha256)
    }

    /// Creates a SASL SCRAM-SHA-512 scheme.
    pub fn scram_sha512(name: impl AsRef<str>) -> Result<Self, AsyncApiBuildError> {
        Self::new(name.as_ref(), SecuritySchemeKind::ScramSha512)
    }

    /// Creates a SASL GSSAPI scheme.
    pub fn gssapi(name: impl AsRef<str>) -> Result<Self, AsyncApiBuildError> {
        Self::new(name.as_ref(), SecuritySchemeKind::Gssapi)
    }

    fn new(name: &str, kind: SecuritySchemeKind) -> Result<Self, AsyncApiBuildError> {
        validate_security_name("security.name", name)?;
        Ok(Self {
            name: name.to_owned(),
            description: None,
            kind,
        })
    }

    /// Sets the bounded public description shared by all scheme kinds.
    pub fn with_description(
        mut self,
        description: impl AsRef<str>,
    ) -> Result<Self, AsyncApiBuildError> {
        let description = description.as_ref();
        validate_required_text(
            "security.description",
            description,
            SECURITY_DESCRIPTION_BYTES,
            TextKind::Description,
        )?;
        self.description = Some(description.to_owned());
        Ok(self)
    }

    /// Returns the exact component key for this scheme.
    pub fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn to_value(&self) -> Value {
        let mut object = Map::new();
        let (kind, fields): (&str, Vec<(&str, Value)>) = match &self.kind {
            SecuritySchemeKind::UserPassword => ("userPassword", vec![]),
            SecuritySchemeKind::ApiKey(location) => (
                "apiKey",
                vec![("in", Value::String(location.as_str().to_owned()))],
            ),
            SecuritySchemeKind::X509 => ("X509", vec![]),
            SecuritySchemeKind::SymmetricEncryption => ("symmetricEncryption", vec![]),
            SecuritySchemeKind::AsymmetricEncryption => ("asymmetricEncryption", vec![]),
            SecuritySchemeKind::HttpApiKey {
                parameter_name,
                location,
            } => (
                "httpApiKey",
                vec![
                    ("name", Value::String(parameter_name.clone())),
                    ("in", Value::String(location.as_str().to_owned())),
                ],
            ),
            SecuritySchemeKind::Http {
                scheme,
                bearer_format,
            } => {
                let mut fields = vec![("scheme", Value::String(scheme.clone()))];
                if let Some(format) = bearer_format {
                    fields.push(("bearerFormat", Value::String(format.clone())));
                }
                ("http", fields)
            }
            SecuritySchemeKind::OAuth2 {
                flows,
                required_scopes,
            } => (
                "oauth2",
                vec![
                    ("flows", flows.to_value()),
                    (
                        "scopes",
                        Value::Array(
                            required_scopes
                                .iter()
                                .map(|scope| Value::String(scope.clone()))
                                .collect(),
                        ),
                    ),
                ],
            ),
            SecuritySchemeKind::OpenIdConnect {
                discovery_url,
                required_scopes,
            } => (
                "openIdConnect",
                vec![
                    ("openIdConnectUrl", Value::String(discovery_url.clone())),
                    (
                        "scopes",
                        Value::Array(
                            required_scopes
                                .iter()
                                .map(|scope| Value::String(scope.clone()))
                                .collect(),
                        ),
                    ),
                ],
            ),
            SecuritySchemeKind::Plain => ("plain", vec![]),
            SecuritySchemeKind::ScramSha256 => ("scramSha256", vec![]),
            SecuritySchemeKind::ScramSha512 => ("scramSha512", vec![]),
            SecuritySchemeKind::Gssapi => ("gssapi", vec![]),
        };
        object.insert("type".to_owned(), Value::String(kind.to_owned()));
        if let Some(description) = &self.description {
            object.insert("description".to_owned(), Value::String(description.clone()));
        }
        for (key, value) in fields {
            object.insert(key.to_owned(), value);
        }
        Value::Object(object)
    }
}

fn collect_required_scopes<I, S>(scopes: I) -> Result<Vec<String>, AsyncApiBuildError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut unique = BTreeSet::new();
    for scope in scopes {
        let scope = scope.as_ref();
        validate_required("security.scope", scope, SCOPE_NAME_BYTES)?;
        if !unique.insert(scope.to_owned()) {
            return Err(AsyncApiBuildError::duplicate(
                "required security scope",
                scope,
            ));
        }
        validate_count("security.scopes", unique.len(), REQUIRED_SCOPE_COUNT)?;
    }
    Ok(unique.into_iter().collect())
}

fn validate_http_token(
    field: &'static str,
    value: &str,
    max_bytes: usize,
) -> Result<(), AsyncApiBuildError> {
    validate_required(field, value, max_bytes)?;
    if !value.bytes().all(|byte| {
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
    }) {
        return Err(AsyncApiBuildError::validation(
            field,
            "must use the HTTP token grammar",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn all_fixed_security_kinds_use_exact_asyncapi_31_tokens() {
        let schemes = [
            AsyncApiSecurityScheme::user_password("a").unwrap(),
            AsyncApiSecurityScheme::api_key("b", AsyncApiApiKeyLocation::User).unwrap(),
            AsyncApiSecurityScheme::x509("c").unwrap(),
            AsyncApiSecurityScheme::symmetric_encryption("d").unwrap(),
            AsyncApiSecurityScheme::asymmetric_encryption("e").unwrap(),
            AsyncApiSecurityScheme::http_api_key(
                "f",
                "X-API-Key",
                AsyncApiHttpApiKeyLocation::Header,
            )
            .unwrap(),
            AsyncApiSecurityScheme::http("g", "basic").unwrap(),
            AsyncApiSecurityScheme::open_id_connect(
                "h",
                "https://identity.example/.well-known/openid-configuration",
                ["openid"],
            )
            .unwrap(),
            AsyncApiSecurityScheme::plain("i").unwrap(),
            AsyncApiSecurityScheme::scram_sha256("j").unwrap(),
            AsyncApiSecurityScheme::scram_sha512("k").unwrap(),
            AsyncApiSecurityScheme::gssapi("l").unwrap(),
        ];
        let kinds: Vec<_> = schemes
            .iter()
            .map(|scheme| scheme.to_value()["type"].as_str().unwrap().to_owned())
            .collect();
        assert_eq!(
            kinds,
            [
                "userPassword",
                "apiKey",
                "X509",
                "symmetricEncryption",
                "asymmetricEncryption",
                "httpApiKey",
                "http",
                "openIdConnect",
                "plain",
                "scramSha256",
                "scramSha512",
                "gssapi",
            ]
        );
    }

    #[test]
    fn oauth2_is_exact_and_required_scopes_are_a_subset() {
        let mut implicit =
            AsyncApiOAuthFlow::implicit("https://identity.example/authorize").unwrap();
        implicit
            .add_available_scope("orders.read", "Read orders")
            .unwrap();
        let mut flows = AsyncApiOAuthFlows::new();
        flows.add_flow(implicit).unwrap();

        assert!(AsyncApiSecurityScheme::oauth2("oauth", flows.clone(), ["orders.write"]).is_err());
        let scheme = AsyncApiSecurityScheme::oauth2("oauth", flows, ["orders.read"]).unwrap();
        assert_eq!(
            scheme.to_value(),
            json!({
                "type": "oauth2",
                "flows": {
                    "implicit": {
                        "authorizationUrl": "https://identity.example/authorize",
                        "availableScopes": { "orders.read": "Read orders" }
                    }
                },
                "scopes": ["orders.read"]
            })
        );
    }

    #[test]
    fn config_rejects_unknown_server_security_reference() {
        let mut config = AsyncApiConfig::new("API", "1.0.0").unwrap();
        let mut server =
            AsyncApiServer::new("public", "example.com", AsyncApiServerProtocol::Wss).unwrap();
        server.add_security("missing").unwrap();
        config.add_server(server).unwrap();
        assert!(config.server_objects().is_err());
    }

    #[test]
    fn document_text_bounds_accept_maximum_and_reject_plus_one() {
        assert!(
            AsyncApiConfig::new(
                "t".repeat(DOCUMENT_TITLE_BYTES),
                "v".repeat(DOCUMENT_VERSION_BYTES)
            )
            .is_ok()
        );
        assert!(
            AsyncApiConfig::new(
                "t".repeat(DOCUMENT_TITLE_BYTES + 1),
                "v".repeat(DOCUMENT_VERSION_BYTES)
            )
            .is_err()
        );
        assert!(AsyncApiConfig::new("title", "v".repeat(DOCUMENT_VERSION_BYTES + 1)).is_err());

        let config = AsyncApiConfig::new("title", "1").unwrap();
        assert!(
            config
                .clone()
                .with_description("d".repeat(DOCUMENT_DESCRIPTION_BYTES))
                .is_ok()
        );
        assert!(
            config
                .with_description("d".repeat(DOCUMENT_DESCRIPTION_BYTES + 1))
                .is_err()
        );
    }

    #[test]
    fn collection_bounds_accept_maximum_and_reject_plus_one() {
        let mut servers = AsyncApiConfig::new("API", "1").unwrap();
        for index in 0..SERVER_COUNT {
            servers
                .add_server(
                    AsyncApiServer::new(
                        format!("s{index}"),
                        format!("server-{index}.example"),
                        AsyncApiServerProtocol::Wss,
                    )
                    .unwrap(),
                )
                .unwrap();
        }
        assert!(
            servers
                .add_server(
                    AsyncApiServer::new(
                        "overflow",
                        "overflow.example",
                        AsyncApiServerProtocol::Wss
                    )
                    .unwrap()
                )
                .is_err()
        );

        let mut tags = AsyncApiConfig::new("API", "1").unwrap();
        for index in 0..DOCUMENT_TAG_COUNT {
            tags.add_tag(AsyncApiTag::new(format!("tag-{index}")).unwrap())
                .unwrap();
        }
        assert!(
            tags.add_tag(AsyncApiTag::new("tag-overflow").unwrap())
                .is_err()
        );

        let mut schemes = AsyncApiConfig::new("API", "1").unwrap();
        for index in 0..SECURITY_SCHEME_COUNT {
            schemes
                .add_security_scheme(
                    AsyncApiSecurityScheme::plain(format!("plain-{index}")).unwrap(),
                )
                .unwrap();
        }
        assert!(
            schemes
                .add_security_scheme(AsyncApiSecurityScheme::plain("overflow").unwrap())
                .is_err()
        );
    }

    #[test]
    fn server_and_tag_field_bounds_are_applied_at_ingestion() {
        assert!(
            AsyncApiServer::new(
                "s".repeat(crate::limits::SERVER_NAME_BYTES),
                "example.com",
                AsyncApiServerProtocol::Wss,
            )
            .is_ok()
        );
        assert!(
            AsyncApiServer::new(
                "s".repeat(crate::limits::SERVER_NAME_BYTES + 1),
                "example.com",
                AsyncApiServerProtocol::Wss,
            )
            .is_err()
        );
        let server = AsyncApiServer::new("s", "example.com", AsyncApiServerProtocol::Wss).unwrap();
        assert!(
            server
                .clone()
                .with_protocol_version("v".repeat(SERVER_PROTOCOL_VERSION_BYTES))
                .is_ok()
        );
        assert!(
            server
                .clone()
                .with_protocol_version("v".repeat(SERVER_PROTOCOL_VERSION_BYTES + 1))
                .is_err()
        );
        assert!(
            server
                .clone()
                .with_description("d".repeat(SERVER_DESCRIPTION_BYTES))
                .is_ok()
        );
        assert!(
            server
                .with_description("d".repeat(SERVER_DESCRIPTION_BYTES + 1))
                .is_err()
        );

        assert!(AsyncApiTag::new("t".repeat(TAG_NAME_BYTES)).is_ok());
        assert!(AsyncApiTag::new("t".repeat(TAG_NAME_BYTES + 1)).is_err());
        let tag = AsyncApiTag::new("tag").unwrap();
        assert!(
            tag.clone()
                .with_description("d".repeat(TAG_DESCRIPTION_BYTES))
                .is_ok()
        );
        assert!(
            tag.with_description("d".repeat(TAG_DESCRIPTION_BYTES + 1))
                .is_err()
        );
    }

    #[test]
    fn oauth_scope_and_flow_bounds_are_fail_closed() {
        let mut flow = AsyncApiOAuthFlow::implicit("https://identity.example/authorize").unwrap();
        for index in 0..OAUTH_SCOPE_COUNT {
            flow.add_available_scope(format!("scope-{index}"), "description")
                .unwrap();
        }
        assert!(
            flow.add_available_scope("scope-overflow", "description")
                .is_err()
        );

        let mut flows = AsyncApiOAuthFlows::new();
        flows.add_flow(flow).unwrap();
        flows
            .add_flow(AsyncApiOAuthFlow::password("https://identity.example/token").unwrap())
            .unwrap();
        flows
            .add_flow(
                AsyncApiOAuthFlow::client_credentials("https://identity.example/token").unwrap(),
            )
            .unwrap();
        flows
            .add_flow(
                AsyncApiOAuthFlow::authorization_code(
                    "https://identity.example/authorize",
                    "https://identity.example/token",
                )
                .unwrap(),
            )
            .unwrap();
        assert!(
            flows
                .add_flow(AsyncApiOAuthFlow::implicit("https://other.example/authorize").unwrap())
                .is_err()
        );

        let scopes = (0..REQUIRED_SCOPE_COUNT).map(|index| format!("scope-{index}"));
        assert!(AsyncApiSecurityScheme::oauth2("oauth", flows.clone(), scopes).is_ok());
        let scopes = (0..=REQUIRED_SCOPE_COUNT).map(|index| format!("scope-{index}"));
        assert!(AsyncApiSecurityScheme::oauth2("oauth", flows, scopes).is_err());
    }

    #[test]
    fn security_text_and_url_bounds_are_applied_at_ingestion() {
        assert!(
            AsyncApiSecurityScheme::plain("s".repeat(crate::limits::SECURITY_NAME_BYTES)).is_ok()
        );
        assert!(
            AsyncApiSecurityScheme::plain("s".repeat(crate::limits::SECURITY_NAME_BYTES + 1))
                .is_err()
        );
        let scheme = AsyncApiSecurityScheme::plain("plain").unwrap();
        assert!(
            scheme
                .clone()
                .with_description("d".repeat(SECURITY_DESCRIPTION_BYTES))
                .is_ok()
        );
        assert!(
            scheme
                .with_description("d".repeat(SECURITY_DESCRIPTION_BYTES + 1))
                .is_err()
        );

        assert!(
            AsyncApiSecurityScheme::http_api_key(
                "key",
                "x".repeat(API_KEY_PARAMETER_BYTES),
                AsyncApiHttpApiKeyLocation::Header,
            )
            .is_ok()
        );
        assert!(
            AsyncApiSecurityScheme::http_api_key(
                "key",
                "x".repeat(API_KEY_PARAMETER_BYTES + 1),
                AsyncApiHttpApiKeyLocation::Header,
            )
            .is_err()
        );

        let bearer = AsyncApiSecurityScheme::http("http", "s".repeat(HTTP_SCHEME_BYTES));
        assert!(bearer.is_ok());
        assert!(AsyncApiSecurityScheme::http("http", "s".repeat(HTTP_SCHEME_BYTES + 1)).is_err());
        let bearer = AsyncApiSecurityScheme::http("bearer", "bearer").unwrap();
        assert!(
            bearer
                .clone()
                .with_bearer_format("f".repeat(BEARER_FORMAT_BYTES))
                .is_ok()
        );
        assert!(
            bearer
                .with_bearer_format("f".repeat(BEARER_FORMAT_BYTES + 1))
                .is_err()
        );

        let prefix = "https://identity.example/";
        let exact = format!(
            "{prefix}{}",
            "a".repeat(crate::limits::URL_BYTES - prefix.len())
        );
        let over = format!(
            "{prefix}{}",
            "a".repeat(crate::limits::URL_BYTES + 1 - prefix.len())
        );
        assert!(AsyncApiOAuthFlow::implicit(exact).is_ok());
        assert!(AsyncApiOAuthFlow::implicit(over).is_err());
    }
}
