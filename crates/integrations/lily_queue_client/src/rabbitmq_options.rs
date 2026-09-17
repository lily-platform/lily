use std::{
    fmt,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use lapin::{
    tcp::{OwnedIdentity, OwnedTLSConfig},
    Connection, ConnectionProperties, DefaultConnectionBuilder,
};
use lily_config::{
    QueueClientCellConfig, QueueClientConfig, RabbitMqConsumerConfig, RabbitMqTlsConfig,
};
use lily_error::application::{
    message_broker::{RabbitMQError, RabbitMqTlsErrorKind},
    MessageBrokerError,
};
use percent_encoding::percent_decode_str;
use rustls::{ClientConfig, RootCertStore};
use rustls_pki_types::{
    pem::{PemObject as _, SectionKind},
    CertificateDer, PrivateKeyDer, PrivatePkcs1KeyDer, PrivatePkcs8KeyDer, PrivateSec1KeyDer,
};
use tokio::{fs::File, io::AsyncReadExt, sync::OnceCell};
use tokio_util::sync::CancellationToken;
use url::Url;
use zeroize::Zeroizing;

const MAX_ADDITIONAL_CA_BYTES: usize = 4 * 1024 * 1024;
const MAX_CLIENT_CERTIFICATE_BYTES: usize = 1024 * 1024;
const MAX_CLIENT_PRIVATE_KEY_BYTES: usize = 128 * 1024;

/// Validated, secret-redacting RabbitMQ connection plan.
///
/// This type is hidden cross-crate ABI shared with `lily_queue`. Applications
/// configure publishers through [`lily_config::QueueClientConfig`] rather than
/// constructing a transport plan directly.
#[derive(Clone)]
pub struct RabbitMqOptions {
    connection_uri: String,
    use_tls: bool,
    tls: Option<RabbitMqTlsPlan>,
    pool_size: usize,
    connection_timeout: Duration,
    confirm_timeout: Duration,
    heartbeat_secs: u16,
    max_reconnect_attempts: u32,
    reconnect_backoff: Duration,
    persistence_enabled: bool,
}

impl RabbitMqOptions {
    /// Validates the RabbitMQ consumer configuration used by `lily_queue`.
    pub fn from_consumer(config: &RabbitMqConsumerConfig) -> Result<Self, MessageBrokerError> {
        Self::build(
            config.connection_string.as_deref(),
            config.username.as_deref(),
            config.password.as_deref(),
            config.hostname.as_deref(),
            config.port,
            config.vhost.as_deref(),
            config.use_tls,
            &config.tls,
            Some(config.pool_size),
            config.connection_timeout_secs,
            config.confirm_timeout_secs,
            config.heartbeat_secs,
            config.max_reconnect_attempts,
            config.reconnect_backoff_millis,
            Some(config.persistence_enabled),
        )
    }

    /// Validates a single-mode RabbitMQ publisher configuration.
    pub fn from_client(config: &QueueClientConfig) -> Result<Self, MessageBrokerError> {
        if config.mode.as_deref().unwrap_or("single") != "single" {
            return Err(configuration(
                "QueueClientService requires queue_client.mode = \"single\"",
            ));
        }
        Self::build(
            config.connection_string.as_deref(),
            config.username.as_deref(),
            config.password.as_deref(),
            config.hostname.as_deref(),
            config.port,
            config.vhost.as_deref(),
            config.use_tls,
            &config.tls,
            config.pool_size,
            config.connection_timeout_secs,
            config.confirm_timeout_secs,
            config.heartbeat_secs,
            config.max_reconnect_attempts,
            config.reconnect_backoff_millis,
            config.persistence_enabled,
        )
    }

    /// Validates one named factory-mode RabbitMQ publisher cell.
    pub fn from_cell(cell: &QueueClientCellConfig) -> Result<Self, MessageBrokerError> {
        Self::build(
            cell.connection_string.as_deref(),
            cell.username.as_deref(),
            cell.password.as_deref(),
            cell.hostname.as_deref(),
            cell.port,
            cell.vhost.as_deref(),
            cell.use_tls,
            &cell.tls,
            cell.pool_size,
            cell.connection_timeout_secs,
            cell.confirm_timeout_secs,
            cell.heartbeat_secs,
            cell.max_reconnect_attempts,
            cell.reconnect_backoff_millis,
            cell.persistence_enabled,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn build(
        connection_string: Option<&str>,
        username: Option<&str>,
        password: Option<&str>,
        hostname: Option<&str>,
        port: Option<u16>,
        vhost: Option<&str>,
        use_tls: Option<bool>,
        tls: &RabbitMqTlsConfig,
        pool_size: Option<usize>,
        connection_timeout_secs: Option<u64>,
        confirm_timeout_secs: Option<u64>,
        heartbeat_secs: Option<u16>,
        max_reconnect_attempts: Option<u32>,
        reconnect_backoff_millis: Option<u64>,
        persistence_enabled: Option<bool>,
    ) -> Result<Self, MessageBrokerError> {
        let use_tls = use_tls.ok_or_else(|| {
            configuration("use_tls must be explicit so AMQP transport cannot silently downgrade")
        })?;
        let tls = RabbitMqTlsPlan::new(use_tls, tls)?;
        let connection_timeout_secs = bounded_u64(
            "connection_timeout_secs",
            connection_timeout_secs.unwrap_or(10),
            1,
            120,
        )?;
        let confirm_timeout_secs = bounded_u64(
            "confirm_timeout_secs",
            confirm_timeout_secs.unwrap_or(10),
            1,
            300,
        )?;
        let heartbeat_secs = u16::try_from(bounded_u64(
            "heartbeat_secs",
            u64::from(heartbeat_secs.unwrap_or(30)),
            5,
            600,
        )?)
        .map_err(|_| configuration("heartbeat_secs exceeds AMQP range"))?;
        let pool_size = usize::try_from(bounded_u64(
            "pool_size",
            u64::try_from(pool_size.unwrap_or(1))
                .map_err(|_| configuration("pool_size exceeds platform range"))?,
            1,
            32,
        )?)
        .map_err(|_| configuration("pool_size exceeds platform range"))?;
        let max_reconnect_attempts = u32::try_from(bounded_u64(
            "max_reconnect_attempts",
            u64::from(max_reconnect_attempts.unwrap_or(5)),
            1,
            100,
        )?)
        .map_err(|_| configuration("max_reconnect_attempts exceeds range"))?;
        let reconnect_backoff_millis = bounded_u64(
            "reconnect_backoff_millis",
            reconnect_backoff_millis.unwrap_or(250),
            50,
            30_000,
        )?;

        let mut url = match connection_string {
            Some(connection_string) => {
                if username.is_some()
                    || password.is_some()
                    || hostname.is_some()
                    || port.is_some()
                    || vhost.is_some()
                {
                    return Err(configuration(
                        "connection_string cannot be combined with username/password/hostname/port/vhost",
                    ));
                }
                Url::parse(connection_string)
                    .map_err(|_| configuration("RabbitMQ connection_string is invalid"))?
            }
            None => build_url_from_fields(username, password, hostname, port, vhost, use_tls)?,
        };

        let expected_scheme = if use_tls { "amqps" } else { "amqp" };
        if url.scheme() != expected_scheme {
            return Err(configuration(format!(
                "use_tls={use_tls} requires {expected_scheme}://"
            )));
        }
        if url.host_str().is_none() || url.username().is_empty() || url.password().is_none() {
            return Err(configuration(
                "RabbitMQ URI requires host, username and password",
            ));
        }
        if url.fragment().is_some() || url.query().is_some() {
            return Err(configuration(
                "RabbitMQ connection_string query/fragment is unsupported; use typed timeout/heartbeat fields",
            ));
        }
        url.query_pairs_mut()
            .append_pair("heartbeat", &heartbeat_secs.to_string())
            .append_pair(
                "connection_timeout",
                &(connection_timeout_secs * 1_000).to_string(),
            );

        Ok(Self {
            connection_uri: url.into(),
            use_tls,
            tls,
            pool_size,
            connection_timeout: Duration::from_secs(connection_timeout_secs),
            confirm_timeout: Duration::from_secs(confirm_timeout_secs),
            heartbeat_secs,
            max_reconnect_attempts,
            reconnect_backoff: Duration::from_millis(reconnect_backoff_millis),
            persistence_enabled: persistence_enabled.unwrap_or(true),
        })
    }

    /// Establishes one configured RabbitMQ connection under the validated
    /// timeout and cancellation policy.
    pub async fn connect(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<Connection, MessageBrokerError> {
        let connect = async {
            let mut builder = DefaultConnectionBuilder::new()
                .map_err(|error| transport(error.to_string()))?
                .with_uri_str(self.connection_uri.clone())
                .with_properties(ConnectionProperties::default());
            if let Some(tls) = &self.tls {
                builder = builder.with_tls_config(tls.native_config().await?);
            }
            builder
                .connect()
                .await
                .map_err(|error| transport(error.to_string()))
        };
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(MessageBrokerError::RabbitMQError(RabbitMQError::Cancelled)),
            _ = tokio::time::sleep(self.connection_timeout) => Err(MessageBrokerError::RabbitMQError(RabbitMQError::Timeout("connection".into()))),
            result = connect => result,
        }
    }

    /// Returns the bounded application-owned connection pool size.
    pub const fn pool_size(&self) -> usize {
        self.pool_size
    }

    /// Returns the decoded RabbitMQ virtual host selected by the validated URI.
    ///
    /// This is secret-safe transport metadata used by sibling Lily adapters;
    /// the complete connection URI and credentials remain private.
    #[doc(hidden)]
    pub fn virtual_host(&self) -> Result<String, MessageBrokerError> {
        let url = Url::parse(&self.connection_uri)
            .map_err(|_| configuration("validated RabbitMQ URI could not be re-read"))?;
        decoded_virtual_host(&url)
    }

    /// Returns whether this plan requires `amqps` and TLS verification.
    pub const fn connection_uses_tls(&self) -> bool {
        self.use_tls
    }

    /// Returns the timeout for one connection attempt.
    pub const fn connection_timeout(&self) -> Duration {
        self.connection_timeout
    }

    /// Returns the publisher-confirm deadline.
    pub const fn confirm_timeout(&self) -> Duration {
        self.confirm_timeout
    }

    /// Returns the negotiated AMQP heartbeat interval in seconds.
    pub const fn heartbeat_secs(&self) -> u16 {
        self.heartbeat_secs
    }

    /// Returns the maximum number of bounded connection attempts.
    pub const fn max_reconnect_attempts(&self) -> u32 {
        self.max_reconnect_attempts
    }

    /// Returns the delay between retryable connection attempts.
    pub const fn reconnect_backoff(&self) -> Duration {
        self.reconnect_backoff
    }

    /// Returns whether published messages use persistent AMQP delivery mode.
    pub const fn persistence_enabled(&self) -> bool {
        self.persistence_enabled
    }
}

impl fmt::Debug for RabbitMqOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RabbitMqOptions")
            .field("connection_uri", &"<redacted>")
            .field("use_tls", &self.use_tls)
            .field("tls", &self.tls)
            .field("pool_size", &self.pool_size)
            .field("connection_timeout", &self.connection_timeout)
            .field("confirm_timeout", &self.confirm_timeout)
            .field("heartbeat_secs", &self.heartbeat_secs)
            .field("max_reconnect_attempts", &self.max_reconnect_attempts)
            .field("reconnect_backoff", &self.reconnect_backoff)
            .field("persistence_enabled", &self.persistence_enabled)
            .finish()
    }
}

fn decoded_virtual_host(url: &Url) -> Result<String, MessageBrokerError> {
    let path = url.path();
    if path.is_empty() {
        return Ok("/".to_owned());
    }
    // Match `amq-protocol-uri`, which removes exactly the URI's leading
    // slash. A present trailing slash therefore denotes RabbitMQ's empty
    // virtual host; it must not be rewritten to the default `/` vhost.
    let encoded = path.strip_prefix('/').unwrap_or(path);

    let decoded = percent_decode_str(encoded)
        .decode_utf8()
        .map_err(|_| configuration("RabbitMQ vhost is not valid UTF-8"))?;
    if decoded.len() > 255 || decoded.chars().any(char::is_control) {
        return Err(configuration(
            "RabbitMQ vhost must contain at most 255 control-free UTF-8 bytes",
        ));
    }
    Ok(decoded.into_owned())
}

#[derive(Clone)]
struct RabbitMqTlsPlan {
    additional_ca_bundle: Option<PathBuf>,
    client_certificate_chain: Option<PathBuf>,
    client_private_key: Option<PathBuf>,
    loaded: Arc<OnceCell<LoadedRabbitMqTls>>,
}

impl RabbitMqTlsPlan {
    fn new(use_tls: bool, config: &RabbitMqTlsConfig) -> Result<Option<Self>, MessageBrokerError> {
        let has_material = config.additional_ca_bundle.is_some()
            || config.client_certificate_chain.is_some()
            || config.client_private_key.is_some();
        if !use_tls && has_material {
            return Err(configuration(
                "RabbitMQ TLS material cannot be configured when use_tls=false",
            ));
        }
        if config.client_certificate_chain.is_some() != config.client_private_key.is_some() {
            return Err(configuration(
                "RabbitMQ mTLS requires both client_certificate_chain and client_private_key",
            ));
        }
        for path in [
            config.additional_ca_bundle.as_ref(),
            config.client_certificate_chain.as_ref(),
            config.client_private_key.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            if !path.is_absolute() {
                return Err(configuration("RabbitMQ TLS paths must be absolute"));
            }
        }
        if !has_material {
            return Ok(None);
        }
        Ok(Some(Self {
            additional_ca_bundle: config.additional_ca_bundle.clone(),
            client_certificate_chain: config.client_certificate_chain.clone(),
            client_private_key: config.client_private_key.clone(),
            loaded: Arc::new(OnceCell::new()),
        }))
    }

    async fn native_config(&self) -> Result<OwnedTLSConfig, MessageBrokerError> {
        let loaded = self.loaded.get_or_try_init(|| self.load()).await?;
        let identity = match (
            &loaded.client_certificate_pem,
            &loaded.client_private_key_pem,
        ) {
            (Some(certificate), Some(private_key)) => Some(OwnedIdentity::PKCS8 {
                pem: certificate.clone(),
                key: private_key.to_vec(),
            }),
            (None, None) => None,
            _ => return Err(configuration("RabbitMQ mTLS identity is incomplete")),
        };
        Ok(OwnedTLSConfig {
            identity,
            cert_chain: loaded.additional_ca_pem.clone(),
        })
    }

    async fn load(&self) -> Result<LoadedRabbitMqTls, MessageBrokerError> {
        let additional_ca_pem = match &self.additional_ca_bundle {
            Some(path) => {
                let pem =
                    read_bounded(path, TlsMaterialRole::AdditionalCa, MAX_ADDITIONAL_CA_BYTES)
                        .await?;
                validate_additional_ca(&pem)?;
                Some(
                    String::from_utf8(pem)
                        .map_err(|_| tls_error(RabbitMqTlsErrorKind::AdditionalCaInvalid))?,
                )
            }
            None => None,
        };

        let (client_certificate_pem, client_private_key_pem) =
            match (&self.client_certificate_chain, &self.client_private_key) {
                (Some(certificate_path), Some(private_key_path)) => {
                    let (certificate_pem, private_key_pem) = tokio::try_join!(
                        read_bounded(
                            certificate_path,
                            TlsMaterialRole::ClientCertificate,
                            MAX_CLIENT_CERTIFICATE_BYTES,
                        ),
                        read_secret_bounded(
                            private_key_path,
                            TlsMaterialRole::ClientPrivateKey,
                            MAX_CLIENT_PRIVATE_KEY_BYTES,
                        ),
                    )?;
                    validate_client_identity(&certificate_pem, &private_key_pem)?;
                    (Some(certificate_pem), Some(private_key_pem))
                }
                (None, None) => (None, None),
                _ => {
                    return Err(configuration(
                        "RabbitMQ mTLS requires both client certificate and private key",
                    ));
                }
            };

        Ok(LoadedRabbitMqTls {
            additional_ca_pem,
            client_certificate_pem,
            client_private_key_pem,
        })
    }
}

impl fmt::Debug for RabbitMqTlsPlan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RabbitMqTlsPlan")
            .field(
                "has_additional_ca_bundle",
                &self.additional_ca_bundle.is_some(),
            )
            .field(
                "has_client_identity",
                &self.client_certificate_chain.is_some(),
            )
            .finish()
    }
}

struct LoadedRabbitMqTls {
    additional_ca_pem: Option<String>,
    client_certificate_pem: Option<Vec<u8>>,
    client_private_key_pem: Option<Zeroizing<Vec<u8>>>,
}

#[derive(Clone, Copy)]
enum TlsMaterialRole {
    AdditionalCa,
    ClientCertificate,
    ClientPrivateKey,
}

impl TlsMaterialRole {
    const fn unavailable(self) -> RabbitMqTlsErrorKind {
        match self {
            Self::AdditionalCa => RabbitMqTlsErrorKind::AdditionalCaUnavailable,
            Self::ClientCertificate => RabbitMqTlsErrorKind::ClientCertificateUnavailable,
            Self::ClientPrivateKey => RabbitMqTlsErrorKind::ClientPrivateKeyUnavailable,
        }
    }

    const fn not_regular_file(self) -> RabbitMqTlsErrorKind {
        match self {
            Self::AdditionalCa => RabbitMqTlsErrorKind::AdditionalCaNotRegularFile,
            Self::ClientCertificate => RabbitMqTlsErrorKind::ClientCertificateNotRegularFile,
            Self::ClientPrivateKey => RabbitMqTlsErrorKind::ClientPrivateKeyNotRegularFile,
        }
    }

    const fn empty(self) -> RabbitMqTlsErrorKind {
        match self {
            Self::AdditionalCa => RabbitMqTlsErrorKind::AdditionalCaEmpty,
            Self::ClientCertificate => RabbitMqTlsErrorKind::ClientCertificateEmpty,
            Self::ClientPrivateKey => RabbitMqTlsErrorKind::ClientPrivateKeyEmpty,
        }
    }

    const fn too_large(self) -> RabbitMqTlsErrorKind {
        match self {
            Self::AdditionalCa => RabbitMqTlsErrorKind::AdditionalCaTooLarge,
            Self::ClientCertificate => RabbitMqTlsErrorKind::ClientCertificateTooLarge,
            Self::ClientPrivateKey => RabbitMqTlsErrorKind::ClientPrivateKeyTooLarge,
        }
    }
}

async fn read_bounded(
    path: &Path,
    role: TlsMaterialRole,
    max_bytes: usize,
) -> Result<Vec<u8>, MessageBrokerError> {
    let mut contents = Vec::new();
    read_into_bounded(path, role, max_bytes, &mut contents).await?;
    Ok(contents)
}

async fn read_secret_bounded(
    path: &Path,
    role: TlsMaterialRole,
    max_bytes: usize,
) -> Result<Zeroizing<Vec<u8>>, MessageBrokerError> {
    let mut contents = Zeroizing::new(Vec::new());
    read_into_bounded(path, role, max_bytes, &mut contents).await?;
    Ok(contents)
}

async fn read_into_bounded(
    path: &Path,
    role: TlsMaterialRole,
    max_bytes: usize,
    contents: &mut Vec<u8>,
) -> Result<(), MessageBrokerError> {
    let file = File::open(path)
        .await
        .map_err(|_| tls_error(role.unavailable()))?;
    let metadata = file
        .metadata()
        .await
        .map_err(|_| tls_error(role.unavailable()))?;
    if !metadata.is_file() {
        return Err(tls_error(role.not_regular_file()));
    }
    if metadata.len() > max_bytes as u64 {
        return Err(tls_error(role.too_large()));
    }
    let mut reader = file.take((max_bytes as u64).saturating_add(1));
    reader
        .read_to_end(contents)
        .await
        .map_err(|_| tls_error(role.unavailable()))?;
    if contents.len() > max_bytes {
        return Err(tls_error(role.too_large()));
    }
    if contents.is_empty() {
        return Err(tls_error(role.empty()));
    }
    Ok(())
}

fn validate_additional_ca(pem: &[u8]) -> Result<(), MessageBrokerError> {
    let certificates = parse_certificates(pem, RabbitMqTlsErrorKind::AdditionalCaInvalid)?;
    let mut roots = RootCertStore::empty();
    for certificate in certificates {
        roots
            .add(certificate)
            .map_err(|_| tls_error(RabbitMqTlsErrorKind::AdditionalCaInvalid))?;
    }
    Ok(())
}

fn validate_client_identity(
    certificate_pem: &[u8],
    private_key_pem: &[u8],
) -> Result<(), MessageBrokerError> {
    let certificates = parse_certificates(
        certificate_pem,
        RabbitMqTlsErrorKind::ClientCertificateInvalid,
    )?;
    let private_key = parse_private_key(private_key_pem)?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|_| tls_error(RabbitMqTlsErrorKind::UnsupportedProtocolVersions))?;
    builder
        .with_root_certificates(RootCertStore::empty())
        .with_client_auth_cert(certificates, private_key)
        .map_err(|_| tls_error(RabbitMqTlsErrorKind::ClientIdentityRejected))?;
    Ok(())
}

fn parse_certificates(
    pem: &[u8],
    invalid: RabbitMqTlsErrorKind,
) -> Result<Vec<CertificateDer<'static>>, MessageBrokerError> {
    let certificates = <(SectionKind, Vec<u8>)>::pem_slice_iter(pem)
        .map(|item| match item {
            Ok((SectionKind::Certificate, certificate)) => Ok(CertificateDer::from(certificate)),
            _ => Err(tls_error(invalid)),
        })
        .collect::<Result<Vec<_>, _>>()?;
    if certificates.is_empty() {
        return Err(tls_error(invalid));
    }
    Ok(certificates)
}

fn parse_private_key(pem: &[u8]) -> Result<PrivateKeyDer<'static>, MessageBrokerError> {
    let mut private_key = None;
    for item in <(SectionKind, Vec<u8>)>::pem_slice_iter(pem) {
        let key = match item {
            Ok((SectionKind::RsaPrivateKey, key)) => {
                PrivateKeyDer::Pkcs1(PrivatePkcs1KeyDer::from(key))
            }
            Ok((SectionKind::PrivateKey, key)) => {
                PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key))
            }
            Ok((SectionKind::EcPrivateKey, key)) => {
                PrivateKeyDer::Sec1(PrivateSec1KeyDer::from(key))
            }
            _ => return Err(tls_error(RabbitMqTlsErrorKind::ClientPrivateKeyInvalid)),
        };
        if private_key.replace(key).is_some() {
            return Err(tls_error(RabbitMqTlsErrorKind::ClientPrivateKeyInvalid));
        }
    }
    private_key.ok_or_else(|| tls_error(RabbitMqTlsErrorKind::ClientPrivateKeyInvalid))
}

fn build_url_from_fields(
    username: Option<&str>,
    password: Option<&str>,
    hostname: Option<&str>,
    port: Option<u16>,
    vhost: Option<&str>,
    use_tls: bool,
) -> Result<Url, MessageBrokerError> {
    let username = username.filter(|value| !value.is_empty()).ok_or_else(|| {
        configuration("RabbitMQ username is required when connection_string is absent")
    })?;
    let password = password.filter(|value| !value.is_empty()).ok_or_else(|| {
        configuration("RabbitMQ password is required when connection_string is absent")
    })?;
    let hostname = hostname.filter(|value| !value.is_empty()).ok_or_else(|| {
        configuration("RabbitMQ hostname is required when connection_string is absent")
    })?;
    let vhost = vhost.filter(|value| !value.is_empty()).unwrap_or("/");
    let scheme = if use_tls { "amqps" } else { "amqp" };
    let mut url = Url::parse(&format!("{scheme}://localhost"))
        .map_err(|_| configuration("failed to construct RabbitMQ URI"))?;
    url.set_host(Some(hostname))
        .map_err(|_| configuration("RabbitMQ hostname is invalid"))?;
    url.set_port(Some(port.unwrap_or(if use_tls { 5671 } else { 5672 })))
        .map_err(|_| configuration("RabbitMQ port is invalid"))?;
    url.set_username(username)
        .map_err(|_| configuration("RabbitMQ username is invalid"))?;
    url.set_password(Some(password))
        .map_err(|_| configuration("RabbitMQ password is invalid"))?;
    {
        let mut segments = url
            .path_segments_mut()
            .map_err(|_| configuration("RabbitMQ vhost cannot be encoded"))?;
        segments.clear();
        segments.push(vhost);
    }
    Ok(url)
}

fn bounded_u64(name: &str, value: u64, min: u64, max: u64) -> Result<u64, MessageBrokerError> {
    if !(min..=max).contains(&value) {
        return Err(configuration(format!(
            "{name} must be between {min} and {max}"
        )));
    }
    Ok(value)
}

fn configuration(message: impl Into<String>) -> MessageBrokerError {
    MessageBrokerError::RabbitMQError(RabbitMQError::Configuration(message.into()))
}

fn transport(message: impl Into<String>) -> MessageBrokerError {
    MessageBrokerError::RabbitMQError(RabbitMQError::Lapin(message.into()))
}

fn tls_error(kind: RabbitMqTlsErrorKind) -> MessageBrokerError {
    MessageBrokerError::RabbitMQError(RabbitMQError::Tls(kind))
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use rcgen::{
        generate_simple_self_signed, BasicConstraints, CertificateParams, CertifiedIssuer,
        ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
    };
    use rustls::server::WebPkiClientVerifier;
    use tempfile::NamedTempFile;
    use tokio::{net::TcpListener, time::timeout};
    use tokio_rustls::TlsAcceptor;

    use super::*;

    fn write_temp(contents: &[u8]) -> NamedTempFile {
        let mut file = NamedTempFile::new().expect("create RabbitMQ TLS fixture");
        file.write_all(contents)
            .expect("write RabbitMQ TLS fixture");
        file
    }

    fn identity() -> (String, String) {
        let certified = generate_simple_self_signed(["lily-rabbitmq-client".to_owned()])
            .expect("generate RabbitMQ client identity");
        (certified.cert.pem(), certified.signing_key.serialize_pem())
    }

    struct MtlsFixtures {
        ca_certificate: String,
        server_certificate: String,
        server_private_key: String,
        client_certificate: String,
        client_private_key: String,
    }

    fn mtls_fixtures() -> MtlsFixtures {
        let mut ca_params = CertificateParams::new(Vec::<String>::new()).expect("CA parameters");
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::CrlSign,
        ];
        let ca = CertifiedIssuer::self_signed(
            ca_params,
            KeyPair::generate().expect("generate RabbitMQ CA key"),
        )
        .expect("generate RabbitMQ CA");

        let server_key = KeyPair::generate().expect("generate RabbitMQ server key");
        let mut server_params =
            CertificateParams::new(["localhost".to_owned()]).expect("server parameters");
        server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let server_certificate = server_params
            .signed_by(&server_key, &ca)
            .expect("sign RabbitMQ server certificate");

        let client_key = KeyPair::generate().expect("generate RabbitMQ client key");
        let mut client_params =
            CertificateParams::new(["lily-client".to_owned()]).expect("client parameters");
        client_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        let client_certificate = client_params
            .signed_by(&client_key, &ca)
            .expect("sign RabbitMQ client certificate");

        MtlsFixtures {
            ca_certificate: ca.pem(),
            server_certificate: server_certificate.pem(),
            server_private_key: server_key.serialize_pem(),
            client_certificate: client_certificate.pem(),
            client_private_key: client_key.serialize_pem(),
        }
    }

    fn mtls_server_config(fixtures: &MtlsFixtures) -> Arc<rustls::ServerConfig> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut client_roots = RootCertStore::empty();
        for certificate in parse_certificates(
            fixtures.ca_certificate.as_bytes(),
            RabbitMqTlsErrorKind::AdditionalCaInvalid,
        )
        .expect("parse RabbitMQ client CA")
        {
            client_roots
                .add(certificate)
                .expect("add RabbitMQ client CA");
        }
        let verifier = WebPkiClientVerifier::builder_with_provider(
            Arc::new(client_roots),
            Arc::clone(&provider),
        )
        .build()
        .expect("build RabbitMQ client verifier");
        let certificates = parse_certificates(
            fixtures.server_certificate.as_bytes(),
            RabbitMqTlsErrorKind::ClientCertificateInvalid,
        )
        .expect("parse RabbitMQ server certificate");
        let private_key = parse_private_key(fixtures.server_private_key.as_bytes())
            .expect("parse RabbitMQ server private key");
        Arc::new(
            rustls::ServerConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .expect("RabbitMQ server protocol versions")
                .with_client_cert_verifier(verifier)
                .with_single_cert(certificates, private_key)
                .expect("RabbitMQ server identity"),
        )
    }

    fn client() -> QueueClientConfig {
        QueueClientConfig {
            username: Some("user:name".into()),
            password: Some("p@ss/word".into()),
            hostname: Some("rabbit.internal".into()),
            port: Some(5672),
            vhost: Some("tenant/a".into()),
            use_tls: Some(false),
            pool_size: Some(3),
            connection_timeout_secs: Some(4),
            confirm_timeout_secs: Some(2),
            heartbeat_secs: Some(20),
            max_reconnect_attempts: Some(4),
            reconnect_backoff_millis: Some(100),
            ..QueueClientConfig::default()
        }
    }

    #[test]
    fn fields_are_encoded_bounded_and_debug_is_redacted() {
        let options = RabbitMqOptions::from_client(&client()).unwrap();
        assert!(options.connection_uri.contains("user%3Aname:p%40ss%2Fword"));
        assert!(options.connection_uri.contains("tenant%2Fa"));
        assert!(options.connection_uri.contains("heartbeat=20"));
        assert_eq!(
            options.virtual_host().expect("decoded virtual host"),
            "tenant/a"
        );
        let debug = format!("{options:?}");
        assert!(!debug.contains("p@ss"));
        assert!(!debug.contains("user:name"));
    }

    #[test]
    fn connection_uri_virtual_host_is_decoded_without_exposing_credentials() {
        let options = RabbitMqOptions::from_client(&QueueClientConfig {
            connection_string: Some(
                "amqp://publisher:secret@rabbit.internal/tenant%2Forders".into(),
            ),
            use_tls: Some(false),
            ..QueueClientConfig::default()
        })
        .expect("encoded virtual host must be accepted");

        let virtual_host = options.virtual_host().expect("decoded virtual host");
        assert_eq!(virtual_host, "tenant/orders");
        assert!(!virtual_host.contains("publisher"));
        assert!(!virtual_host.contains("secret"));

        let default = RabbitMqOptions::from_client(&QueueClientConfig {
            connection_string: Some("amqp://publisher:secret@rabbit.internal".into()),
            use_tls: Some(false),
            ..QueueClientConfig::default()
        })
        .expect("URI without a path selects RabbitMQ's default virtual host");
        assert_eq!(default.virtual_host().expect("default virtual host"), "/");

        let empty = RabbitMqOptions::from_client(&QueueClientConfig {
            connection_string: Some("amqp://publisher:secret@rabbit.internal/".into()),
            use_tls: Some(false),
            ..QueueClientConfig::default()
        })
        .expect("a trailing slash is the runtime parser's explicit empty virtual host");
        assert_eq!(empty.virtual_host().expect("empty virtual host"), "");
    }

    #[test]
    fn consumer_and_publisher_use_the_same_tls_transport_contract() {
        let consumer = RabbitMqConsumerConfig {
            connection_string: Some("amqps://consumer:secret@rabbit.internal/%2f".into()),
            use_tls: Some(true),
            ..RabbitMqConsumerConfig::default()
        };
        let consumer_options =
            RabbitMqOptions::from_consumer(&consumer).expect("consumer TLS options");

        let publisher = QueueClientConfig {
            connection_string: Some("amqps://publisher:secret@rabbit.internal/%2f".into()),
            use_tls: Some(true),
            ..QueueClientConfig::default()
        };
        let publisher_options =
            RabbitMqOptions::from_client(&publisher).expect("publisher TLS options");

        assert!(consumer_options.connection_uses_tls());
        assert!(publisher_options.connection_uses_tls());
    }

    #[test]
    fn tls_mismatch_ambiguous_source_and_bounds_fail_before_io() {
        let mut config = client();
        config.use_tls = Some(true);
        assert!(RabbitMqOptions::from_client(&config).is_ok());

        config.connection_string = Some("amqp://user:pass@localhost/%2f".into());
        assert!(RabbitMqOptions::from_client(&config).is_err());

        let mut url_only = QueueClientConfig {
            connection_string: Some("amqp://user:pass@localhost/%2f".into()),
            use_tls: Some(true),
            ..QueueClientConfig::default()
        };
        assert!(RabbitMqOptions::from_client(&url_only).is_err());
        url_only.use_tls = Some(false);
        url_only.pool_size = Some(0);
        assert!(RabbitMqOptions::from_client(&url_only).is_err());
    }

    #[test]
    fn tls_material_is_fail_closed_and_paths_are_not_debugged() {
        let (certificate_pem, private_key_pem) = identity();
        let certificate = write_temp(certificate_pem.as_bytes());
        let private_key = write_temp(private_key_pem.as_bytes());
        let mut config = client();
        config.tls.client_certificate_chain = Some(certificate.path().to_path_buf());
        config.tls.client_private_key = Some(private_key.path().to_path_buf());

        let error = RabbitMqOptions::from_client(&config)
            .expect_err("TLS material over plaintext must be rejected");
        assert!(matches!(
            error,
            MessageBrokerError::RabbitMQError(RabbitMQError::Configuration(_))
        ));

        config.use_tls = Some(true);
        config.tls.client_private_key = None;
        assert!(RabbitMqOptions::from_client(&config).is_err());

        config.tls.client_private_key = Some(private_key.path().to_path_buf());
        let options = RabbitMqOptions::from_client(&config).expect("valid mTLS options");
        let debug = format!("{options:?}");
        assert!(!debug.contains(&certificate.path().to_string_lossy().into_owned()));
        assert!(!debug.contains(&private_key.path().to_string_lossy().into_owned()));
    }

    #[tokio::test]
    async fn native_tls_config_loads_valid_ca_and_mtls_identity_once() {
        let (certificate_pem, private_key_pem) = identity();
        let ca = write_temp(certificate_pem.as_bytes());
        let certificate = write_temp(certificate_pem.as_bytes());
        let private_key = write_temp(private_key_pem.as_bytes());
        let plan = RabbitMqTlsPlan::new(
            true,
            &RabbitMqTlsConfig {
                additional_ca_bundle: Some(ca.path().to_path_buf()),
                client_certificate_chain: Some(certificate.path().to_path_buf()),
                client_private_key: Some(private_key.path().to_path_buf()),
            },
        )
        .expect("valid TLS plan")
        .expect("custom TLS material");

        let native = plan.native_config().await.expect("load TLS material");
        assert!(native.cert_chain.is_some());
        assert!(native.identity.is_some());

        drop(ca);
        drop(certificate);
        drop(private_key);
        let cached = plan
            .native_config()
            .await
            .expect("reuse loaded TLS material");
        assert!(cached.cert_chain.is_some());
        assert!(cached.identity.is_some());
    }

    #[tokio::test]
    async fn lapin_native_connector_completes_a_hostname_verified_mtls_handshake() {
        let fixtures = mtls_fixtures();
        let ca = write_temp(fixtures.ca_certificate.as_bytes());
        let certificate = write_temp(fixtures.client_certificate.as_bytes());
        let private_key = write_temp(fixtures.client_private_key.as_bytes());
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind local RabbitMQ TLS fixture");
        let port = listener.local_addr().expect("fixture address").port();
        let acceptor = TlsAcceptor::from(mtls_server_config(&fixtures));

        let mut config = client();
        config.use_tls = Some(true);
        config.hostname = Some("localhost".into());
        config.port = Some(port);
        config.connection_timeout_secs = Some(3);
        config.tls = RabbitMqTlsConfig {
            additional_ca_bundle: Some(ca.path().to_path_buf()),
            client_certificate_chain: Some(certificate.path().to_path_buf()),
            client_private_key: Some(private_key.path().to_path_buf()),
        };
        let options = RabbitMqOptions::from_client(&config).expect("mTLS RabbitMQ options");
        let cancellation = CancellationToken::new();

        let server = async {
            let (stream, _) = listener.accept().await.expect("accept RabbitMQ TLS client");
            let stream = acceptor
                .accept(stream)
                .await
                .expect("Lapin must present the trusted client identity");
            drop(stream);
        };
        let ((), client_result) = timeout(Duration::from_secs(5), async {
            tokio::join!(server, options.connect(&cancellation))
        })
        .await
        .expect("RabbitMQ mTLS handshake timed out");
        assert!(matches!(
            client_result,
            Err(MessageBrokerError::RabbitMQError(RabbitMQError::Lapin(_)))
        ));
    }

    #[tokio::test]
    async fn mismatched_identity_and_oversized_key_are_typed_and_secret_safe() {
        let (certificate_pem, _) = identity();
        let (_, other_private_key_pem) = identity();
        let certificate = write_temp(certificate_pem.as_bytes());
        let private_key = write_temp(other_private_key_pem.as_bytes());
        let plan = RabbitMqTlsPlan::new(
            true,
            &RabbitMqTlsConfig {
                additional_ca_bundle: None,
                client_certificate_chain: Some(certificate.path().to_path_buf()),
                client_private_key: Some(private_key.path().to_path_buf()),
            },
        )
        .unwrap()
        .unwrap();
        let error = plan
            .native_config()
            .await
            .expect_err("mismatched identity must fail locally");
        assert!(matches!(
            error,
            MessageBrokerError::RabbitMQError(RabbitMQError::Tls(
                RabbitMqTlsErrorKind::ClientIdentityRejected
            ))
        ));

        let oversized = write_temp(&vec![b'x'; MAX_CLIENT_PRIVATE_KEY_BYTES + 1]);
        let oversized_path = oversized.path().to_path_buf();
        let plan = RabbitMqTlsPlan::new(
            true,
            &RabbitMqTlsConfig {
                additional_ca_bundle: None,
                client_certificate_chain: Some(certificate.path().to_path_buf()),
                client_private_key: Some(oversized.path().to_path_buf()),
            },
        )
        .unwrap()
        .unwrap();
        let error = plan
            .native_config()
            .await
            .expect_err("oversized private key must fail before parsing");
        assert!(matches!(
            error,
            MessageBrokerError::RabbitMQError(RabbitMQError::Tls(
                RabbitMqTlsErrorKind::ClientPrivateKeyTooLarge
            ))
        ));
        let rendered = format!("{error:?} {error}");
        assert!(!rendered.contains(&oversized_path.to_string_lossy().into_owned()));
    }
}
