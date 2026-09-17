//! Shared Rustls server configuration and bounded PEM loading.
//!
//! [`RustlsConfig`] deliberately does not select application protocols. HTTP and
//! WebSocket servers add their own ALPN protocols when they bind this configuration
//! to a listener.

use std::{fmt, path::Path, sync::Arc};

use rustls::{server::WebPkiClientVerifier, RootCertStore, ServerConfig};
use rustls_pki_types::{
    pem::{PemObject as _, SectionKind},
    CertificateDer, PrivateKeyDer, PrivatePkcs1KeyDer, PrivatePkcs8KeyDer, PrivateSec1KeyDer,
};
use thiserror::Error;
use tokio::{fs::File, io::AsyncReadExt};
use zeroize::Zeroizing;

const MAX_CERTIFICATE_CHAIN_BYTES: usize = 1024 * 1024;
const MAX_PRIVATE_KEY_BYTES: usize = 128 * 1024;
const MAX_CLIENT_CA_BYTES: usize = 4 * 1024 * 1024;

/// Identifies a TLS input without retaining or exposing its filesystem path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TlsFileRole {
    /// The server's leaf certificate and optional intermediate certificates.
    ServerCertificateChain,
    /// The server's private key.
    ServerPrivateKey,
    /// The trust anchors accepted for client certificates.
    ClientCaBundle,
}

impl fmt::Display for TlsFileRole {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ServerCertificateChain => "server certificate chain",
            Self::ServerPrivateKey => "server private key",
            Self::ClientCaBundle => "client CA bundle",
        })
    }
}

/// Errors produced while loading or validating a Rustls server configuration.
///
/// Error values contain neither file paths nor PEM contents, so they are safe to
/// propagate through normal application logs.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum TlsConfigError {
    /// A TLS input could not be opened or read.
    #[error("could not read {role} ({kind:?})")]
    FileUnavailable {
        /// The kind of TLS input that failed.
        role: TlsFileRole,
        /// The stable I/O error category. The original error and path are omitted.
        kind: std::io::ErrorKind,
    },
    /// The opened path did not resolve to a regular file.
    #[error("{role} must be a regular file")]
    NotRegularFile {
        /// The kind of TLS input that failed.
        role: TlsFileRole,
    },
    /// The file had no content.
    #[error("{role} must not be empty")]
    EmptyFile {
        /// The kind of TLS input that failed.
        role: TlsFileRole,
    },
    /// The file exceeded the loader's startup memory bound.
    #[error("{role} exceeds the {max_bytes}-byte limit")]
    FileTooLarge {
        /// The kind of TLS input that failed.
        role: TlsFileRole,
        /// The configured maximum size.
        max_bytes: usize,
    },
    /// A recognized PEM block was malformed.
    #[error("{role} contains malformed PEM data")]
    MalformedPem {
        /// The kind of TLS input that failed.
        role: TlsFileRole,
    },
    /// The PEM file contained a recognized block of the wrong kind.
    #[error("{role} contains an unexpected PEM item")]
    UnexpectedPemItem {
        /// The kind of TLS input that failed.
        role: TlsFileRole,
    },
    /// No server certificate was present.
    #[error("server certificate chain contains no certificates")]
    EmptyCertificateChain,
    /// A private-key file must contain exactly one supported key.
    #[error("server private key must contain exactly one PKCS#1, PKCS#8, or SEC1 key")]
    InvalidPrivateKeyCount,
    /// No client trust anchor was present.
    #[error("client CA bundle contains no certificates")]
    EmptyClientCa,
    /// Rustls rejected a client trust anchor.
    #[error("client CA bundle contains a certificate that cannot be used as a trust anchor")]
    ClientCaRejected,
    /// Rustls could not create the required client-certificate verifier.
    #[error("could not construct the required client-certificate verifier")]
    ClientVerifierRejected,
    /// The selected crypto provider and protocol versions were incompatible.
    #[error("Rustls crypto provider does not support the default TLS protocol versions")]
    UnsupportedProtocolVersions,
    /// The server certificate or private key was invalid, unsupported, or mismatched.
    #[error("Rustls rejected the server certificate and private key")]
    CertificateKeyRejected,
}

/// A cloneable Rustls server configuration shared by Lily server crates.
///
/// Applications that need SNI, custom certificate resolvers, custom verifiers, or
/// other advanced Rustls behavior can construct [`ServerConfig`] directly and pass
/// it to [`RustlsConfig::new`]. The PEM constructors use the workspace's explicit
/// Ring crypto provider and safe default TLS protocol versions.
///
/// The configuration is loaded once. Replacing certificate files on disk requires
/// constructing a new value and restarting or explicitly rebinding the server.
#[derive(Clone)]
pub struct RustlsConfig {
    inner: Arc<ServerConfig>,
}

impl RustlsConfig {
    /// Wraps a complete Rustls server configuration without modifying it.
    #[must_use]
    pub fn new(config: Arc<ServerConfig>) -> Self {
        Self { inner: config }
    }

    /// Returns the shared Rustls server configuration.
    #[must_use]
    pub fn server_config(&self) -> Arc<ServerConfig> {
        Arc::clone(&self.inner)
    }

    /// Consumes this wrapper and returns the shared Rustls server configuration.
    #[must_use]
    pub fn into_server_config(self) -> Arc<ServerConfig> {
        self.inner
    }

    /// Loads a server certificate chain and private key without client authentication.
    ///
    /// The certificate file may contain a leaf certificate followed by intermediate
    /// certificates. The key file must contain exactly one unencrypted PKCS#1,
    /// PKCS#8, or SEC1 private key.
    pub async fn from_pem_file(
        certificate_chain_path: impl AsRef<Path>,
        private_key_path: impl AsRef<Path>,
    ) -> Result<Self, TlsConfigError> {
        let (certificate_pem, private_key_pem) = tokio::try_join!(
            read_bounded(
                certificate_chain_path.as_ref(),
                TlsFileRole::ServerCertificateChain,
                MAX_CERTIFICATE_CHAIN_BYTES,
            ),
            read_secret_bounded(
                private_key_path.as_ref(),
                TlsFileRole::ServerPrivateKey,
                MAX_PRIVATE_KEY_BYTES,
            ),
        )?;

        let certificates = parse_server_certificates(&certificate_pem)?;
        let private_key = parse_private_key(&private_key_pem)?;
        build_config(certificates, private_key, None)
    }

    /// Loads a server identity and requires a trusted certificate from every client.
    ///
    /// Every certificate in `client_ca_path` is added as a trust anchor. Anonymous
    /// clients are rejected; callers needing optional or custom client authentication
    /// should supply a fully constructed [`ServerConfig`] through [`Self::new`].
    pub async fn from_pem_file_with_client_ca(
        certificate_chain_path: impl AsRef<Path>,
        private_key_path: impl AsRef<Path>,
        client_ca_path: impl AsRef<Path>,
    ) -> Result<Self, TlsConfigError> {
        let (certificate_pem, private_key_pem, client_ca_pem) = tokio::try_join!(
            read_bounded(
                certificate_chain_path.as_ref(),
                TlsFileRole::ServerCertificateChain,
                MAX_CERTIFICATE_CHAIN_BYTES,
            ),
            read_secret_bounded(
                private_key_path.as_ref(),
                TlsFileRole::ServerPrivateKey,
                MAX_PRIVATE_KEY_BYTES,
            ),
            read_bounded(
                client_ca_path.as_ref(),
                TlsFileRole::ClientCaBundle,
                MAX_CLIENT_CA_BYTES,
            ),
        )?;

        let certificates = parse_server_certificates(&certificate_pem)?;
        let private_key = parse_private_key(&private_key_pem)?;
        let client_ca_certificates = parse_client_ca_certificates(&client_ca_pem)?;
        build_config(certificates, private_key, Some(client_ca_certificates))
    }
}

impl fmt::Debug for RustlsConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RustlsConfig")
            .finish_non_exhaustive()
    }
}

impl From<ServerConfig> for RustlsConfig {
    fn from(config: ServerConfig) -> Self {
        Self::new(Arc::new(config))
    }
}

impl From<Arc<ServerConfig>> for RustlsConfig {
    fn from(config: Arc<ServerConfig>) -> Self {
        Self::new(config)
    }
}

async fn read_bounded(
    path: &Path,
    role: TlsFileRole,
    max_bytes: usize,
) -> Result<Vec<u8>, TlsConfigError> {
    let mut contents = Vec::new();
    read_into_bounded(path, role, max_bytes, &mut contents).await?;
    Ok(contents)
}

async fn read_secret_bounded(
    path: &Path,
    role: TlsFileRole,
    max_bytes: usize,
) -> Result<Zeroizing<Vec<u8>>, TlsConfigError> {
    let mut contents = Zeroizing::new(Vec::new());
    read_into_bounded(path, role, max_bytes, &mut contents).await?;
    Ok(contents)
}

async fn read_into_bounded(
    path: &Path,
    role: TlsFileRole,
    max_bytes: usize,
    contents: &mut Vec<u8>,
) -> Result<(), TlsConfigError> {
    let file = File::open(path)
        .await
        .map_err(|error| TlsConfigError::FileUnavailable {
            role,
            kind: error.kind(),
        })?;
    let metadata = file
        .metadata()
        .await
        .map_err(|error| TlsConfigError::FileUnavailable {
            role,
            kind: error.kind(),
        })?;

    if !metadata.is_file() {
        return Err(TlsConfigError::NotRegularFile { role });
    }
    if metadata.len() > max_bytes as u64 {
        return Err(TlsConfigError::FileTooLarge { role, max_bytes });
    }

    let mut reader = file.take((max_bytes as u64).saturating_add(1));
    reader
        .read_to_end(contents)
        .await
        .map_err(|error| TlsConfigError::FileUnavailable {
            role,
            kind: error.kind(),
        })?;

    if contents.len() > max_bytes {
        return Err(TlsConfigError::FileTooLarge { role, max_bytes });
    }
    if contents.is_empty() {
        return Err(TlsConfigError::EmptyFile { role });
    }
    Ok(())
}

fn parse_server_certificates(pem: &[u8]) -> Result<Vec<CertificateDer<'static>>, TlsConfigError> {
    let certificates = parse_certificate_items(pem, TlsFileRole::ServerCertificateChain)?;
    if certificates.is_empty() {
        return Err(TlsConfigError::EmptyCertificateChain);
    }
    Ok(certificates)
}

fn parse_client_ca_certificates(
    pem: &[u8],
) -> Result<Vec<CertificateDer<'static>>, TlsConfigError> {
    let certificates = parse_certificate_items(pem, TlsFileRole::ClientCaBundle)?;
    if certificates.is_empty() {
        return Err(TlsConfigError::EmptyClientCa);
    }
    Ok(certificates)
}

fn parse_certificate_items(
    pem: &[u8],
    role: TlsFileRole,
) -> Result<Vec<CertificateDer<'static>>, TlsConfigError> {
    <(SectionKind, Vec<u8>)>::pem_slice_iter(pem)
        .map(|item| match item {
            Ok((SectionKind::Certificate, certificate)) => Ok(CertificateDer::from(certificate)),
            Ok(_) => Err(TlsConfigError::UnexpectedPemItem { role }),
            Err(_) => Err(TlsConfigError::MalformedPem { role }),
        })
        .collect()
}

fn parse_private_key(pem: &[u8]) -> Result<PrivateKeyDer<'static>, TlsConfigError> {
    let role = TlsFileRole::ServerPrivateKey;
    let mut private_key = None;

    for item in <(SectionKind, Vec<u8>)>::pem_slice_iter(pem) {
        let (kind, key) = item.map_err(|_| TlsConfigError::MalformedPem { role })?;
        let key = match kind {
            SectionKind::RsaPrivateKey => PrivateKeyDer::Pkcs1(PrivatePkcs1KeyDer::from(key)),
            SectionKind::PrivateKey => PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key)),
            SectionKind::EcPrivateKey => PrivateKeyDer::Sec1(PrivateSec1KeyDer::from(key)),
            _ => return Err(TlsConfigError::UnexpectedPemItem { role }),
        };

        if private_key.replace(key).is_some() {
            return Err(TlsConfigError::InvalidPrivateKeyCount);
        }
    }

    private_key.ok_or(TlsConfigError::InvalidPrivateKeyCount)
}

fn build_config(
    certificates: Vec<CertificateDer<'static>>,
    private_key: PrivateKeyDer<'static>,
    client_ca_certificates: Option<Vec<CertificateDer<'static>>>,
) -> Result<RustlsConfig, TlsConfigError> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = ServerConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .map_err(|_| TlsConfigError::UnsupportedProtocolVersions)?;

    let builder = if let Some(client_ca_certificates) = client_ca_certificates {
        let mut roots = RootCertStore::empty();
        for certificate in client_ca_certificates {
            roots
                .add(certificate)
                .map_err(|_| TlsConfigError::ClientCaRejected)?;
        }
        let verifier = WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider)
            .build()
            .map_err(|_| TlsConfigError::ClientVerifierRejected)?;
        builder.with_client_cert_verifier(verifier)
    } else {
        builder.with_no_client_auth()
    };

    builder
        .with_single_cert(certificates, private_key)
        .map(RustlsConfig::from)
        .map_err(|_| TlsConfigError::CertificateKeyRejected)
}

#[cfg(test)]
mod tests {
    use std::{io::Write, sync::Arc, time::Duration};

    use rcgen::{
        generate_simple_self_signed, BasicConstraints, CertificateParams, CertifiedIssuer,
        ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
    };
    use rustls::{pki_types::ServerName, ClientConfig};
    use tempfile::{tempdir, NamedTempFile};
    use tokio::{io::duplex, time::timeout};
    use tokio_rustls::{TlsAcceptor, TlsConnector};

    use super::*;

    fn write_temp(contents: &[u8]) -> NamedTempFile {
        let mut file = NamedTempFile::new().expect("create temporary TLS fixture");
        file.write_all(contents).expect("write TLS fixture");
        file
    }

    fn server_identity() -> (String, String) {
        let certified = generate_simple_self_signed(["localhost".to_owned()])
            .expect("generate test server identity");
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
        let ca_key = KeyPair::generate().expect("generate CA key");
        let ca = CertifiedIssuer::self_signed(ca_params, ca_key).expect("generate CA");

        let server_key = KeyPair::generate().expect("generate server key");
        let mut server_params =
            CertificateParams::new(["localhost".to_owned()]).expect("server parameters");
        server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let server_certificate = server_params
            .signed_by(&server_key, &ca)
            .expect("sign server certificate");

        let client_key = KeyPair::generate().expect("generate client key");
        let mut client_params =
            CertificateParams::new(["lily-test-client".to_owned()]).expect("client parameters");
        client_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        let client_certificate = client_params
            .signed_by(&client_key, &ca)
            .expect("sign client certificate");

        MtlsFixtures {
            ca_certificate: ca.pem(),
            server_certificate: server_certificate.pem(),
            server_private_key: server_key.serialize_pem(),
            client_certificate: client_certificate.pem(),
            client_private_key: client_key.serialize_pem(),
        }
    }

    fn client_config(fixtures: &MtlsFixtures, with_identity: bool) -> Arc<ClientConfig> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut roots = RootCertStore::empty();
        for certificate in
            parse_client_ca_certificates(fixtures.ca_certificate.as_bytes()).expect("client roots")
        {
            roots.add(certificate).expect("add client root");
        }
        let builder = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .expect("client protocol versions")
            .with_root_certificates(roots);

        let config = if with_identity {
            let certificates = parse_server_certificates(fixtures.client_certificate.as_bytes())
                .expect("client certificate");
            let private_key = parse_private_key(fixtures.client_private_key.as_bytes())
                .expect("client private key");
            builder
                .with_client_auth_cert(certificates, private_key)
                .expect("client identity")
        } else {
            builder.with_no_client_auth()
        };
        Arc::new(config)
    }

    #[tokio::test]
    async fn loads_a_valid_server_identity() {
        let (certificate, private_key) = server_identity();
        let certificate = write_temp(certificate.as_bytes());
        let private_key = write_temp(private_key.as_bytes());

        let config = RustlsConfig::from_pem_file(certificate.path(), private_key.path())
            .await
            .expect("load server identity");

        assert!(config.server_config().alpn_protocols.is_empty());
    }

    #[tokio::test]
    async fn rejects_multiple_private_keys() {
        let (certificate, private_key) = server_identity();
        let certificate = write_temp(certificate.as_bytes());
        let duplicated_key = format!("{private_key}\n{private_key}");
        let private_key = write_temp(duplicated_key.as_bytes());

        let error = RustlsConfig::from_pem_file(certificate.path(), private_key.path())
            .await
            .expect_err("multiple keys must be rejected");

        assert_eq!(error, TlsConfigError::InvalidPrivateKeyCount);
    }

    #[tokio::test]
    async fn rejects_an_unexpected_pem_item() {
        let (certificate_pem, _) = server_identity();
        let certificate = write_temp(certificate_pem.as_bytes());
        let certificate_as_key = write_temp(certificate_pem.as_bytes());

        let error = RustlsConfig::from_pem_file(certificate.path(), certificate_as_key.path())
            .await
            .expect_err("certificate in key file must be rejected");

        assert_eq!(
            error,
            TlsConfigError::UnexpectedPemItem {
                role: TlsFileRole::ServerPrivateKey,
            }
        );
    }

    #[tokio::test]
    async fn mtls_requires_a_trusted_client_certificate() {
        let fixtures = mtls_fixtures();
        let server_certificate = write_temp(fixtures.server_certificate.as_bytes());
        let server_private_key = write_temp(fixtures.server_private_key.as_bytes());
        let client_ca = write_temp(fixtures.ca_certificate.as_bytes());
        let server_config = RustlsConfig::from_pem_file_with_client_ca(
            server_certificate.path(),
            server_private_key.path(),
            client_ca.path(),
        )
        .await
        .expect("load mTLS server config");

        let (server_io, client_io) = duplex(16 * 1024);
        let acceptor = TlsAcceptor::from(server_config.server_config());
        let connector = TlsConnector::from(client_config(&fixtures, true));
        let server_name = ServerName::try_from("localhost").expect("server name");
        let (server_result, client_result) = timeout(Duration::from_secs(2), async {
            tokio::join!(
                acceptor.accept(server_io),
                connector.connect(server_name, client_io)
            )
        })
        .await
        .expect("authenticated handshake timed out");
        assert!(server_result.is_ok());
        assert!(client_result.is_ok());

        let (server_io, client_io) = duplex(16 * 1024);
        let acceptor = TlsAcceptor::from(server_config.server_config());
        let connector = TlsConnector::from(client_config(&fixtures, false));
        let server_name = ServerName::try_from("localhost").expect("server name");
        let (server_result, client_result) = timeout(Duration::from_secs(2), async {
            tokio::join!(
                acceptor.accept(server_io),
                connector.connect(server_name, client_io)
            )
        })
        .await
        .expect("anonymous handshake timed out");
        assert!(server_result.is_err() || client_result.is_err());
    }

    #[tokio::test]
    async fn rejects_oversized_private_key_before_parsing() {
        let (certificate, _) = server_identity();
        let certificate = write_temp(certificate.as_bytes());
        let oversized_key = vec![b'x'; MAX_PRIVATE_KEY_BYTES + 1];
        let private_key = write_temp(&oversized_key);

        let error = RustlsConfig::from_pem_file(certificate.path(), private_key.path())
            .await
            .expect_err("oversized private key must be rejected");

        assert_eq!(
            error,
            TlsConfigError::FileTooLarge {
                role: TlsFileRole::ServerPrivateKey,
                max_bytes: MAX_PRIVATE_KEY_BYTES,
            }
        );
    }

    #[tokio::test]
    async fn errors_do_not_expose_paths() {
        let directory = tempdir().expect("create temporary directory");
        let missing_path = directory.path().join("lily-secret-key-do-not-log.pem");
        let error = RustlsConfig::from_pem_file(&missing_path, &missing_path)
            .await
            .expect_err("missing files must fail");
        let rendered = format!("{error:?} {error}");

        assert!(!rendered.contains(&missing_path.to_string_lossy().into_owned()));
        assert!(!rendered.contains("do-not-log"));
    }

    #[test]
    fn debug_does_not_expose_the_inner_configuration() {
        let (certificate, private_key) = server_identity();
        let certificates = parse_server_certificates(certificate.as_bytes()).expect("certificate");
        let private_key = parse_private_key(private_key.as_bytes()).expect("private key");
        let config = build_config(certificates, private_key, None).expect("server config");

        assert_eq!(format!("{config:?}"), "RustlsConfig { .. }");
        assert!(Arc::ptr_eq(
            &config.server_config(),
            &config.server_config()
        ));
    }
}
