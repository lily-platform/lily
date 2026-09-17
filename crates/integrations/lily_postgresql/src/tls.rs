use std::sync::Arc;

use diesel::result::ConnectionError;
use diesel_async::AsyncPgConnection;
use diesel_async::pooled_connection::{
    AsyncDieselConnectionManager, ManagerConfig, RecyclingMethod,
};
use futures_util::FutureExt;
use lily_config::PgTlsMode;
use rustls::{ClientConfig, RootCertStore};
use rustls_pki_types::{
    CertificateDer,
    pem::{PemObject as _, SectionKind},
};
use tokio_postgres::{NoTls, config::SslMode};
use tokio_postgres_rustls::MakeRustlsConnect;

use crate::plan::PgConnectionPlan;
use crate::{PgError, PgResult, PgTlsErrorKind};

pub(crate) async fn connection_manager(
    plan: &PgConnectionPlan,
) -> PgResult<(
    AsyncDieselConnectionManager<AsyncPgConnection>,
    Option<ClientConfig>,
)> {
    let tls = match plan.tls().mode {
        PgTlsMode::VerifyFull => Some(build_tls_config(plan).await?),
        PgTlsMode::Disable => None,
    };
    let mut config = ManagerConfig::<AsyncPgConnection>::default();
    config.recycling_method = RecyclingMethod::Verified;
    // Keep the same trust configuration for PostgreSQL's separate cancel connection.
    let cancellation_tls = tls.clone();
    config.custom_setup = Box::new(move |connection_string| {
        let connection_string = connection_string.to_owned();
        let tls = tls.clone();
        async move {
            let mut postgres = connection_string
                .parse::<tokio_postgres::Config>()
                .map_err(|_| generic_connection_error("invalid connection configuration"))?;
            if let Some(tls) = tls {
                postgres.ssl_mode(SslMode::Require);
                let connector = MakeRustlsConnect::new(tls);
                let (client, connection) = postgres
                    .connect(connector)
                    .await
                    .map_err(|_| generic_connection_error("TLS connection failed"))?;
                AsyncPgConnection::try_from_client_and_connection(client, connection)
                    .await
                    .map_err(|_| generic_connection_error("Diesel connection setup failed"))
            } else {
                postgres.ssl_mode(SslMode::Disable);
                let (client, connection) = postgres
                    .connect(NoTls)
                    .await
                    .map_err(|_| generic_connection_error("plaintext connection failed"))?;
                AsyncPgConnection::try_from_client_and_connection(client, connection)
                    .await
                    .map_err(|_| generic_connection_error("Diesel connection setup failed"))
            }
        }
        .boxed()
    });
    Ok((
        AsyncDieselConnectionManager::new_with_config(plan.connection_string(), config),
        cancellation_tls,
    ))
}

async fn build_tls_config(plan: &PgConnectionPlan) -> PgResult<ClientConfig> {
    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    if let Some(path) = &plan.tls().additional_ca_bundle {
        let pem = tokio::fs::read(path).await.map_err(|_| PgError::Tls {
            kind: PgTlsErrorKind::CaBundleRead,
        })?;
        add_ca_bundle(&mut roots, &pem)?;
    }

    // Select the same explicit ring provider as Lily's HTTP/OIDC TLS stacks.
    // Qualification binaries intentionally compose crates that may also enable
    // rustls' aws-lc feature; the process-wide feature union must not make
    // PostgreSQL TLS provider selection ambiguous or panic.
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    Ok(ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|_| PgError::Tls {
            kind: PgTlsErrorKind::InvalidConfiguration,
        })?
        .with_root_certificates(roots)
        .with_no_client_auth())
}

fn add_ca_bundle(roots: &mut RootCertStore, pem: &[u8]) -> PgResult<()> {
    let mut certificate_count = 0usize;
    for item in <(SectionKind, Vec<u8>)>::pem_slice_iter(pem) {
        match item.map_err(|_| PgError::Tls {
            kind: PgTlsErrorKind::InvalidCaCertificate,
        })? {
            (SectionKind::Certificate, certificate) => {
                roots
                    .add(CertificateDer::from(certificate))
                    .map_err(|_| PgError::Tls {
                        kind: PgTlsErrorKind::InvalidCaCertificate,
                    })?;
                certificate_count += 1;
            }
            (SectionKind::RsaPrivateKey, _)
            | (SectionKind::PrivateKey, _)
            | (SectionKind::EcPrivateKey, _) => {
                return Err(PgError::Tls {
                    kind: PgTlsErrorKind::PrivateKeyInCaBundle,
                });
            }
            (SectionKind::PublicKey, _) | (SectionKind::Crl, _) | (SectionKind::Csr, _) => {}
            _ => {
                return Err(PgError::Tls {
                    kind: PgTlsErrorKind::InvalidCaCertificate,
                });
            }
        }
    }
    if certificate_count == 0 {
        return Err(PgError::Tls {
            kind: PgTlsErrorKind::InvalidCaCertificate,
        });
    }
    Ok(())
}

fn generic_connection_error(category: &'static str) -> ConnectionError {
    ConnectionError::BadConnection(category.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn additional_ca_bundle_rejects_private_keys() {
        let mut roots = RootCertStore::empty();
        let error = add_ca_bundle(
            &mut roots,
            b"-----BEGIN PRIVATE KEY-----\nAA==\n-----END PRIVATE KEY-----\n",
        )
        .unwrap_err();
        assert_eq!(
            error,
            PgError::Tls {
                kind: PgTlsErrorKind::PrivateKeyInCaBundle
            }
        );
    }

    #[test]
    fn additional_ca_bundle_requires_at_least_one_certificate() {
        let mut roots = RootCertStore::empty();
        assert_eq!(
            add_ca_bundle(&mut roots, b""),
            Err(PgError::Tls {
                kind: PgTlsErrorKind::InvalidCaCertificate
            })
        );
    }
}
