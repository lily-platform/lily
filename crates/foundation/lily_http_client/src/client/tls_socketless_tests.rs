use std::sync::Arc;
use std::time::Duration;

use base64::{engine::general_purpose::STANDARD, Engine as _};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs1KeyDer, ServerName};
use rustls::{RootCertStore, ServerConfig};
use tokio::io::duplex;
use tokio::time::timeout;
use tokio_rustls::{TlsAcceptor, TlsConnector};

use super::{ClientConfig, ProtocolPreference, TransportClients, ValidatedClientProfile};

// Test-only CA, localhost certificate and key. They are DER values encoded to
// avoid checking a PEM private-key marker into the repository. The key has no
// use outside this in-memory fixture.
const TEST_CA_DER: &str = "MIIDKTCCAhGgAwIBAgIUJmvNdHYdRaCUfzzb7G5IlObIkFcwDQYJKoZIhvcNAQELBQAwHDEaMBgGA1UEAwwRTGlseS1IVFRQLVRlc3QtQ0EwHhcNMjYwNzE3MDQwMzMxWhcNMzYwNzE0MDQwMzMxWjAcMRowGAYDVQQDDBFMaWx5LUhUVFAtVGVzdC1DQTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAKWH/oHeMQEtxdqZKgaj4hFbFAN3Bo/eDM7JPrOXSsIweXff2jHVPuBXBqK3+8KbO6+lxvf3W/CPIBEoWtfsx8HV7CuMkz7PdfY3TdPvbVziPw4VXwcuBhIG3lK3j87Nruc5XzlxJiwMNL1lGCiEu6NLt3vOnpu+gKjmINiZZUg8Dv3eZLEtjnDgwpzUyPwbgB16Scpove0Ci1hZo9pJ1M0vO6dyRqxWQ61QDRnRamiVvjBbm0YB5Fr/RpMvDPoMYN8MjKjUIKhnzQ4H0c6Bpoph0WgvgTvAL+aWAqPVkmVCf3ulljooI4J6JDDT1leWHuH6D4JuM3RVyxNWuXq7D2cCAwEAAaNjMGEwHQYDVR0OBBYEFGIh1MRg66jibeKgIg0C6QRKPSZAMB8GA1UdIwQYMBaAFGIh1MRg66jibeKgIg0C6QRKPSZAMA8GA1UdEwEB/wQFMAMBAf8wDgYDVR0PAQH/BAQDAgEGMA0GCSqGSIb3DQEBCwUAA4IBAQCLEBNNlVHUyXuSCLtZdx81bq4nC9Rt3JO82BalQX2L7cuwGf20BvBc2w9awQjnqxOmEmTLfpF+bBScCfeMwcbsLt1mWn21+SuIR+Ba82I7j9UWrV3PlTZp2gXjzaCS52YBlIB4H11fxybx3xIk/DNzoVC4NXE/sX7T3itd4y8+vsz32o9R5OK5Vs9g8RDmOosTeWKrlyBGBJGJTCKUdU5kbOAHIqiaylGnaCwkuUyQEDeRQXYeHDpwW4HgjMIQS6PM7yg8s5LpVAUq1tG9dNL3XzRS7PxKh++zYvivnMwLEZ7IU5Zb3T9DNdZaJVlbWSccdE05M9zazG1BMPPJOKvc";
const TEST_SERVER_DER: &str = "MIIDOzCCAiOgAwIBAgIUR8wsrFrF1Lj4YENt1Qtxrt8PWSEwDQYJKoZIhvcNAQELBQAwHDEaMBgGA1UEAwwRTGlseS1IVFRQLVRlc3QtQ0EwHhcNMjYwNzE3MDQwMzQ2WhcNMzYwNzE0MDQwMzQ2WjAUMRIwEAYDVQQDDAlsb2NhbGhvc3QwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCT4nSPh5sP5lof8fS9K1t2dfWA1D/E4nZWl2L7g9KppzIJOUAi6Nx4Kdbj24eEFjsI7nIWeAswFzgo+HtA7knRUtZo2wubLz/9pfhkjV6tXz2z6gQ/c3+lgpzkTUG49kDSwHZnift8C6vtUtq4XG4iIL2CywSv0ESHR2ZN7BpH4urJDc0dAADu/4mYKNWgAetiPQtBPE7Xea6nfWAXVa2P3K/nlCkyDNPz8MZ2BQcGGS0adlYFC5mn+Ijhoo9MZ49+Su4aff8DsdG+b/i8OwqfEilzK27FgeN340ailA3kqQRFoW/N4qOUbQQA9lwkBsHv7c8nzW2/7WZ4kWeqk2O5AgMBAAGjfTB7MBQGA1UdEQQNMAuCCWxvY2FsaG9zdDATBgNVHSUEDDAKBggrBgEFBQcDATAOBgNVHQ8BAf8EBAMCBaAwHQYDVR0OBBYEFIhPiXSRODpnTV4GlrG0EjApw5CqMB8GA1UdIwQYMBaAFGIh1MRg66jibeKgIg0C6QRKPSZAMA0GCSqGSIb3DQEBCwUAA4IBAQBIy2V9g0o2SskhRFJYKftvW0QxSUNT2NTT31UtP3fcFE2cd8e+8kTJGFsPhvp6Aw8Pr1Fb9jJgOfBmEnNgyUv/HV9bbXLEj5+JF6sVPhjwxDyz1lHNQcTTeLZppbmn/lz/mNmJ3xMewWzOIEaczYbtW79j+u3cHV90JsCmMAntD+IOEpCgFAK6yLzcb3scB2XHXrgoivPYYuT1bYy+8AYimzObduRMX+PQmpbmkNV6c/W2fq0Gp+ChC9mpROJo2vH+BPdSwVeDp5rhLeaxbHj9c87L4dD6lqRBq+Ws0SQWcDUJmAtoOmYlekRb1DlyG0payuQl7KmReTt4OKsb2HFo";
const TEST_SERVER_KEY_DER: &str = "MIIEoQIBAAKCAQEAk+J0j4ebD+ZaH/H0vStbdnX1gNQ/xOJ2Vpdi+4PSqacyCTlAIujceCnW49uHhBY7CO5yFngLMBc4KPh7QO5J0VLWaNsLmy8//aX4ZI1erV89s+oEP3N/pYKc5E1BuPZA0sB2Z4n7fAur7VLauFxuIiC9gssEr9BEh0dmTewaR+LqyQ3NHQAA7v+JmCjVoAHrYj0LQTxO13mup31gF1Wtj9yv55QpMgzT8/DGdgUHBhktGnZWBQuZp/iI4aKPTGePfkruGn3/A7HRvm/4vDsKnxIpcytuxYHjd+NGopQN5KkERaFvzeKjlG0EAPZcJAbB7+3PJ81tv+1meJFnqpNjuQIDAQABAoH/bjzg/B1EpGrnw+huspUfblmAKLNk1d9QAjyCDKYM42qUYfZ2A4/nc6u8rx4hEYArgaeTDtdtf5Z6HBBz0HMmPmOscNLYVAC6MtpbJJmSz1UFKe3IPNmxeC9lGh/SXjkzGTy0XCT/fU3gsN3n19u5krcqjdUeKUZBz0CU19aoa0IZ+R9iecNcPCzYrqaVXNK2hwZ3xa4EBuix6t85hxd6YJZibNIdrRHVvS/b+QcPEle7Krc4afrey++8xz/opPUugtlTG2zH6DXGNRWkLuF2I9wwuJ40NDKSDHJaBewn9n8IAvu4+DJAXHFQtS9nG7duPskKD4xgD5bKxtZkPh8PAoGBAMWmkjnh4jsUWHvkJ6LKC7z9U8zA8BlZX3DiqWKkw/JlASajDI5+RPAyGFGJVLXThoTuEoBVV12goV/B2/PB233+f4Dt1TbgBih5GIMhbZPDD3piIeQoM31ewpjlQdpi9O/iP151prPlqlGwVwGr3ywNSXYwikGvVP/JGPsPNE2PAoGBAL+K0WW2Vaoh7V9HH+t+4/hDuaCzhfQnOwiQ3hWBWShRAIXlrgFUgpCeRFB9f09/1x0X5FJFPAOJ8oDtQ4/VZva2FW2uOZa60sSok847FJeyqe0c666gWFDAV9A2IlaJ/sNxAH72aiUKnZGxp+y+pWjF9SAox54rBpmeTfEVNKY3AoGBAMG0txidRV/LV9DL0QCc7ZYh3FAOQwFE8uGqcoFno1ZbIR6hq3u3So7xOZ4nbmrozKxYuq8ldIMhGybC0nL56ch4dLOB43VtZvuheqGBUGgBQpkZtcdqktPq2+KGxNxoIU88OAi2W1Nx4VM/9HWB4S3GM9nuRoGLeU1Z4+6hfwwHAoGAHwVmgGiVWyZ/gSzNuKAmX7DoQWSRz0cDQpHjxeva+rKTuRvHoKOFOdLIEZkho0h7GFUkP0bDP3d59PN4O7U+Jbq7obXT0duUAxGiToY3AZKH/sTuTqvdYcak8i2yRf23awPEJsvVyQX9GvmAztDZjSxyVLEGE1G4keyXhvH+QuUCgYBHGQgx2MYEodSXIrjTpvCobONrcGhBaaKrLR4nj9G4VI4DW4OkxHkh/lrXMG8BDd7ZtCehKNnpb5DrGmLKERtIGkRSPsRchmw9dY56hNNN6P4xotYiGPgTxHlOijqY9H3FXRlv0h3NN8b+vg3QF+SsqdfE51pltuIQFL08MmM5xQ==";

fn decode(value: &str) -> Vec<u8> {
    STANDARD.decode(value).expect("valid test fixture base64")
}

fn trusted_roots() -> RootCertStore {
    let mut roots = RootCertStore::empty();
    roots
        .add(CertificateDer::from(decode(TEST_CA_DER)))
        .expect("valid test CA");
    roots
}

#[test]
fn additional_ca_builder_keeps_verification_enabled_and_rejects_non_certificates() {
    let pem = format!(
        "-----BEGIN CERTIFICATE-----\n{}\n-----END CERTIFICATE-----\n",
        TEST_CA_DER
    );
    let builder = super::HttpClientBuilder::new()
        .additional_ca_pem(pem.as_bytes())
        .expect("test CA is accepted");
    assert_eq!(builder.additional_ca_certificates, 1);
    assert!(!format!("{builder:?}").contains(TEST_CA_DER));

    let private_key = b"-----BEGIN PRIVATE KEY-----\nAA==\n-----END PRIVATE KEY-----\n";
    assert!(super::HttpClientBuilder::new()
        .additional_ca_pem(private_key)
        .is_err());
    assert!(super::HttpClientBuilder::new()
        .additional_ca_pem(b"not pem")
        .is_err());
}

fn server_config() -> ServerConfig {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("ring supports safe TLS versions")
        .with_no_client_auth()
        .with_single_cert(
            vec![CertificateDer::from(decode(TEST_SERVER_DER))],
            PrivateKeyDer::Pkcs1(PrivatePkcs1KeyDer::from(decode(TEST_SERVER_KEY_DER))),
        )
        .expect("valid test server identity");
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    config
}

async fn successful_handshake_with_roots(
    protocol: ProtocolPreference,
    roots: RootCertStore,
) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
    let (client_io, server_io) = duplex(64 * 1024);
    let acceptor = TlsAcceptor::from(Arc::new(server_config()));
    let server = tokio::spawn(async move {
        acceptor
            .accept(server_io)
            .await
            .expect("server TLS handshake")
    });
    let client_config = TransportClients::tls_config_with_roots(roots, protocol);
    let connector = TlsConnector::from(Arc::new(client_config));
    let server_name = ServerName::try_from("localhost".to_string()).expect("valid DNS name");
    let client = timeout(
        Duration::from_secs(2),
        connector.connect(server_name, client_io),
    )
    .await
    .expect("client TLS handshake deadline")
    .expect("trusted localhost certificate");
    let server = timeout(Duration::from_secs(2), server)
        .await
        .expect("server TLS handshake deadline")
        .expect("server task");
    (
        client.get_ref().1.alpn_protocol().map(ToOwned::to_owned),
        server.get_ref().1.alpn_protocol().map(ToOwned::to_owned),
    )
}

async fn successful_handshake(protocol: ProtocolPreference) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
    successful_handshake_with_roots(protocol, trusted_roots()).await
}

#[tokio::test]
async fn production_tls_policy_negotiates_the_codec_specific_alpn() {
    for (protocol, expected) in [
        (ProtocolPreference::Auto, b"h2".as_slice()),
        (ProtocolPreference::Http1Only, b"http/1.1".as_slice()),
        (ProtocolPreference::Http2Only, b"h2".as_slice()),
    ] {
        let (client, server) = successful_handshake(protocol).await;
        assert_eq!(client.as_deref(), Some(expected));
        assert_eq!(server.as_deref(), Some(expected));
    }
}

#[tokio::test]
async fn validated_factory_profile_applies_private_ca_to_the_real_tls_policy() {
    let pem = format!(
        "-----BEGIN CERTIFICATE-----\n{}\n-----END CERTIFICATE-----\n",
        TEST_CA_DER
    );
    let profile = ValidatedClientProfile::new(ClientConfig::default(), Some(pem.as_bytes()))
        .expect("the named-client CA profile must parse");
    let client = profile.build_client().expect("validated profile builds");

    let (client_protocol, server_protocol) = successful_handshake_with_roots(
        ProtocolPreference::Auto,
        client.trust_roots.as_ref().clone(),
    )
    .await;
    assert_eq!(client_protocol.as_deref(), Some(b"h2".as_slice()));
    assert_eq!(server_protocol.as_deref(), Some(b"h2".as_slice()));
}

async fn rejected_handshake(roots: RootCertStore, hostname: &str) {
    let (client_io, server_io) = duplex(64 * 1024);
    let acceptor = TlsAcceptor::from(Arc::new(server_config()));
    let server = tokio::spawn(async move { acceptor.accept(server_io).await });
    let client_config = TransportClients::tls_config_with_roots(roots, ProtocolPreference::Auto);
    let connector = TlsConnector::from(Arc::new(client_config));
    let server_name = ServerName::try_from(hostname.to_string()).expect("valid DNS name");
    let result = timeout(
        Duration::from_secs(2),
        connector.connect(server_name, client_io),
    )
    .await
    .expect("failed handshake must remain bounded");
    assert!(result.is_err());
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn production_tls_policy_rejects_untrusted_and_wrong_hostname_certificates() {
    rejected_handshake(RootCertStore::empty(), "localhost").await;
    rejected_handshake(trusted_roots(), "not-localhost").await;
}
