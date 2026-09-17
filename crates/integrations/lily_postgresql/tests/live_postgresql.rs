//! Host/provider PostgreSQL qualification fixtures.
//!
//! These tests are intentionally ignored. They require dedicated PostgreSQL
//! instances and real TLS material; a successful socketless unit test is not a
//! substitute for these provider claims.

use std::ffi::{OsStr, OsString};

use lily_injection::ApplicationContainer;
use lily_postgresql::PgDatabaseService;

static ENVIRONMENT: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct BootstrapEnvironment {
    path: Option<OsString>,
    mode: Option<OsString>,
}

impl BootstrapEnvironment {
    fn install(path: &OsStr) -> Self {
        let previous = Self {
            path: std::env::var_os("LILY_CONFIG_PATH"),
            mode: std::env::var_os("LILY_CONFIG_MODE"),
        };
        // SAFETY: all tests in this integration binary serialize bootstrap
        // environment mutation with `ENVIRONMENT` and do not spawn threads
        // before ApplicationContainer has consumed the values.
        unsafe {
            std::env::set_var("LILY_CONFIG_PATH", path);
            std::env::set_var("LILY_CONFIG_MODE", "production");
        }
        previous
    }
}

impl Drop for BootstrapEnvironment {
    fn drop(&mut self) {
        // SAFETY: see `install`; the async environment mutex remains held until
        // this guard is dropped.
        unsafe {
            restore("LILY_CONFIG_PATH", self.path.take());
            restore("LILY_CONFIG_MODE", self.mode.take());
        }
    }
}

unsafe fn restore(name: &str, value: Option<OsString>) {
    if let Some(value) = value {
        // SAFETY: caller holds the integration-test environment mutex.
        unsafe { std::env::set_var(name, value) };
    } else {
        // SAFETY: caller holds the integration-test environment mutex.
        unsafe { std::env::remove_var(name) };
    }
}

fn required_path(name: &str) -> OsString {
    std::env::var_os(name).unwrap_or_else(|| {
        panic!("{name} must point to a production lily.toml fixture for this ignored live test")
    })
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a dedicated authenticated PostgreSQL TLS fixture"]
async fn authenticated_readiness_scoped_acquire_and_idempotent_shutdown() {
    let _environment = ENVIRONMENT.lock().await;
    let path = required_path("LILY_PG_LIVE_CONFIG");
    let _bootstrap = BootstrapEnvironment::install(&path);

    let application = ApplicationContainer::build()
        .await
        .expect("valid PostgreSQL fixture must pass authenticated readiness");
    let database = application
        .resolve::<PgDatabaseService>(None)
        .await
        .expect("DI must publish the ready PostgreSQL service");

    let status = database.status().expect("ready pool must expose status");
    assert!(status.size >= 1);
    assert!(!status.closed);

    application
        .close()
        .await
        .expect("first container close must drain PostgreSQL");
    application
        .close()
        .await
        .expect("second container close must be a no-op");
    assert!(database.status().expect("status remains observable").closed);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a PostgreSQL endpoint and deliberately untrusted CA"]
async fn wrong_additional_ca_is_rejected_before_service_publication() {
    let _environment = ENVIRONMENT.lock().await;
    let path = required_path("LILY_PG_WRONG_CA_CONFIG");
    let _bootstrap = BootstrapEnvironment::install(&path);

    assert!(
        ApplicationContainer::build().await.is_err(),
        "an untrusted PostgreSQL certificate must fail closed"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a PostgreSQL TLS certificate with a mismatched hostname"]
async fn wrong_tls_hostname_is_rejected_before_service_publication() {
    let _environment = ENVIRONMENT.lock().await;
    let path = required_path("LILY_PG_WRONG_HOST_CONFIG");
    let _bootstrap = BootstrapEnvironment::install(&path);

    assert!(
        ApplicationContainer::build().await.is_err(),
        "a PostgreSQL certificate hostname mismatch must fail closed"
    );
}
