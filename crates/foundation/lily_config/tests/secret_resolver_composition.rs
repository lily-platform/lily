use async_trait::async_trait;
use lily_config::{ConfigError, ConfigOptions, ConfigService, SecretResolver};
use lily_injection::ApplicationContainer;
use std::path::PathBuf;

struct StaticSecretResolver {
    value: &'static str,
    provider: &'static str,
}

#[async_trait]
impl SecretResolver for StaticSecretResolver {
    async fn resolve(&self, _key: &str) -> Result<String, ConfigError> {
        Ok(self.value.to_string())
    }

    fn provider_name(&self) -> &'static str {
        self.provider
    }
}

fn write_config(label: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "lily-config-pcr09-{label}-{}-{}.toml",
        std::process::id(),
        std::thread::current().name().unwrap_or("test")
    ));
    std::fs::write(
        &path,
        "[server]\nhost = '127.0.0.1'\nport = 8080\n[custom]\ntoken = '${secret:application.token}'\n",
    )
    .unwrap();
    path
}

#[tokio::test]
async fn resolver_is_available_before_config_initialization_and_is_container_scoped() {
    let first_path = write_config("first");
    let second_path = write_config("second");

    let first = ApplicationContainer::builder()
        .seed_singleton(ConfigService::with_options_and_secret_resolver(
            ConfigOptions::test(&first_path),
            StaticSecretResolver {
                value: "first-secret-value",
                provider: "first-provider",
            },
        ))
        .build()
        .await
        .unwrap();
    let second = ApplicationContainer::builder()
        .seed_singleton(ConfigService::with_options_and_secret_resolver(
            ConfigOptions::test(&second_path),
            StaticSecretResolver {
                value: "second-secret-value",
                provider: "second-provider",
            },
        ))
        .build()
        .await
        .unwrap();

    let first_config = first.resolve::<ConfigService>(None).await.unwrap();
    let second_config = second.resolve::<ConfigService>(None).await.unwrap();
    assert_eq!(
        first_config.get::<String>("custom.token").await.unwrap(),
        "first-secret-value"
    );
    assert_eq!(
        second_config.get::<String>("custom.token").await.unwrap(),
        "second-secret-value"
    );

    let first_snapshot = first_config.snapshot().await;
    let second_snapshot = second_config.snapshot().await;
    assert_eq!(
        first_snapshot.metadata().secret_bindings[0].provider,
        "first-provider"
    );
    assert_eq!(
        second_snapshot.metadata().secret_bindings[0].provider,
        "second-provider"
    );
    assert!(!format!("{first_snapshot:?}").contains("first-secret-value"));
    assert!(!format!("{second_snapshot:?}").contains("second-secret-value"));
    assert_eq!(
        first_config.redacted_effective_config().await.values["custom.token"],
        "<redacted>"
    );
    assert_eq!(
        second_config.redacted_effective_config().await.values["custom.token"],
        "<redacted>"
    );

    first.close().await.unwrap();
    second.close().await.unwrap();
    let _ = std::fs::remove_file(first_path);
    let _ = std::fs::remove_file(second_path);
}
