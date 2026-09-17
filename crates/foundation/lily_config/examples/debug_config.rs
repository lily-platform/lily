use lily_config::{ConfigOptions, ConfigService};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔧 ConfigService Debug Test");

    // Development is explicit here. Production should use an absolute mounted
    // path and ConfigOptions::production(...).
    let config_service =
        ConfigService::new(ConfigOptions::development("crates/foundation/lily_config/lily.toml"));

    println!("📁 Loading ConfigService...");
    config_service.load().await?;

    println!("🔍 Testing key lookup: postgresql.connection_string");

    // Test the problematic key
    match config_service
        .get::<String>("postgresql.connection_string")
        .await
    {
        Ok(_connection_string) => {
            println!("✅ Found PostgreSQL connection_string (value redacted)");
        }
        Err(e) => {
            println!("❌ Error getting connection_string: {e}");
        }
    }

    // Test other keys
    println!("🔍 Testing key lookup: server.host");
    match config_service.get::<String>("server.host").await {
        Ok(host) => {
            println!("✅ Found server.host: {host}");
        }
        Err(e) => {
            println!("❌ Error getting server.host: {e}");
        }
    }

    println!("🔍 Testing key lookup: server.port");
    match config_service.get::<u16>("server.port").await {
        Ok(port) => {
            println!("✅ Found server.port: {port}");
        }
        Err(e) => {
            println!("❌ Error getting server.port: {e}");
        }
    }

    // Typed configuration is available, but it can contain secrets and must not
    // be printed wholesale.
    let lily_config = config_service.get_lily_config().await;
    println!("📋 LilyConfig structure:");
    println!(
        "  - Server: {}:{}",
        lily_config.server.host, lily_config.server.port
    );
    if let Some(postgresql) = &lily_config.postgresql {
        println!(
            "  - PostgreSQL mode: {:?} (connection string redacted: {})",
            postgresql.mode,
            postgresql.connection_string.is_some()
        );
    } else {
        println!("  - PostgreSQL: None");
    }

    let safe = config_service.redacted_effective_config().await;
    println!("📦 Effective config version: {}", safe.metadata.version);

    Ok(())
}
