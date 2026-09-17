// =============================================================================
// WebSocket Client Factory Example - lily_websocket_client
// =============================================================================
//
// This example demonstrates how to use WebSocketClientFactory with DI
// to manage multiple WebSocket client connections.

use std::sync::Arc;

use lily_injection::ApplicationContainer;
use lily_websocket_client::WebSocketClientFactory;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct StatusUpdate {
    pub service: String,
    pub status: String,
    pub timestamp: i64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔══════════════════════════════════════════════════════════╗");
    println!("║    Lily WebSocket Client - Factory Pattern Example     ║");
    println!("╚══════════════════════════════════════════════════════════╝\n");

    // Initialize DI container and get WebSocket client factory
    let container = ApplicationContainer::build().await?;

    let ws_factory: Arc<WebSocketClientFactory> =
        container.resolve::<WebSocketClientFactory>(None).await?;

    println!("✅ WebSocket client factory initialized\n");

    // Get main WebSocket client
    let main_client = ws_factory.get("main").ok_or("Main client not found")?;
    println!("✅ Retrieved 'main' WebSocket client");

    // Get backup WebSocket client
    let backup_client = ws_factory.get("backup").ok_or("Backup client not found")?;
    println!("✅ Retrieved 'backup' WebSocket client\n");

    // Register event listeners for main client
    main_client.on("status:status", |event, data| {
        println!(
            "  [MAIN] 📨 Received event '{}': {} bytes",
            event,
            data.len()
        );
    })?;

    // Register event listeners for backup client
    backup_client.on("status:status", |event, data| {
        println!(
            "  [BACKUP] 📨 Received event '{}': {} bytes",
            event,
            data.len()
        );
    })?;

    println!("✅ Event listeners registered for both clients\n");

    // Send messages through main client
    if main_client.is_connected()? {
        println!("📤 Sending messages through MAIN client...\n");

        for i in 1..=3 {
            let msg = StatusUpdate {
                service: "main-service".to_string(),
                status: format!("Active - Update {}", i),
                timestamp: unix_timestamp_millis(),
            };

            println!("  [MAIN] 📨 Sending update {}/3", i);

            main_client.send("status:status_update", &msg).await?;

            println!("  [MAIN] ✅ Update {} sent\n", i);

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        }
    } else {
        eprintln!("❌ Main client not connected");
    }

    // Send messages through backup client
    if backup_client.is_connected()? {
        println!("📤 Sending messages through BACKUP client...\n");

        for i in 1..=3 {
            let msg = StatusUpdate {
                service: "backup-service".to_string(),
                status: format!("Standby - Update {}", i),
                timestamp: unix_timestamp_millis(),
            };

            println!("  [BACKUP] 📨 Sending update {}/3", i);

            backup_client.send("status:status_update", &msg).await?;

            println!("  [BACKUP] ✅ Update {} sent\n", i);

            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        }
    } else {
        eprintln!("❌ Backup client not connected");
    }

    println!("🎉 All messages sent successfully through both clients!");

    // Keep connections alive for a bit
    println!("\n⏳ Waiting for responses (3 seconds)...\n");
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    println!("\n💡 Tip: Configure multiple cells in lily.toml:");
    println!("   [websocket_client]");
    println!("   mode = \"factory\"");
    println!("   ");
    println!("   [[websocket_client.cells]]");
    println!("   name = \"main\"");
    println!("   url = \"ws://localhost:8080/ws\"");
    println!("   namespace = \"status\"");
    println!("   ");
    println!("   [[websocket_client.cells]]");
    println!("   name = \"backup\"");
    println!("   url = \"ws://localhost:8081/ws\"");
    println!("   namespace = \"status\"");

    container.close().await?;
    println!("\n🛑 Factory stopped gracefully");
    Ok(())
}

fn unix_timestamp_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}
