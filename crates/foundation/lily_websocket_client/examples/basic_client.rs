// =============================================================================
// Basic WebSocket Client Example - lily_websocket_client
// =============================================================================
//
// This example demonstrates how to use lily_websocket_client with DI
// to connect to a WebSocket server and send/receive messages.

use std::sync::Arc;

use lily_injection::ApplicationContainer;
use lily_websocket_client::WebSocketClientService;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct ChatMessage {
    pub user: String,
    pub message: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔══════════════════════════════════════════════════════════╗");
    println!("║       Lily WebSocket Client - Basic Example            ║");
    println!("╚══════════════════════════════════════════════════════════╝\n");

    // Initialize DI container and get WebSocket client service
    let container = ApplicationContainer::build().await?;

    let ws_client_service: Arc<WebSocketClientService> =
        container.resolve::<WebSocketClientService>(None).await?;

    println!("✅ WebSocket client service initialized\n");

    // Register event listeners
    ws_client_service.on("chat:message", |event, data| {
        println!("  📨 Received event '{}': {} bytes", event, data.len());
    })?;

    // Initialization already emitted the first on_connect event. This handler
    // observes later reconnects.
    ws_client_service.on("on_connect", |_, _| {
        println!("  🔗 Connected to server");
    })?;

    ws_client_service.on("on_disconnect", |_, _| {
        println!("  ❌ Disconnected from server");
    })?;

    println!("✅ Event listeners registered\n");

    // Check connection status
    if ws_client_service.is_connected()? {
        println!("✅ Connected to WebSocket server\n");

        // Send some test messages
        println!("📤 Sending messages...\n");

        for i in 1..=5 {
            let msg = ChatMessage {
                user: format!("User_{}", i),
                message: format!("Hello from message {}", i),
            };

            println!("  Sending message {}/5: {}", i, msg.message);

            ws_client_service.send("chat:chat_message", &msg).await?;

            println!("  ✅ Message {} sent\n", i);

            // Small delay between messages
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        }

        println!("🎉 All messages sent successfully!");

        // Keep connection alive for a bit to receive responses
        println!("\n⏳ Waiting for responses (5 seconds)...\n");
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    } else {
        eprintln!("❌ Not connected to WebSocket server");
        eprintln!("\n💡 Tip: Make sure lily.toml has correct websocket_client configuration");
        eprintln!("💡 Example configuration:");
        eprintln!("   [websocket_client]");
        eprintln!("   mode = \"single\"");
        eprintln!("   url = \"ws://localhost:8080/ws\"");
        eprintln!("   namespace = \"chat\"");
    }

    container.close().await?;
    println!("\nClient stopped gracefully");
    Ok(())
}
