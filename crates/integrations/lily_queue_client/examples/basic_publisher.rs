// =============================================================================
// Basic Publisher Example - lily_queue_client
// =============================================================================
//
// This example demonstrates how to use lily_queue_client to publish messages
// to RabbitMQ with automatic connection management and publisher confirms.

use std::sync::Arc;

use lily_injection::ApplicationContainer;
use lily_queue_client::QueueClientService;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct UserCreated {
    pub user_id: String,
    pub email: String,
    pub name: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔══════════════════════════════════════════════════════════╗");
    println!("║         Lily Queue Client - Basic Publisher             ║");
    println!("╚══════════════════════════════════════════════════════════╝\n");

    let container = ApplicationContainer::build().await?;
    let queue_client_service: Arc<QueueClientService> =
        container.resolve::<QueueClientService>(None).await?;
    println!("✅ Client started successfully\n");

    // Publish some test messages
    println!("📤 Publishing messages...\n");

    for i in 1..=5 {
        let event = UserCreated {
            user_id: format!("test_user_{}", i),
            email: format!("testuser{}@example.com", i),
            name: format!("Test User {}", i),
        };

        println!("  📨 Publishing message {}/5: {}", i, event.user_id);

        queue_client_service
            .publish(
                "user-service", // existing exchange selected by the topology plan
                "user.created", // routing key selected by an existing binding
                &event,
            )
            .await?;
        println!("  ✅ Message {} published and confirmed\n", i);

        // Small delay between messages
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }

    println!("🎉 All messages published successfully!");
    container.close().await?;
    println!("🛑 Client stopped gracefully");

    Ok(())
}
