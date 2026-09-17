// =============================================================================
// Multi-Queue Consumer Example - Advanced Queue Handler Demo
// =============================================================================
//
// This example demonstrates:
// - Multiple services with multiple queue handlers
// - Different retry configurations
// - Dead letter queue setup
// - Service dependencies via DI
//
// Setup:
// 1. Start RabbitMQ: docker-compose up -d
// 2. Select examples/lily.toml.multi through LILY_CONFIG_PATH and set
//    LILY_CONFIG_MODE=development
// 3. Run: cargo run -p lily_consumer --example multi_queue_consumer

use async_trait::async_trait;
use lily_consumer::Consumer;
use lily_error::injection::InjectionError;
use lily_injection::Injectable;
use lily_injection::ServiceTrait;
use lily_queue::{queue, queue_service, Json, QueueHandlerError};
use serde::{Deserialize, Serialize};
// =============================================================================
// Message DTOs
// =============================================================================

#[derive(Debug, Serialize, Deserialize)]
pub struct UserCreated {
    pub user_id: String,
    pub email: String,
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UserDeleted {
    pub user_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OrderPlaced {
    pub order_id: String,
    pub user_id: String,
    pub total_amount: f64,
    pub items: Vec<OrderItem>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OrderItem {
    pub product_id: String,
    pub quantity: u32,
    pub price: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OrderCancelled {
    pub order_id: String,
    pub reason: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EmailNotification {
    pub to: String,
    pub subject: String,
    pub body: String,
}

// =============================================================================
// User Service - Handles user-related events
// =============================================================================

#[derive(Injectable, Default)]
#[service(lifetime = "Singleton")]
pub struct UserService {}

#[queue_service]
impl UserService {
    #[queue("user.created", version = 1, content = "json")]
    async fn handle_user_created(
        &self,
        Json(msg): Json<UserCreated>,
    ) -> Result<(), QueueHandlerError> {
        println!("👤 [UserService] User Created:");
        println!("   - ID: {}", msg.user_id);
        println!("   - Email: {}", msg.email);
        println!("   - Name: {}", msg.name);

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        println!("   ✅ Processed\n");
        Ok(())
    }

    #[queue("user.deleted", version = 1, content = "json")]
    async fn handle_user_deleted(
        &self,
        Json(msg): Json<UserDeleted>,
    ) -> Result<(), QueueHandlerError> {
        println!("🗑️  [UserService] User Deleted:");
        println!("   - ID: {}", msg.user_id);

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        println!("   ✅ Processed\n");
        Ok(())
    }
}
#[async_trait]
impl ServiceTrait for UserService {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        Ok(())
    }
    async fn dispose(&self) -> Result<(), InjectionError> {
        Ok(())
    }
}
// =============================================================================
// Order Service - Handles order-related events
// =============================================================================

#[derive(Injectable, Default)]
#[service(lifetime = "Singleton")]
pub struct OrderService {
    // Example: Inject other services
    // #[inject]
    // notification_service: Arc<NotificationService>,
}

#[queue_service]
impl OrderService {
    #[queue("order.placed", version = 1, content = "json")]
    async fn handle_order_placed(
        &self,
        Json(msg): Json<OrderPlaced>,
    ) -> Result<(), QueueHandlerError> {
        println!("🛒 [OrderService] Order Placed:");
        println!("   - Order ID: {}", msg.order_id);
        println!("   - User ID: {}", msg.user_id);
        println!("   - Total: ${:.2}", msg.total_amount);
        println!("   - Items: {}", msg.items.len());

        // Simulate order processing
        for item in &msg.items {
            println!(
                "     • {} x {} @ ${:.2}",
                item.quantity, item.product_id, item.price
            );
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        println!("   ✅ Order processed\n");
        Ok(())
    }

    #[queue("order.cancelled", version = 1, content = "json")]
    async fn handle_order_cancelled(
        &self,
        Json(msg): Json<OrderCancelled>,
    ) -> Result<(), QueueHandlerError> {
        println!("❌ [OrderService] Order Cancelled:");
        println!("   - Order ID: {}", msg.order_id);
        println!("   - Reason: {}", msg.reason);

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        println!("   ✅ Cancellation processed\n");
        Ok(())
    }
}
#[async_trait]
impl ServiceTrait for OrderService {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        Ok(())
    }
    async fn dispose(&self) -> Result<(), InjectionError> {
        Ok(())
    }
}
// =============================================================================
// Notification Service - Handles notification events
// =============================================================================

#[derive(Injectable, Default)]
#[service(lifetime = "Singleton")]
pub struct NotificationService {}

#[queue_service]
impl NotificationService {
    #[queue("notification.email", version = 1, content = "json")]
    async fn handle_email_notification(
        &self,
        Json(msg): Json<EmailNotification>,
    ) -> Result<(), QueueHandlerError> {
        println!("📧 [NotificationService] Email Notification:");
        println!("   - To: {}", msg.to);
        println!("   - Subject: {}", msg.subject);
        println!("   - Body: {}...", &msg.body[..msg.body.len().min(50)]);

        // Simulate email sending
        tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;
        println!("   ✅ Email sent\n");
        Ok(())
    }
}

#[async_trait]
impl ServiceTrait for NotificationService {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        Ok(())
    }
    async fn dispose(&self) -> Result<(), InjectionError> {
        Ok(())
    }
}

// =============================================================================
// Main - Start Consumer
// =============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔══════════════════════════════════════════════════════════╗");
    println!("║      Lily Consumer - Multi-Queue Example                ║");
    println!("╚══════════════════════════════════════════════════════════╝\n");

    println!("📋 Configuration:");
    println!("   - Services: UserService, OrderService, NotificationService");
    println!("   - Total Queues: 5");
    println!("   - Retry Support: Yes");
    println!("   - Concurrent Processing: Yes\n");

    println!("🔧 Queue Handlers:");
    println!("   UserService:");
    println!("     • user.created (retry: 3, concurrency: 2)");
    println!("     • user.deleted (concurrency: 1)");
    println!("   OrderService:");
    println!("     • order.placed (retry: 5, concurrency: 4)");
    println!("     • order.cancelled (retry: 2, concurrency: 2)");
    println!("   NotificationService:");
    println!("     • notification.email (retry: 2, concurrency: 3)\n");

    Consumer::run().await?;
    Ok(())
}
