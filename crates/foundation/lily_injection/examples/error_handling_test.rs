//! Standalone lifecycle error-handling example.

extern crate linkme;

use async_trait::async_trait;
use lily_error::injection::InjectionError;
use lily_injectable_derive::Injectable;
use lily_injection::{ApplicationContainer, ServiceTrait};

/// Test service that demonstrates error handling in initialize/dispose
#[derive(Injectable)]
#[service(lifetime = "Singleton")]
pub struct TestService {
    pub name: String,
    pub initialized: bool,
}

impl Default for TestService {
    fn default() -> Self {
        Self {
            name: "TestService".to_string(),
            initialized: false,
        }
    }
}

#[async_trait]
impl ServiceTrait for TestService {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        println!("🔧 Initializing TestService...");
        self.initialized = true;
        self.name = "Initialized TestService".to_string();
        println!("✅ TestService initialized successfully!");
        Ok(())
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        println!("🧹 Disposing TestService...");
        println!("✅ TestService disposed successfully!");
        Ok(())
    }
}

/// Test service that fails during initialization
#[derive(Injectable)]
#[service(lifetime = "Transient")]
pub struct FailingService {
    pub name: String,
}

impl Default for FailingService {
    fn default() -> Self {
        Self {
            name: "FailingService".to_string(),
        }
    }
}

#[async_trait]
impl ServiceTrait for FailingService {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        println!("🔧 Attempting to initialize FailingService...");
        // Simulate initialization failure
        Err(InjectionError::ServiceNotFound(
            "Simulated initialization failure".to_string(),
        ))
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        println!("🧹 Disposing FailingService...");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 ServiceTrait Error Handling Test");
    println!("=====================================");

    // Build the application-owned, validated composition root.
    let container = ApplicationContainer::build().await?;
    println!("✅ ApplicationContainer built from auto-discovered services");

    // Test successful service
    println!("\n📋 Testing successful service...");
    match container.resolve::<TestService>(None).await {
        Ok(service) => {
            println!("✅ TestService resolved successfully!");
            println!("   Name: {}", service.name);
            println!("   Initialized: {}", service.initialized);
        }
        Err(e) => {
            println!("❌ Failed to resolve TestService: {e}");
        }
    }

    // Test failing service
    println!("\n📋 Testing failing service...");
    match container.resolve::<FailingService>(None).await {
        Ok(service) => {
            println!("✅ FailingService resolved successfully!");
            println!("   Name: {}", service.name);
        }
        Err(e) => {
            println!("❌ Failed to resolve FailingService (expected): {e}");
        }
    }

    container.close().await?;
    println!("\n🎉 Error handling test completed!");
    Ok(())
}
