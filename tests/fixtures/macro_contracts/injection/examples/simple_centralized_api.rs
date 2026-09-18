//! Canonical standalone use of Lily dependency injection.

extern crate linkme;

use std::sync::Arc;

use lily_injectable_derive::Injectable;
use lily_injection::{ApplicationContainer, ServiceTrait};

trait ClockApi: Send + Sync {
    fn now(&self) -> &'static str;
}

#[derive(Injectable)]
#[service(interface = dyn ClockApi, lifetime = "Singleton")]
struct SystemClock;

impl ClockApi for SystemClock {
    fn now(&self) -> &'static str {
        "2026-08-26T00:00:00Z"
    }
}

impl ServiceTrait for SystemClock {}

#[derive(Injectable)]
#[service(lifetime = "Singleton")]
struct AuditService {
    #[inject]
    clock: Arc<dyn ClockApi>,
}

impl AuditService {
    fn event_time(&self) -> &'static str {
        self.clock.now()
    }
}

impl ServiceTrait for AuditService {}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Lily HTTP/WebSocket/consumer builders already own this container. Build
    // one explicitly only in a standalone composition root such as this demo.
    let application = ApplicationContainer::build().await?;

    let audit = application.resolve::<AuditService>(None).await?;
    let clock = application.resolve::<dyn ClockApi>(None).await?;

    assert_eq!(audit.event_time(), clock.now());

    application.close().await?;
    Ok(())
}
