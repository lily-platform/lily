use std::sync::Arc;

use lily_injection::{ApplicationContainer, Injectable, ServiceTrait};

trait PaymentService: Send + Sync {
    fn provider(&self) -> &'static str;
}

#[derive(Injectable)]
#[service(interface = dyn PaymentService, lifetime = "Singleton")]
struct StripePaymentService;

impl PaymentService for StripePaymentService {
    fn provider(&self) -> &'static str {
        "stripe"
    }
}

impl ServiceTrait for StripePaymentService {}

// There is deliberately no struct-level Default implementation. The macro
// can construct a dependency-only service directly from its resolved trait
// object, without inventing a dummy default value.
#[derive(Injectable)]
#[service(lifetime = "Singleton")]
struct CheckoutService {
    #[inject]
    payment_service: Arc<dyn PaymentService>,
}

impl ServiceTrait for CheckoutService {}

impl CheckoutService {
    fn provider(&self) -> &'static str {
        self.payment_service.provider()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let application = ApplicationContainer::build().await?;

    let concrete = application.resolve::<StripePaymentService>(None).await?;
    let interface = application.resolve::<dyn PaymentService>(None).await?;
    let consumer = application.resolve::<CheckoutService>(None).await?;

    assert_eq!(interface.provider(), "stripe");
    assert_eq!(consumer.provider(), "stripe");
    assert_eq!(
        Arc::as_ptr(&concrete) as *const (),
        Arc::as_ptr(&interface) as *const (),
        "concrete and interface routes must preserve Arc identity",
    );

    application.close().await?;
    Ok(())
}
