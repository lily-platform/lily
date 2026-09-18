use std::sync::Arc;

use lily_injectable_derive::Injectable;
use lily_injection::ServiceTrait;

trait PaymentService: Send + Sync {
    fn provider(&self) -> &'static str;
}

#[derive(Injectable)]
#[service(interface = dyn PaymentService)]
struct StripePaymentService;

impl PaymentService for StripePaymentService {
    fn provider(&self) -> &'static str {
        "stripe"
    }
}

impl ServiceTrait for StripePaymentService {}

// This dependency-only consumer intentionally does not implement or derive
// `Default`. Its injected `Arc<dyn PaymentService>` is constructed by the
// application container; there is no dummy trait-object default value.
#[derive(Injectable)]
struct CheckoutService {
    #[inject]
    payment_service: Arc<dyn PaymentService>,
}

impl ServiceTrait for CheckoutService {}

fn main() {
    let _ = StripePaymentService.provider();
}
