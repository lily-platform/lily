use lily_injection::ApplicationContainer;

trait PaymentService: Send + Sync {}

struct StripePaymentService;

impl PaymentService for StripePaymentService {}

fn main() {
    ApplicationContainer::add_service::<dyn PaymentService, StripePaymentService>();
}
