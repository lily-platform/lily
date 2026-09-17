use lily_injectable_derive::Injectable;
use lily_injection::ServiceTrait;

trait PaymentService: Send + Sync {}

#[derive(Default, Injectable)]
#[service(interface = dyn PaymentService)]
struct StripePaymentService;

impl PaymentService for StripePaymentService {}
impl ServiceTrait for StripePaymentService {}

fn main() {}
