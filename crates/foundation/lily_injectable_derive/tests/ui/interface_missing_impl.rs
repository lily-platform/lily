use lily_injectable_derive::Injectable;
use lily_injection::ServiceTrait;

trait PaymentService: Send + Sync {}

#[derive(Default, Injectable)]
#[service(interface = dyn PaymentService)]
struct NotAPaymentService;

impl ServiceTrait for NotAPaymentService {}

fn main() {}
