use lily_injectable_derive::Injectable;
use lily_injection::ServiceTrait;

trait LocalPaymentService {}

#[derive(Injectable)]
#[service(interface = dyn LocalPaymentService)]
struct LocalPaymentServiceImpl;

impl LocalPaymentService for LocalPaymentServiceImpl {}
impl ServiceTrait for LocalPaymentServiceImpl {}

fn main() {}
