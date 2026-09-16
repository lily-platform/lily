use lily_injectable_derive::Injectable;
use lily_injection::ServiceTrait;

trait GenericPaymentService: Send + Sync {
    fn charge<T>(&self, command: T);
}

#[derive(Injectable)]
#[service(interface = dyn GenericPaymentService)]
struct GenericPaymentServiceImpl;

impl GenericPaymentService for GenericPaymentServiceImpl {
    fn charge<T>(&self, _command: T) {}
}

impl ServiceTrait for GenericPaymentServiceImpl {}

fn main() {}
