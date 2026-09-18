use lily_injectable_derive::Injectable;
use lily_injection::ServiceTrait;

#[derive(Injectable)]
struct RuntimeStateService {
    state: String,
}

impl ServiceTrait for RuntimeStateService {}

fn main() {}
