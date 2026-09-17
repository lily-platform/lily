use lily_injectable_derive::Injectable;
use lily_injection::ServiceTrait;

#[derive(Injectable)]
#[service(disabled = true)]
struct DisabledService;

impl ServiceTrait for DisabledService {}

fn main() {}
