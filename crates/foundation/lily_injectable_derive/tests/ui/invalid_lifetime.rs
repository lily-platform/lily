use lily_injectable_derive::Injectable;
use lily_injection::ServiceTrait;

#[derive(Default, Injectable)]
#[service(lifetime = "PerThread")]
struct InvalidLifetime;

impl ServiceTrait for InvalidLifetime {}

fn main() {}
