use lily_injectable_derive::Injectable;
use lily_injection::ServiceTrait;

#[derive(Injectable)]
struct Dependency;

impl ServiceTrait for Dependency {}

#[derive(Injectable)]
struct InvalidConsumer {
    #[inject]
    dependency: Dependency,
}

impl ServiceTrait for InvalidConsumer {}

fn main() {}
