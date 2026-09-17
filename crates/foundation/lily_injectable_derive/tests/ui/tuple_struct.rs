use lily_injectable_derive::Injectable;
use lily_injection::ServiceTrait;

#[derive(Default, Injectable)]
struct TupleService(String);

impl ServiceTrait for TupleService {}

fn main() {}
