use lily_injectable_derive::Injectable;
use lily_injection::ServiceTrait;

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
pub struct PublicService;

impl ServiceTrait for PublicService {}

fn main() {}
