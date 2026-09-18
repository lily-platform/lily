use lily_injectable_derive::Injectable;
use lily_injection::ServiceTrait;

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
struct SingletonService;
impl ServiceTrait for SingletonService {}

#[derive(Default, Injectable)]
#[service(lifetime = "Scoped")]
struct ScopedService;
impl ServiceTrait for ScopedService {}

#[derive(Default, Injectable)]
#[service(lifetime = "Transient")]
struct TransientService;
impl ServiceTrait for TransientService {}

fn main() {}
