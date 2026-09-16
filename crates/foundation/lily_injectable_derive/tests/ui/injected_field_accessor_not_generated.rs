extern crate linkme;

use std::sync::Arc;

use lily_injectable_derive::Injectable;
use lily_injection::ServiceTrait;

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
struct Dependency;

impl ServiceTrait for Dependency {}

#[derive(Injectable)]
#[service(lifetime = "Singleton")]
struct Consumer {
    #[inject]
    dependency: Arc<Dependency>,
}

impl ServiceTrait for Consumer {}

fn generated_dependency_accessors_are_not_application_api(service: &Consumer) {
    let _ = service.dependency();
}

fn main() {}
