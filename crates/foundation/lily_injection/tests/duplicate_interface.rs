use lily_error::injection::InjectionError;
use lily_injectable_derive::Injectable;
use lily_injection::{ApplicationContainer, ServiceTrait};

trait Clock: Send + Sync {}

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton", interface = dyn Clock)]
struct SystemClock;
impl Clock for SystemClock {}
impl ServiceTrait for SystemClock {}

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton", interface = dyn Clock)]
struct TestClock;
impl Clock for TestClock {}
impl ServiceTrait for TestClock {}

#[tokio::test]
async fn duplicate_active_interface_is_a_typed_startup_error() {
    assert!(matches!(
        ApplicationContainer::build().await,
        Err(InjectionError::DuplicateInterfaceBinding {
            interface,
            implementations,
            ..
        }) if interface.contains("Clock")
            && implementations.iter().any(|name| name.contains("SystemClock"))
            && implementations.iter().any(|name| name.contains("TestClock"))
    ));
}
