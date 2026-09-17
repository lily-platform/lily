use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use crate::{
    lifecycle,
    provider::{Injectable, InjectionError, ServiceTrait},
};

pub static CLOCK_STARTS: AtomicUsize = AtomicUsize::new(0);
pub static CLOCK_STOPS: AtomicUsize = AtomicUsize::new(0);
pub static SCOPED_STARTS: AtomicUsize = AtomicUsize::new(0);
pub static SCOPED_STOPS: AtomicUsize = AtomicUsize::new(0);
pub static TRANSIENT_STOPS: AtomicUsize = AtomicUsize::new(0);

pub trait ClockApi: Send + Sync {
    fn name(&self) -> &'static str;
}

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton", interface = dyn ClockApi)]
pub struct Clock;

impl ClockApi for Clock {
    fn name(&self) -> &'static str {
        "clock"
    }
}

#[lifecycle]
impl ServiceTrait for Clock {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        CLOCK_STARTS.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        CLOCK_STOPS.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

// Nesting checks expansion hygiene; trait-object injection requires no Default.
pub mod nested {
    use super::*;

    #[derive(Injectable)]
    #[service(lifetime = "Scoped")]
    pub struct RequestService {
        #[inject]
        pub clock: Arc<dyn ClockApi>,
    }

    #[lifecycle]
    impl ServiceTrait for RequestService {
        async fn initialize(&mut self) -> Result<(), InjectionError> {
            assert_eq!(self.clock.name(), "clock");
            SCOPED_STARTS.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn dispose(&self) -> Result<(), InjectionError> {
            // Dependencies must remain usable until their dependents dispose.
            assert_eq!(CLOCK_STOPS.load(Ordering::SeqCst), 0);
            SCOPED_STOPS.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
}

// Missing lifetime means Transient. Mixed state retains its Default value.
#[derive(Injectable)]
pub struct TransientService {
    #[inject]
    pub clock: Arc<Clock>,
    pub value: usize,
}

impl Default for TransientService {
    fn default() -> Self {
        Self {
            clock: Arc::new(Clock),
            value: 17,
        }
    }
}

#[lifecycle]
impl ServiceTrait for TransientService {
    async fn dispose(&self) -> Result<(), InjectionError> {
        assert_eq!(CLOCK_STOPS.load(Ordering::SeqCst), 0);
        TRANSIENT_STOPS.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Injectable)]
#[service(disabled)]
pub struct Disabled;
impl ServiceTrait for Disabled {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{provider::ServiceLifetime, runtime::__private::registry};
    use std::any::TypeId;

    #[test]
    fn generated_registrations_reach_the_runtime_registry() {
        let metadata = registry::get_all_service_metadata();
        let routes = registry::get_all_service_route_metadata();
        for (id, lifetime, dependencies) in [
            (TypeId::of::<Clock>(), ServiceLifetime::Singleton, vec![]),
            (
                TypeId::of::<nested::RequestService>(),
                ServiceLifetime::Scoped,
                vec![TypeId::of::<dyn ClockApi>()],
            ),
            (
                TypeId::of::<TransientService>(),
                ServiceLifetime::Transient,
                vec![TypeId::of::<Clock>()],
            ),
        ] {
            let entries: Vec<_> = metadata
                .iter()
                .filter(|entry| entry.type_id == id)
                .collect();
            assert_eq!(entries.len(), 1, "exactly one factory per concrete service");
            assert_eq!(entries[0].lifetime, lifetime);
            assert_eq!(entries[0].dependencies, dependencies);
            assert!(registry::get_service_disposer(id).is_some());
            let concrete: Vec<_> = routes
                .iter()
                .filter(|entry| entry.requested_type_id == id)
                .collect();
            assert_eq!(concrete.len(), 1);
            assert_eq!(concrete[0].implementation_type_id, id);
            assert_eq!(concrete[0].kind, registry::ServiceRouteKind::Concrete);
        }
        let interface: Vec<_> = routes
            .iter()
            .filter(|entry| entry.requested_type_id == TypeId::of::<dyn ClockApi>())
            .collect();
        assert_eq!(interface.len(), 1);
        assert_eq!(interface[0].implementation_type_id, TypeId::of::<Clock>());
        assert_eq!(interface[0].kind, registry::ServiceRouteKind::Interface);
        assert!(!metadata
            .iter()
            .any(|entry| entry.type_id == TypeId::of::<Disabled>()));
        assert!(!routes
            .iter()
            .any(|entry| entry.implementation_type_id == TypeId::of::<Disabled>()));
        assert!(registry::get_service_disposer(TypeId::of::<Disabled>()).is_none());
    }
}
