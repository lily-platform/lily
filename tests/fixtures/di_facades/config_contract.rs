use std::sync::Arc;

use crate::{
    configuration::ConfigService,
    provider::{Injectable, ServiceTrait},
};

#[derive(Injectable)]
#[service(lifetime = "Scoped")]
struct ConfiguredService {
    #[inject]
    configuration: Arc<ConfigService>,
}

impl ServiceTrait for ConfiguredService {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::{__private::registry, ServiceLifetime};
    use std::any::TypeId;

    #[test]
    fn configuration_and_application_service_share_the_di_registry() {
        let registrations = registry::get_all_service_metadata();
        for (id, lifetime, dependencies) in [
            (
                TypeId::of::<ConfiguredService>(),
                ServiceLifetime::Scoped,
                vec![TypeId::of::<ConfigService>()],
            ),
            (
                TypeId::of::<ConfigService>(),
                ServiceLifetime::Singleton,
                vec![],
            ),
        ] {
            let entries: Vec<_> = registrations
                .iter()
                .filter(|entry| entry.type_id == id)
                .collect();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].lifetime, lifetime);
            assert_eq!(entries[0].dependencies, dependencies);
            assert!(registry::get_service_disposer(id).is_some());
        }
    }
}
