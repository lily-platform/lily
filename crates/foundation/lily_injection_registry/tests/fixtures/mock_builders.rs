//! Mock builders for creating test doubles
//! Provides builder patterns for creating mock objects

use crate::ServiceRegistrar;
use lily_injection_registry::*;
use std::any::TypeId;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Builder for creating mock ServiceRegistrar implementations
pub struct MockRegistrarBuilder {
    registered_services: HashMap<TypeId, ServiceLifetime>,
    initialization_order: Vec<TypeId>,
    should_fail: bool,
}

impl MockRegistrarBuilder {
    pub fn new() -> Self {
        Self {
            registered_services: HashMap::new(),
            initialization_order: Vec::new(),
            should_fail: false,
        }
    }

    pub fn with_existing_service(mut self, type_id: TypeId, lifetime: ServiceLifetime) -> Self {
        self.registered_services.insert(type_id, lifetime);
        self
    }

    pub fn with_initialization_order(mut self, order: Vec<TypeId>) -> Self {
        self.initialization_order = order;
        self
    }

    pub fn should_fail_registration(mut self, fail: bool) -> Self {
        self.should_fail = fail;
        self
    }

    pub fn build(self) -> MockRegistrar {
        MockRegistrar {
            registered_services: Arc::new(Mutex::new(self.registered_services)),
            initialization_order: Arc::new(Mutex::new(self.initialization_order)),
            should_fail: self.should_fail,
        }
    }
}

pub struct MockRegistrar {
    registered_services: Arc<Mutex<HashMap<TypeId, ServiceLifetime>>>,
    initialization_order: Arc<Mutex<Vec<TypeId>>>,
    should_fail: bool,
}

impl crate::ServiceRegistrar for MockRegistrar {
    fn register_service<F>(&mut self, type_id: TypeId, lifetime: ServiceLifetime, _factory: F)
    where
        F: Fn(
                Arc<dyn std::any::Any + Send + Sync>,
            ) -> std::pin::Pin<
                Box<
                    dyn std::future::Future<
                            Output = Result<
                                Box<dyn std::any::Any + Send + Sync>,
                                lily_error::injection::InjectionError,
                            >,
                        > + Send
                        + 'static,
                >,
            > + Send
            + Sync
            + 'static,
    {
        if !self.should_fail {
            self.registered_services
                .lock()
                .unwrap()
                .insert(type_id, lifetime);
            self.initialization_order.lock().unwrap().push(type_id);
        }
    }
}

/// Builder for creating mock service implementations
pub struct MockServiceBuilder<T: Send + Sync + 'static> {
    value: T,
    initialization_count: Arc<Mutex<i32>>,
    disposal_count: Arc<Mutex<i32>>,
}

impl<T: Send + Sync + 'static> MockServiceBuilder<T> {
    pub fn new(value: T) -> Self {
        Self {
            value,
            initialization_count: Arc::new(Mutex::new(0)),
            disposal_count: Arc::new(Mutex::new(0)),
        }
    }

    pub fn build(self) -> MockService<T> {
        MockService {
            value: self.value,
            initialization_count: self.initialization_count,
            disposal_count: self.disposal_count,
        }
    }
}

pub struct MockService<T: Send + Sync + 'static> {
    value: T,
    initialization_count: Arc<Mutex<i32>>,
    disposal_count: Arc<Mutex<i32>>,
}

impl<T: Send + Sync + 'static> MockService<T> {
    pub fn get_value(&self) -> &T {
        &self.value
    }

    pub fn get_initialization_count(&self) -> i32 {
        *self.initialization_count.lock().unwrap()
    }

    pub fn get_disposal_count(&self) -> i32 {
        *self.disposal_count.lock().unwrap()
    }

    pub fn initialize(&self) {
        *self.initialization_count.lock().unwrap() += 1;
    }

    pub fn dispose(&self) {
        *self.disposal_count.lock().unwrap() += 1;
    }
}

/// Builder for creating mock factory functions
pub struct MockFactoryBuilder {
    success: bool,
    delay_ms: u64,
}

impl MockFactoryBuilder {
    pub fn new() -> Self {
        Self {
            success: true,
            delay_ms: 0,
        }
    }

    pub fn with_success(mut self, success: bool) -> Self {
        self.success = success;
        self
    }

    pub fn with_delay(mut self, delay_ms: u64) -> Self {
        self.delay_ms = delay_ms;
        self
    }

    pub fn build<T: Clone + Send + Sync + 'static>(
        self,
        value: T,
    ) -> impl Fn(
        Arc<dyn std::any::Any + Send + Sync>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<
                        Box<dyn std::any::Any + Send + Sync>,
                        lily_error::injection::InjectionError,
                    >,
                > + Send
                + 'static,
        >,
    > {
        let success = self.success;
        let delay_ms = self.delay_ms;

        move |_| {
            let value = value.clone();
            Box::pin(async move {
                if delay_ms > 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                }

                if success {
                    Ok(Box::new(value) as Box<dyn std::any::Any + Send + Sync>)
                } else {
                    Err(lily_error::injection::InjectionError::ServiceNotFound(
                        "Mock factory error".to_string(),
                    ))
                }
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_registrar_builder() {
        let registrar = MockRegistrarBuilder::new()
            .with_existing_service(TypeId::of::<i32>(), ServiceLifetime::Singleton)
            .with_initialization_order(vec![TypeId::of::<i32>(), TypeId::of::<String>()])
            .should_fail_registration(false)
            .build();

        assert!(registrar
            .registered_services
            .lock()
            .unwrap()
            .contains_key(&TypeId::of::<i32>()));
        assert_eq!(registrar.initialization_order.lock().unwrap().len(), 2);
    }

    #[test]
    fn test_mock_service_builder() {
        let service = MockServiceBuilder::new(42i32).build();

        assert_eq!(*service.get_value(), 42);
        assert_eq!(service.get_initialization_count(), 0);

        service.initialize();
        assert_eq!(service.get_initialization_count(), 1);

        service.dispose();
        assert_eq!(service.get_disposal_count(), 1);
    }

    #[tokio::test]
    async fn test_mock_factory_builder() {
        // Test successful factory
        let success_factory = MockFactoryBuilder::new()
            .with_success(true)
            .with_delay(10)
            .build(42i32);

        let extensions = Arc::new(()) as Arc<dyn std::any::Any + Send + Sync>;
        let result = success_factory(extensions).await;
        assert!(result.is_ok());

        // Test failing factory
        let fail_factory = MockFactoryBuilder::new().with_success(false).build(42i32);

        let extensions = Arc::new(()) as Arc<dyn std::any::Any + Send + Sync>;
        let result = fail_factory(extensions).await;
        assert!(result.is_err());
    }
}
