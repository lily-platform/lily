Registers a concrete service and generates constructor injection.

The target must be a non-generic named or unit struct that implements
`lily_injection::ServiceTrait + Send + Sync + 'static`. Fields carrying
`#[inject]` must use `Arc<T>` or `Arc<dyn Interface>`.

Supported service options are:

- `lifetime = "Singleton" | "Scoped" | "Transient"`;
- `interface = dyn Trait` for one trait-object resolution route;
- `disabled` (or `enabled = false`) to omit the registration.

Registration is automatic and belongs to the application container. Do not
construct registry metadata manually.

# Lifecycle example

```rust
use lily_injection::{Injectable, InjectionError, ServiceTrait, async_trait::async_trait};

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
pub struct UserService;

#[async_trait]
impl ServiceTrait for UserService {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        Ok(())
    }
}
```
