use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::fmt;

/// Request-scoped typed storage for middleware and handlers.
///
/// Values live only for the lifetime of one [`crate::Request`]. The
/// `Send + Sync` bounds keep a request safe to move through asynchronous
/// middleware without coupling this map to Lily's dependency-injection
/// registry.
#[derive(Default)]
pub struct RequestExtensions {
    values: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}

/// Concise alias for request-scoped typed storage.
pub type RequestLocal = RequestExtensions;

impl RequestExtensions {
    /// Creates an empty request-local map.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts `value`, returning the previous value of the same concrete
    /// type when present.
    pub fn insert<T>(&mut self, value: T) -> Option<T>
    where
        T: Send + Sync + 'static,
    {
        self.values
            .insert(TypeId::of::<T>(), Box::new(value))
            .map(|previous| {
                *previous
                    .downcast::<T>()
                    .expect("request-local TypeId and stored value type must agree")
            })
    }

    /// Returns a shared reference to the value of type `T`.
    #[must_use]
    pub fn get<T>(&self) -> Option<&T>
    where
        T: Send + Sync + 'static,
    {
        self.values
            .get(&TypeId::of::<T>())
            .and_then(|value| value.downcast_ref::<T>())
    }

    /// Returns an exclusive reference to the value of type `T`.
    #[must_use]
    pub fn get_mut<T>(&mut self) -> Option<&mut T>
    where
        T: Send + Sync + 'static,
    {
        self.values
            .get_mut(&TypeId::of::<T>())
            .and_then(|value| value.downcast_mut::<T>())
    }

    /// Removes and returns the value of type `T`.
    pub fn remove<T>(&mut self) -> Option<T>
    where
        T: Send + Sync + 'static,
    {
        self.values.remove(&TypeId::of::<T>()).map(|value| {
            *value
                .downcast::<T>()
                .expect("request-local TypeId and stored value type must agree")
        })
    }

    /// Removes every request-local value.
    pub fn clear(&mut self) {
        self.values.clear();
    }

    /// Returns the number of distinct concrete types in the map.
    #[must_use]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns `true` when no request-local values are stored.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

impl fmt::Debug for RequestExtensions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RequestExtensions")
            .field("entry_count", &self.values.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::RequestExtensions;

    #[derive(Debug, PartialEq, Eq)]
    struct CurrentTenant(String);

    #[derive(Debug, PartialEq, Eq)]
    struct RequestSequence(u64);

    #[test]
    fn typed_values_can_be_replaced_mutated_and_removed() {
        let mut local = RequestExtensions::new();

        assert!(local.insert(CurrentTenant("first".to_string())).is_none());
        local.insert(RequestSequence(7));
        assert_eq!(
            local.get::<CurrentTenant>(),
            Some(&CurrentTenant("first".to_string()))
        );
        assert_eq!(local.get::<RequestSequence>(), Some(&RequestSequence(7)));

        local
            .get_mut::<RequestSequence>()
            .expect("sequence must exist")
            .0 += 1;
        assert_eq!(local.get::<RequestSequence>(), Some(&RequestSequence(8)));

        assert_eq!(
            local.insert(CurrentTenant("second".to_string())),
            Some(CurrentTenant("first".to_string()))
        );
        assert_eq!(
            local.remove::<CurrentTenant>(),
            Some(CurrentTenant("second".to_string()))
        );
        assert!(local.get::<CurrentTenant>().is_none());
        assert_eq!(local.len(), 1);

        local.clear();
        assert!(local.is_empty());
    }

    #[test]
    fn debug_output_does_not_expose_stored_values_or_type_names() {
        let mut local = RequestExtensions::new();
        local.insert(CurrentTenant("tenant-secret".to_string()));

        let output = format!("{local:?}");
        assert_eq!(output, "RequestExtensions { entry_count: 1 }");
        assert!(!output.contains("tenant-secret"));
        assert!(!output.contains("CurrentTenant"));
    }
}
