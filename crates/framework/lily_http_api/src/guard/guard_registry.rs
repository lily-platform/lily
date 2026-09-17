use crate::guard::guard::{GuardInitError, GuardTrait};
use lily_injection::Extensions;
use once_cell::sync::Lazy;
use std::any::TypeId;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;

/// Hidden generated-code metadata for one guard type.
#[doc(hidden)]
#[derive(Clone)]
pub struct GuardMetadata {
    /// Concrete guard type identifier.
    pub type_id: TypeId,
    /// Concrete guard type name used in bounded startup diagnostics.
    pub type_name: &'static str,
    /// App-scoped constructor emitted by the controller macro.
    #[allow(clippy::type_complexity)]
    pub factory_fn: fn(
        Arc<Extensions>,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Box<dyn GuardTrait + Send + Sync>, GuardInitError>>
                + Send
                + 'static,
        >,
    >,
}

impl std::fmt::Debug for GuardMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GuardMetadata")
            .field("type_name", &self.type_name)
            .field("factory_fn", &"<async function>")
            .finish()
    }
}

/// Global guard metadata registry
static GUARD_REGISTRY: Lazy<Mutex<Vec<GuardMetadata>>> = Lazy::new(|| Mutex::new(Vec::new()));

/// Register a guard metadata (called by guard macro)
/// Prevents duplicate registration by checking type_id
#[doc(hidden)]
pub fn register_guard_metadata(metadata: GuardMetadata) {
    if let Ok(mut registry) = GUARD_REGISTRY.lock() {
        // Check if guard with same type_id already exists
        if registry.iter().any(|g| g.type_id == metadata.type_id) {
            // Guard already registered, skip duplicate
            return;
        }

        registry.push(metadata.clone());
    }
}

/// Get all registered guards
pub(crate) fn get_all_guards() -> Vec<GuardMetadata> {
    GUARD_REGISTRY
        .lock()
        .map(|registry| registry.clone())
        .unwrap_or_default()
}
