#[allow(clippy::module_inception)]
mod guard;
pub(crate) mod guard_registry;

pub use guard::{GuardInitError, GuardRejection, GuardTrait};
