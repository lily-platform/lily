#[allow(clippy::module_inception)]
mod registry;

mod openapi;

#[cfg(test)]
mod controller_runtime_tests;

#[doc(hidden)]
pub use openapi::*;
pub use registry::*;
