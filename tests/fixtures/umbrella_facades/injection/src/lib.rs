#![allow(dead_code, unused_imports)]
pub use lilyrs::__private::lily_injection as runtime;
pub use lilyrs::injection as provider;
pub use provider::async_trait::async_trait as lifecycle;
#[path = "../../../di_facades/contract.rs"]
mod contract;
#[cfg(test)]
#[path = "../../../di_facades/runtime_tests.rs"]
mod runtime_tests;
