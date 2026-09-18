#![allow(dead_code, unused_imports)]
pub use lilyrs::__private::lily_injection as runtime;
pub use lilyrs::http_api as provider;
pub use provider::async_trait::async_trait as lifecycle;
#[path = "../../../di_facades/contract.rs"]
mod contract;
include!("../../http_api.rs");
