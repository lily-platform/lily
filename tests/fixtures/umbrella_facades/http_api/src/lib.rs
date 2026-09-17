#![allow(dead_code, unused_imports)]
pub use lily::__private::lily_injection as runtime;
pub use lily::http_api as provider;
pub use provider::async_trait::async_trait as lifecycle;
#[path = "../../../di_facades/contract.rs"]
mod contract;
include!("../../http_api.rs");
