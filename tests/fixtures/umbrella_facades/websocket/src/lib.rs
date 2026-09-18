#![allow(dead_code, unused_imports)]
pub use lilyrs::__private::lily_injection as runtime;
pub use lilyrs::websocket as provider;
pub use provider::async_trait as lifecycle;
#[path = "../../../di_facades/contract.rs"]
mod contract;
include!("../../websocket.rs");
