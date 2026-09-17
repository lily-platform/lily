#![allow(dead_code, unused_imports)]
pub use lily::__private::lily_injection as runtime;
pub use lily::websocket as provider;
pub use provider::async_trait as lifecycle;
#[path = "../../../di_facades/contract.rs"]
mod contract;
include!("../../websocket.rs");
