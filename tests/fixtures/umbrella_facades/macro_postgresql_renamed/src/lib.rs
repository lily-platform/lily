#![allow(dead_code, unused_imports)]

pub use platform::postgresql as runtime;
include!("../../../component_facades/postgresql.rs");
