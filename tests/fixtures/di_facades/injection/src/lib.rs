#![allow(dead_code)]

use lily_injection as provider;
use provider::async_trait::async_trait as lifecycle;

// Tests observe the same registry the real runtime reads, without starting
// facade-specific services that require external databases or brokers.
#[cfg(test)]
use provider as runtime;

#[path = "../../contract.rs"]
mod contract;

#[cfg(test)]
#[path = "../../runtime_tests.rs"]
mod runtime_tests;
