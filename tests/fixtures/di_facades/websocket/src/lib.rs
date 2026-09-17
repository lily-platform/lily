#![allow(dead_code)]

use lily_websocket as provider;
use provider::async_trait as lifecycle;

// Tests observe the same registry the real runtime reads, without starting
// facade-specific services that require external databases or brokers.
#[cfg(test)]
use provider::__private::lily_injection as runtime;

#[path = "../../contract.rs"]
mod contract;
