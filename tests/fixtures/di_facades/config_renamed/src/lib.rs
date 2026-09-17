#![allow(dead_code)]

use app_config as configuration;
use app_di as provider;
use provider::async_trait::async_trait as lifecycle;

// Inspect registration without loading application configuration.
#[cfg(test)]
use provider as runtime;

#[path = "../../config_contract.rs"]
mod config_contract;

#[path = "../../contract.rs"]
mod contract;
