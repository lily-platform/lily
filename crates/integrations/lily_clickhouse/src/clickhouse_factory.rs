//! Immutable, application-owned multi-cell ClickHouse composition root.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use lily_config::{ClickhouseCellConfig, ConfigService, LilyConfig};
use lily_error::injection::InjectionError;
use lily_injectable_derive::Injectable;
use lily_injection::ServiceTrait;

use crate::{ClickhouseClientPlan, DatabaseService};

#[derive(Injectable, Default)]
#[service(lifetime = "Singleton")]
/// DI-owned registry of named ClickHouse database services.
///
/// Select this crate's `factory` feature, inject the factory and call
/// [`Self::get`] with a configured `[[clickhouse.cells]]` name.
pub struct ClickhouseFactory {
    cells: RwLock<HashMap<String, Arc<DatabaseService>>>,
    #[inject]
    config_service: Arc<ConfigService>,
}

impl ClickhouseFactory {
    /// Returns a cloned handle to a named ready cell, or `None` if absent.
    pub fn get(&self, name: &str) -> Option<Arc<DatabaseService>> {
        self.cells.read().ok()?.get(name).cloned()
    }

    /// Idempotently closes every client cell owned by this factory.
    ///
    /// Lily DI invokes this automatically during container shutdown.
    pub fn close(&self) -> Result<(), crate::ClickhouseError> {
        let cells = {
            let mut cells = self.cells.write().map_err(|_| {
                crate::ClickhouseError::InternalError("ClickhouseFactory cell lock poisoned".into())
            })?;
            std::mem::take(&mut *cells)
        };
        for service in cells.values() {
            service.close()?;
        }
        Ok(())
    }

    fn configured_cells(config: &LilyConfig) -> Result<&[ClickhouseCellConfig], InjectionError> {
        let clickhouse = config.clickhouse.as_ref();
        let mode = clickhouse
            .and_then(|clickhouse| clickhouse.mode.as_deref())
            .unwrap_or("single");
        if mode != "factory" {
            return Err(InjectionError::InitError(
                "ClickhouseFactory requires clickhouse.mode = \"factory\"".into(),
            ));
        }

        let cells = clickhouse
            .and_then(|clickhouse| clickhouse.cells.as_deref())
            .filter(|cells| !cells.is_empty())
            .ok_or_else(|| {
                InjectionError::InitError(
                    "No ClickHouse cells configured; add [[clickhouse.cells]] entries".into(),
                )
            })?;

        let mut names = HashSet::with_capacity(cells.len());
        for cell in cells {
            if cell.name.trim().is_empty() || !names.insert(cell.name.as_str()) {
                return Err(InjectionError::InitError(
                    "ClickHouse cell names must be non-empty and unique".into(),
                ));
            }
        }
        Ok(cells)
    }
}

#[async_trait]
impl ServiceTrait for ClickhouseFactory {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        let config = self.config_service.get_lily_config().await;
        let configured_cells = Self::configured_cells(&config)?;

        // Validate the complete immutable snapshot before opening any socket.
        let plans = configured_cells
            .iter()
            .map(ClickhouseClientPlan::from_cell)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| InjectionError::InitError(error.to_string()))?;

        let mut cells = HashMap::with_capacity(configured_cells.len());
        for (cell, plan) in configured_cells.iter().zip(plans) {
            let service = DatabaseService::connect(plan).await.map_err(|error| {
                tracing::error!(cell = %cell.name, error = %error, "ClickHouse cell readiness failed");
                InjectionError::InitError(format!(
                    "ClickHouse cell '{}' readiness probe failed",
                    cell.name
                ))
            })?;
            cells.insert(cell.name.clone(), Arc::new(service));
        }

        // Publish only after every configured cell is ready.
        self.cells = RwLock::new(cells);
        tracing::info!(
            cell_count = configured_cells.len(),
            "ClickhouseFactory initialized"
        );
        Ok(())
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        self.close()
            .map_err(|error| InjectionError::General(error.to_string()))?;
        tracing::info!("ClickhouseFactory disposed");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lily_config::ClickhouseConfig;

    fn clickhouse_cell(name: &str) -> ClickhouseCellConfig {
        ClickhouseCellConfig {
            name: name.to_owned(),
            host: "typed-clickhouse.internal".into(),
            port: None,
            database: "typed_database".into(),
            username: Some("typed-user".into()),
            password: Some("typed-password".into()),
            pool_size: Some(11),
            connection_timeout_secs: Some(4),
            query_timeout_secs: Some(8),
            use_tls: Some(true),
            compression_enabled: Some(true),
        }
    }

    fn config_with(mode: &str, cells: Option<Vec<ClickhouseCellConfig>>) -> LilyConfig {
        let mut config = LilyConfig::default();
        let clickhouse = ClickhouseConfig {
            mode: Some(mode.into()),
            cells,
            ..ClickhouseConfig::default()
        };
        config.clickhouse = Some(clickhouse);
        config
    }

    #[test]
    fn typed_snapshot_cells_are_immutable_and_complete() {
        let config = config_with("factory", Some(vec![clickhouse_cell("analytics")]));
        let cells = ClickhouseFactory::configured_cells(&config).unwrap();
        assert_eq!(cells.len(), 1);
        assert_eq!(cells[0].name, "analytics");
        assert!(ClickhouseClientPlan::from_cell(&cells[0]).is_ok());
    }

    #[test]
    fn wrong_mode_empty_and_duplicate_cells_are_rejected() {
        assert!(ClickhouseFactory::configured_cells(&config_with("single", None)).is_err());
        assert!(
            ClickhouseFactory::configured_cells(&config_with("factory", Some(Vec::new()))).is_err()
        );
        let duplicate = vec![clickhouse_cell("same"), clickhouse_cell("same")];
        assert!(
            ClickhouseFactory::configured_cells(&config_with("factory", Some(duplicate))).is_err()
        );
    }
}
