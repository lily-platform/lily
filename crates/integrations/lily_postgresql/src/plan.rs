use std::path::Path;
use std::time::Duration;

#[cfg(feature = "factory")]
use lily_config::PgCellConfig;
use lily_config::{PgConfig, PgMode, PgPoolConfig, PgTlsConfig, PgTlsMode};

use crate::{PgError, PgResult};

/// Fully validated, immutable connection input. It is deliberately private so
/// callers cannot bypass ConfigService or mutate a live database service.
#[derive(Clone)]
pub(crate) struct PgConnectionPlan {
    connection_string: String,
    pool: PgPoolConfig,
    tls: PgTlsConfig,
}

impl std::fmt::Debug for PgConnectionPlan {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PgConnectionPlan")
            .field("connection_string", &"<redacted>")
            .field("pool", &self.pool)
            .field("tls", &self.tls)
            .finish()
    }
}

impl PgConnectionPlan {
    #[cfg(feature = "single")]
    pub(crate) fn from_single(config: &PgConfig) -> PgResult<Self> {
        if config.mode != PgMode::Single {
            return Err(PgError::FeatureModeMismatch {
                compiled: "single",
                configured: "factory",
            });
        }
        if !config.cells.is_empty() {
            return Err(PgError::InvalidConfiguration {
                code: "single_cells_forbidden",
            });
        }
        let connection_string =
            config
                .connection_string
                .as_deref()
                .ok_or(PgError::InvalidConfiguration {
                    code: "single_connection_required",
                })?;
        Self::new(connection_string, &config.pool, &config.tls)
    }

    #[cfg(feature = "factory")]
    pub(crate) fn from_cell(cell: &PgCellConfig) -> PgResult<Self> {
        Self::new(&cell.connection_string, &cell.pool, &cell.tls)
    }

    fn new(connection_string: &str, pool: &PgPoolConfig, tls: &PgTlsConfig) -> PgResult<Self> {
        if connection_string.trim().is_empty() {
            return Err(PgError::InvalidConfiguration {
                code: "connection_string_required",
            });
        }
        connection_string
            .parse::<tokio_postgres::Config>()
            .map_err(|_| PgError::InvalidConfiguration {
                code: "connection_string_invalid",
            })?;
        validate_pool(pool)?;
        validate_tls(tls)?;
        Ok(Self {
            connection_string: connection_string.to_owned(),
            pool: *pool,
            tls: tls.clone(),
        })
    }

    pub(crate) fn connection_string(&self) -> &str {
        &self.connection_string
    }

    pub(crate) fn max_size(&self) -> usize {
        self.pool.max_size
    }

    pub(crate) fn connect_timeout(&self) -> Duration {
        Duration::from_secs(self.pool.connect_timeout_secs)
    }

    pub(crate) fn acquire_timeout(&self) -> Duration {
        Duration::from_secs(self.pool.acquire_timeout_secs)
    }

    pub(crate) fn recycle_timeout(&self) -> Duration {
        Duration::from_secs(self.pool.recycle_timeout_secs)
    }

    pub(crate) fn transaction_cleanup_timeout(&self) -> Duration {
        Duration::from_secs(self.pool.transaction_cleanup_timeout_secs)
    }

    pub(crate) fn shutdown_timeout(&self) -> Duration {
        Duration::from_secs(self.pool.shutdown_timeout_secs)
    }

    pub(crate) fn tls(&self) -> &PgTlsConfig {
        &self.tls
    }
}

#[cfg(feature = "factory")]
pub(crate) fn validate_factory_config(config: &PgConfig) -> PgResult<()> {
    use std::collections::HashSet;

    if config.mode != PgMode::Factory {
        return Err(PgError::FeatureModeMismatch {
            compiled: "factory",
            configured: "single",
        });
    }
    if config.connection_string.is_some() {
        return Err(PgError::InvalidConfiguration {
            code: "factory_connection_forbidden",
        });
    }
    if config.cells.is_empty() {
        return Err(PgError::InvalidConfiguration {
            code: "factory_cells_required",
        });
    }
    let mut names = HashSet::with_capacity(config.cells.len());
    for cell in &config.cells {
        if cell.name.trim().is_empty() {
            return Err(PgError::InvalidConfiguration {
                code: "factory_cell_name_empty",
            });
        }
        if !names.insert(cell.name.as_str()) {
            return Err(PgError::InvalidConfiguration {
                code: "factory_cell_name_duplicate",
            });
        }
        PgConnectionPlan::from_cell(cell)?;
    }
    Ok(())
}

fn validate_pool(pool: &PgPoolConfig) -> PgResult<()> {
    if pool.max_size == 0 {
        return Err(PgError::InvalidConfiguration {
            code: "pool_max_size_zero",
        });
    }
    if pool.connect_timeout_secs == 0
        || pool.acquire_timeout_secs == 0
        || pool.recycle_timeout_secs == 0
        || pool.transaction_cleanup_timeout_secs == 0
        || pool.shutdown_timeout_secs == 0
    {
        return Err(PgError::InvalidConfiguration {
            code: "pool_timeout_zero",
        });
    }
    Ok(())
}

fn validate_tls(tls: &PgTlsConfig) -> PgResult<()> {
    if tls.mode == PgTlsMode::Disable && tls.additional_ca_bundle.is_some() {
        return Err(PgError::InvalidConfiguration {
            code: "ca_requires_tls",
        });
    }
    if let Some(path) = &tls.additional_ca_bundle
        && !Path::new(path).is_absolute()
    {
        return Err(PgError::InvalidConfiguration {
            code: "ca_path_not_absolute",
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "single")]
    #[test]
    fn single_plan_debug_never_contains_credentials() {
        let config = PgConfig {
            connection_string: Some("postgresql://alice:secret@localhost/orders".into()),
            tls: PgTlsConfig {
                mode: PgTlsMode::Disable,
                additional_ca_bundle: None,
            },
            ..PgConfig::default()
        };
        let debug = format!("{:?}", PgConnectionPlan::from_single(&config).unwrap());
        assert!(!debug.contains("alice"));
        assert!(!debug.contains("secret"));
        assert!(debug.contains("<redacted>"));
    }

    #[cfg(feature = "single")]
    #[test]
    fn single_binary_rejects_factory_configuration_before_io() {
        let config = PgConfig {
            mode: PgMode::Factory,
            ..PgConfig::default()
        };
        assert!(matches!(
            PgConnectionPlan::from_single(&config),
            Err(PgError::FeatureModeMismatch {
                compiled: "single",
                configured: "factory",
            })
        ));
    }

    #[cfg(feature = "factory")]
    #[test]
    fn factory_binary_rejects_single_configuration_before_io() {
        assert_eq!(
            validate_factory_config(&PgConfig::default()),
            Err(PgError::FeatureModeMismatch {
                compiled: "factory",
                configured: "single",
            })
        );
    }

    #[cfg(feature = "factory")]
    #[test]
    fn factory_rejects_duplicate_cell_names_before_io() {
        let cell = PgCellConfig {
            name: "orders".into(),
            connection_string: "postgresql://localhost/orders".into(),
            pool: PgPoolConfig::default(),
            tls: PgTlsConfig {
                mode: PgTlsMode::Disable,
                additional_ca_bundle: None,
            },
        };
        let config = PgConfig {
            mode: PgMode::Factory,
            cells: vec![cell.clone(), cell],
            ..PgConfig::default()
        };
        assert_eq!(
            validate_factory_config(&config),
            Err(PgError::InvalidConfiguration {
                code: "factory_cell_name_duplicate",
            })
        );
    }
}
