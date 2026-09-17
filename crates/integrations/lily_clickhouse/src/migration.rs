//! Explicit deployment-time ClickHouse migrations.
//!
//! ClickHouse DDL is not transactional. The runner serializes deployers and
//! records content-addressed history, but it cannot promise automatic rollback.
//! A failed step remains fail-closed and requires the documented forward-fix or
//! restore procedure. An abrupt deployer crash can leave the lock table behind;
//! operators must verify the history before manually clearing that stale lock.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::{ClickhouseError, ClickhouseOperationContext, DatabaseService};

const MAX_MIGRATIONS: usize = 1_000;
const MAX_STATEMENTS: usize = 100;
const MAX_STATEMENT_BYTES: usize = 1_048_576;
const LOCK_TABLE: &str = "_lily_migration_lock";
const VERSION_TABLE: &str = "_lily_schema_migrations";

#[derive(Debug, Clone, PartialEq, Eq)]
/// One immutable, positive-version deployment migration.
pub struct ClickhouseMigration {
    version: i64,
    name: String,
    statements: Vec<String>,
}

impl ClickhouseMigration {
    /// Creates and validates one bounded migration definition.
    pub fn new(
        version: i64,
        name: impl Into<String>,
        statements: Vec<String>,
    ) -> Result<Self, ClickhouseError> {
        let migration = Self {
            version,
            name: name.into(),
            statements,
        };
        validate_migration(&migration)?;
        Ok(migration)
    }

    /// Returns the positive migration version.
    pub const fn version(&self) -> i64 {
        self.version
    }

    /// Returns the operator-readable migration name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the ordered SQL statements applied by this migration.
    pub fn statements(&self) -> &[String] {
        &self.statements
    }

    /// Computes the stable content checksum recorded in migration history.
    pub fn checksum(&self) -> String {
        let mut digest = Sha256::new();
        digest.update(self.version.to_be_bytes());
        digest.update([0]);
        digest.update(self.name.as_bytes());
        for statement in &self.statements {
            digest.update([0]);
            digest.update(statement.as_bytes());
        }
        format!("{:x}", digest.finalize())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
/// Summary returned after one migration-plan application.
pub struct ClickhouseMigrationReport {
    /// Number of plan versions already present in migration history.
    pub previously_applied: usize,
    /// Versions newly applied by this invocation, in ascending order.
    pub newly_applied: Vec<i64>,
}

#[derive(Clone)]
/// Explicit deployment-time runner for an ordered migration plan.
pub struct ClickhouseMigrationRunner {
    database: Arc<DatabaseService>,
    migrations: Arc<[ClickhouseMigration]>,
}

impl ClickhouseMigrationRunner {
    /// Validates and freezes an ordered migration plan for one database.
    pub fn new(
        database: Arc<DatabaseService>,
        migrations: Vec<ClickhouseMigration>,
    ) -> Result<Self, ClickhouseError> {
        validate_plan(&migrations)?;
        Ok(Self {
            database,
            migrations: migrations.into(),
        })
    }

    /// Applies migrations from a dedicated deployment process.
    ///
    /// Application startup and health checks must never invoke this method.
    pub async fn apply(
        &self,
        operation: &ClickhouseOperationContext,
    ) -> Result<ClickhouseMigrationReport, ClickhouseError> {
        self.acquire_lock(operation).await?;
        let result = self.apply_locked(operation).await;

        // Cleanup gets a fresh deadline and token. A cancelled request must not
        // prevent release of a successfully acquired deployment lock.
        let release = match self.database.bounded_operation() {
            Ok(cleanup) => self.release_lock(&cleanup).await,
            Err(error) => Err(error),
        };
        match (result, release) {
            (Err(error), _) => Err(error),
            (Ok(report), Ok(())) => Ok(report),
            (Ok(_), Err(error)) => Err(error),
        }
    }

    async fn acquire_lock(
        &self,
        operation: &ClickhouseOperationContext,
    ) -> Result<(), ClickhouseError> {
        let statement = format!(
            "CREATE TABLE `{LOCK_TABLE}` \
             (acquired_at DateTime64(3)) ENGINE = Memory"
        );
        match self.database.execute_migration(&statement, operation).await {
            Ok(()) => Ok(()),
            Err(ClickhouseError::QueryError(message))
                if message.to_ascii_lowercase().contains("already exists") =>
            {
                Err(ClickhouseError::MigrationLockUnavailable)
            }
            Err(error) => Err(error),
        }
    }

    async fn release_lock(
        &self,
        operation: &ClickhouseOperationContext,
    ) -> Result<(), ClickhouseError> {
        self.database
            .execute_migration(&format!("DROP TABLE `{LOCK_TABLE}`"), operation)
            .await
    }

    async fn apply_locked(
        &self,
        operation: &ClickhouseOperationContext,
    ) -> Result<ClickhouseMigrationReport, ClickhouseError> {
        self.database
            .execute_migration(
                &format!(
                    "CREATE TABLE IF NOT EXISTS `{VERSION_TABLE}` (\
                     version Int64, name String, checksum String, applied_at DateTime64(3)\
                     ) ENGINE = MergeTree() ORDER BY version"
                ),
                operation,
            )
            .await?;

        let rows = self
            .database
            .fetch_migration_rows::<MigrationHistoryRow>(
                &format!(
                    "SELECT ?fields FROM `{VERSION_TABLE}` ORDER BY version \
                     LIMIT {}",
                    MAX_MIGRATIONS + 1
                ),
                operation,
            )
            .await?;
        if rows.len() > MAX_MIGRATIONS {
            return Err(invalid("migration history exceeds 1000 versions"));
        }
        let row_count = rows.len();
        let applied = rows
            .into_iter()
            .map(|row| (row.version, row.checksum))
            .collect::<BTreeMap<_, _>>();
        if applied.len() != row_count {
            return Err(invalid("migration history contains duplicate versions"));
        }
        let expected = self
            .migrations
            .iter()
            .map(|migration| (migration.version, migration))
            .collect::<BTreeMap<_, _>>();
        for (version, checksum) in &applied {
            let migration = expected
                .get(version)
                .ok_or(ClickhouseError::MigrationHistoryDiverged(*version))?;
            if migration.checksum() != *checksum {
                return Err(ClickhouseError::MigrationChecksumMismatch(*version));
            }
        }

        let mut report = ClickhouseMigrationReport {
            previously_applied: applied.len(),
            newly_applied: Vec::new(),
        };
        for migration in self.migrations.iter() {
            if applied.contains_key(&migration.version) {
                continue;
            }
            for statement in &migration.statements {
                self.database
                    .execute_migration(statement, operation)
                    .await?;
            }
            self.database
                .record_migration(
                    migration.version,
                    &migration.name,
                    &migration.checksum(),
                    operation,
                )
                .await?;
            report.newly_applied.push(migration.version);
        }
        Ok(report)
    }
}

#[derive(clickhouse::Row, Deserialize)]
struct MigrationHistoryRow {
    version: i64,
    checksum: String,
}

fn validate_plan(migrations: &[ClickhouseMigration]) -> Result<(), ClickhouseError> {
    if migrations.len() > MAX_MIGRATIONS {
        return Err(invalid("migration plan exceeds 1000 versions"));
    }
    let mut previous = 0_i64;
    for migration in migrations {
        validate_migration(migration)?;
        if migration.version <= previous {
            return Err(invalid(
                "migration versions must be unique and strictly increasing",
            ));
        }
        previous = migration.version;
    }
    Ok(())
}

fn validate_migration(migration: &ClickhouseMigration) -> Result<(), ClickhouseError> {
    if migration.version <= 0 {
        return Err(invalid("migration versions must be positive"));
    }
    if migration.name.trim().is_empty() || migration.name.len() > 128 {
        return Err(invalid("migration names must contain 1 to 128 characters"));
    }
    if migration.statements.is_empty() || migration.statements.len() > MAX_STATEMENTS {
        return Err(invalid(
            "each migration must contain between 1 and 100 statements",
        ));
    }
    if migration
        .statements
        .iter()
        .any(|statement| statement.trim().is_empty() || statement.len() > MAX_STATEMENT_BYTES)
    {
        return Err(invalid(
            "migration statements must be non-empty and at most 1 MiB",
        ));
    }
    Ok(())
}

fn invalid(message: &str) -> ClickhouseError {
    ClickhouseError::InvalidMigrationPlan(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plans_are_ordered_bounded_and_content_addressed() {
        let first =
            ClickhouseMigration::new(1, "events", vec!["CREATE TABLE events".into()]).unwrap();
        let changed =
            ClickhouseMigration::new(1, "events", vec!["CREATE TABLE changed_events".into()])
                .unwrap();
        assert_ne!(first.checksum(), changed.checksum());
        assert!(validate_plan(std::slice::from_ref(&first)).is_ok());
        assert!(validate_plan(&[first.clone(), first]).is_err());
        assert!(ClickhouseMigration::new(0, "bad", vec!["SELECT 1".into()]).is_err());
        assert!(ClickhouseMigration::new(1, "", vec!["SELECT 1".into()]).is_err());
        assert!(ClickhouseMigration::new(1, "bad", Vec::new()).is_err());
    }
}
