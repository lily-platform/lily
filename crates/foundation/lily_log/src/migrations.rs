//! Deployment-owned OpenTelemetry ClickHouse schema set.

use lily_clickhouse::{ClickhouseError, ClickhouseMigration};

use crate::schemas::{
    OtelLogTable, OtelMetricExponentialHistogramTable, OtelMetricGaugeTable,
    OtelMetricHistogramTable, OtelMetricSumTable, OtelMetricSummaryTable, OtelTraceTable,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OtelRetentionPolicy {
    days: u16,
}

impl OtelRetentionPolicy {
    pub fn new(days: u16) -> Result<Self, ClickhouseError> {
        if !(1..=3_650).contains(&days) {
            return Err(ClickhouseError::InvalidMigrationPlan(
                "OTEL retention must be between 1 and 3650 days".into(),
            ));
        }
        Ok(Self { days })
    }

    pub const fn days(self) -> u16 {
        self.days
    }
}

/// Returns the v1 analytics schema migration for a dedicated OTEL database.
///
/// Cargo Registry tables are intentionally not part of this set: Registry is
/// outside framework v1 and ClickHouse is not declared its transactional
/// system of record.
pub fn otel_schema_migrations(
    database: &str,
    retention: OtelRetentionPolicy,
) -> Result<Vec<ClickhouseMigration>, ClickhouseError> {
    let statements = vec![
        partitioned(
            OtelLogTable::migration_sql(database)?,
            "Timestamp",
            retention,
        ),
        partitioned(
            OtelTraceTable::migration_sql(database)?,
            "Timestamp",
            retention,
        ),
        partitioned(
            OtelMetricSumTable::migration_sql(database)?,
            "TimeUnix",
            retention,
        ),
        partitioned(
            OtelMetricGaugeTable::migration_sql(database)?,
            "TimeUnix",
            retention,
        ),
        partitioned(
            OtelMetricHistogramTable::migration_sql(database)?,
            "TimeUnix",
            retention,
        ),
        partitioned(
            OtelMetricSummaryTable::migration_sql(database)?,
            "TimeUnix",
            retention,
        ),
        partitioned(
            OtelMetricExponentialHistogramTable::migration_sql(database)?,
            "TimeUnix",
            retention,
        ),
    ];
    Ok(vec![ClickhouseMigration::new(
        1,
        "create partitioned OTEL analytics tables",
        statements,
    )?])
}

fn partitioned(sql: String, time_column: &str, retention: OtelRetentionPolicy) -> String {
    let order_marker = " ORDER BY ";
    let partition = format!(" PARTITION BY toYYYYMM(`{time_column}`){order_marker}");
    let sql = sql.replacen(order_marker, &partition, 1);
    format!(
        "{sql} TTL `{time_column}` + INTERVAL {} DAY DELETE",
        retention.days()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workload_schema_is_partitioned_retained_and_separate() {
        let migrations =
            otel_schema_migrations("otel", OtelRetentionPolicy::new(30).unwrap()).unwrap();
        assert_eq!(migrations.len(), 1);
        assert_eq!(migrations[0].statements().len(), 7);
        for statement in migrations[0].statements() {
            assert!(statement.contains("PARTITION BY toYYYYMM"));
            assert!(statement.contains("TTL `"));
            assert!(statement.contains("INTERVAL 30 DAY DELETE"));
            assert!(!statement.contains("registry"));
        }
        assert!(OtelRetentionPolicy::new(0).is_err());
        assert!(OtelRetentionPolicy::new(3_651).is_err());
    }
}
