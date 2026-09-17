use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use lily_postgresql::{PgDatabaseService, diesel, diesel_async};

use super::PostgresReliabilityError;

pub(crate) const SCHEMA_VERSION: i64 = 1;
const COMPONENT: &str = "postgresql_inbox_outbox";
const MIGRATION_LOCK: i64 = 5_495_876_150_254_454_101;

const CREATE_SCHEMA: &str = "CREATE SCHEMA IF NOT EXISTS lily_queue";
const CREATE_LEDGER: &str = r#"
CREATE TABLE IF NOT EXISTS lily_queue.schema_migrations (
    component TEXT PRIMARY KEY,
    version BIGINT NOT NULL CHECK (version > 0),
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)"#;
const CREATE_INBOX: &str = r#"
CREATE TABLE IF NOT EXISTS lily_queue.inbox (
    handler_identity VARCHAR(512) NOT NULL,
    event_id UUID NOT NULL,
    state SMALLINT NOT NULL CHECK (state IN (0, 1)),
    lock_token UUID NOT NULL,
    locked_until TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ NULL,
    PRIMARY KEY (handler_identity, event_id)
)"#;
const CREATE_OUTBOX: &str = r#"
CREATE TABLE IF NOT EXISTS lily_queue.outbox (
    record_id UUID PRIMARY KEY,
    source_handler_identity VARCHAR(512) NOT NULL,
    source_event_id UUID NOT NULL,
    event_id UUID NOT NULL UNIQUE,
    exchange_name VARCHAR(200) NOT NULL,
    routing_key VARCHAR(200) NOT NULL,
    schema_version INTEGER NOT NULL CHECK (schema_version BETWEEN 1 AND 65535),
    content_kind VARCHAR(64) NOT NULL,
    content_type VARCHAR(255) NOT NULL,
    body BYTEA NOT NULL,
    traceparent VARCHAR(128) NULL,
    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    claim_token UUID NULL,
    claimed_until TIMESTAMPTZ NULL,
    publish_attempts BIGINT NOT NULL DEFAULT 0 CHECK (publish_attempts >= 0),
    last_failure_code VARCHAR(64) NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    delivered_at TIMESTAMPTZ NULL
)"#;
const CREATE_OUTBOX_READY_INDEX: &str = r#"
CREATE INDEX IF NOT EXISTS lily_queue_outbox_ready
ON lily_queue.outbox (available_at, created_at, record_id)
WHERE delivered_at IS NULL"#;
const CREATE_INBOX_COMPLETED_INDEX: &str = r#"
CREATE INDEX IF NOT EXISTS lily_queue_inbox_completed
ON lily_queue.inbox (completed_at)
WHERE state = 1"#;
const CREATE_OUTBOX_DELIVERED_INDEX: &str = r#"
CREATE INDEX IF NOT EXISTS lily_queue_outbox_delivered
ON lily_queue.outbox (delivered_at)
WHERE delivered_at IS NOT NULL"#;

// The ledger is checked before CREATE TABLE or a version SELECT. This turns a
// foreign object occupying Lily's ledger name into a stable schema-drift error
// instead of leaking an implementation-specific PostgreSQL error.
const VALIDATE_LEDGER_SCHEMA: &str = r#"
WITH expected_columns(column_name, ordinal, formatted_type, not_null, default_expr) AS (
    VALUES
      ('component', 1, 'text', true, NULL),
      ('version', 2, 'bigint', true, NULL),
      ('applied_at', 3, 'timestamp with time zone', true, 'now()')
), actual_columns AS (
    SELECT a.attname::text AS column_name, a.attnum::integer AS ordinal,
           format_type(a.atttypid, a.atttypmod) AS formatted_type,
           a.attnotnull AS not_null, pg_get_expr(d.adbin, d.adrelid) AS default_expr
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
    LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum
    WHERE n.nspname = 'lily_queue' AND c.relname = 'schema_migrations'
      AND c.relkind = 'r' AND a.attnum > 0 AND NOT a.attisdropped
), columns_match AS (
    SELECT COUNT(*) = (SELECT COUNT(*) FROM expected_columns)
       AND NOT EXISTS (
          SELECT 1 FROM expected_columns e
          FULL JOIN actual_columns a USING (column_name)
          WHERE e.column_name IS NULL OR a.column_name IS NULL
             OR e.ordinal <> a.ordinal OR e.formatted_type <> a.formatted_type
             OR e.not_null <> a.not_null OR e.default_expr IS DISTINCT FROM a.default_expr
       ) AS value
    FROM actual_columns
), relation_match AS (
    SELECT COUNT(*) = 1
       AND bool_and(c.relkind = 'r' AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity) AS value
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'lily_queue' AND c.relname = 'schema_migrations'
), constraints_match AS (
    SELECT COUNT(*) = 2
       AND bool_and(
          (contype = 'p' AND pg_get_constraintdef(oid) = 'PRIMARY KEY (component)')
          OR (contype = 'c' AND replace(pg_get_constraintdef(oid), ' ', '') = 'CHECK((version>0))')
       ) AS value
    FROM pg_catalog.pg_constraint
    WHERE conrelid = 'lily_queue.schema_migrations'::regclass
      -- PostgreSQL 18 exposes NOT NULL constraints as catalog rows. Column
      -- nullability is already frozen by the column fingerprint; every other
      -- user-visible constraint kind must be part of Lily's exact contract.
      AND contype <> 'n'
), indexes_match AS (
    SELECT COUNT(*) = 1 AND bool_and(i.indisvalid AND i.indisready) AS value
    FROM pg_catalog.pg_index i
    WHERE i.indrelid = 'lily_queue.schema_migrations'::regclass
), no_runtime_hooks AS (
    SELECT
      NOT EXISTS (SELECT 1 FROM pg_catalog.pg_trigger WHERE tgrelid = 'lily_queue.schema_migrations'::regclass AND NOT tgisinternal)
      AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_policy WHERE polrelid = 'lily_queue.schema_migrations'::regclass)
      AS value
)
SELECT (SELECT value FROM columns_match)
   AND (SELECT value FROM relation_match)
   AND (SELECT value FROM constraints_match)
   AND (SELECT value FROM indexes_match)
   AND (SELECT value FROM no_runtime_hooks) AS value
"#;

// This query is deliberately catalog-based rather than ledger-only. A version
// row proves migration ordering; it does not prove that a pre-existing object
// with the same name has Lily's columns, constraints, or partial indexes.
const VALIDATE_V1_SCHEMA: &str = r#"
WITH expected_columns(table_name, column_name, ordinal, formatted_type, not_null, default_expr) AS (
    VALUES
      ('schema_migrations', 'component', 1, 'text', true, NULL),
      ('schema_migrations', 'version', 2, 'bigint', true, NULL),
      ('schema_migrations', 'applied_at', 3, 'timestamp with time zone', true, 'now()'),
      ('inbox', 'handler_identity', 1, 'character varying(512)', true, NULL),
      ('inbox', 'event_id', 2, 'uuid', true, NULL),
      ('inbox', 'state', 3, 'smallint', true, NULL),
      ('inbox', 'lock_token', 4, 'uuid', true, NULL),
      ('inbox', 'locked_until', 5, 'timestamp with time zone', true, NULL),
      ('inbox', 'created_at', 6, 'timestamp with time zone', true, 'now()'),
      ('inbox', 'updated_at', 7, 'timestamp with time zone', true, 'now()'),
      ('inbox', 'completed_at', 8, 'timestamp with time zone', false, NULL),
      ('outbox', 'record_id', 1, 'uuid', true, NULL),
      ('outbox', 'source_handler_identity', 2, 'character varying(512)', true, NULL),
      ('outbox', 'source_event_id', 3, 'uuid', true, NULL),
      ('outbox', 'event_id', 4, 'uuid', true, NULL),
      ('outbox', 'exchange_name', 5, 'character varying(200)', true, NULL),
      ('outbox', 'routing_key', 6, 'character varying(200)', true, NULL),
      ('outbox', 'schema_version', 7, 'integer', true, NULL),
      ('outbox', 'content_kind', 8, 'character varying(64)', true, NULL),
      ('outbox', 'content_type', 9, 'character varying(255)', true, NULL),
      ('outbox', 'body', 10, 'bytea', true, NULL),
      ('outbox', 'traceparent', 11, 'character varying(128)', false, NULL),
      ('outbox', 'available_at', 12, 'timestamp with time zone', true, 'now()'),
      ('outbox', 'claim_token', 13, 'uuid', false, NULL),
      ('outbox', 'claimed_until', 14, 'timestamp with time zone', false, NULL),
      ('outbox', 'publish_attempts', 15, 'bigint', true, '0'),
      ('outbox', 'last_failure_code', 16, 'character varying(64)', false, NULL),
      ('outbox', 'created_at', 17, 'timestamp with time zone', true, 'now()'),
      ('outbox', 'delivered_at', 18, 'timestamp with time zone', false, NULL)
), actual_columns AS (
    SELECT c.relname::text AS table_name, a.attname::text AS column_name,
           a.attnum::integer AS ordinal, format_type(a.atttypid, a.atttypmod) AS formatted_type,
           a.attnotnull AS not_null, pg_get_expr(d.adbin, d.adrelid) AS default_expr
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
    LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum
    WHERE n.nspname = 'lily_queue' AND c.relname IN ('schema_migrations', 'inbox', 'outbox')
      AND c.relkind IN ('r', 'p') AND a.attnum > 0 AND NOT a.attisdropped
), columns_match AS (
    SELECT COUNT(*) = (SELECT COUNT(*) FROM expected_columns)
       AND NOT EXISTS (
          SELECT 1 FROM expected_columns e
          FULL JOIN actual_columns a USING (table_name, column_name)
          WHERE e.table_name IS NULL OR a.table_name IS NULL
             OR e.ordinal <> a.ordinal OR e.formatted_type <> a.formatted_type
             OR e.not_null <> a.not_null OR e.default_expr IS DISTINCT FROM a.default_expr
       ) AS value
    FROM actual_columns
), relations_match AS (
    SELECT COUNT(*) = 3
       AND bool_and(c.relkind = 'r' AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity) AS value
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'lily_queue'
      AND c.relname IN ('schema_migrations', 'inbox', 'outbox')
), constraints_match AS (
    SELECT (SELECT COUNT(*) FROM pg_constraint WHERE conrelid IN (
        'lily_queue.schema_migrations'::regclass,
        'lily_queue.inbox'::regclass,
        'lily_queue.outbox'::regclass
      ) AND contype <> 'n') = 8
      AND EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'lily_queue.schema_migrations'::regclass AND contype = 'p' AND pg_get_constraintdef(oid) = 'PRIMARY KEY (component)')
      AND EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'lily_queue.schema_migrations'::regclass AND contype = 'c' AND replace(pg_get_constraintdef(oid), ' ', '') = 'CHECK((version>0))')
      AND EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'lily_queue.inbox'::regclass AND contype = 'p' AND pg_get_constraintdef(oid) = 'PRIMARY KEY (handler_identity, event_id)')
      AND EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'lily_queue.inbox'::regclass AND contype = 'c' AND replace(pg_get_constraintdef(oid), ' ', '') IN ('CHECK((state=ANY(ARRAY[0,1])))', 'CHECK(((state=0)OR(state=1)))'))
      AND EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'lily_queue.outbox'::regclass AND contype = 'p' AND pg_get_constraintdef(oid) = 'PRIMARY KEY (record_id)')
      AND EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'lily_queue.outbox'::regclass AND contype = 'u' AND pg_get_constraintdef(oid) = 'UNIQUE (event_id)')
      AND EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'lily_queue.outbox'::regclass AND contype = 'c' AND replace(pg_get_constraintdef(oid), ' ', '') = 'CHECK(((schema_version>=1)AND(schema_version<=65535)))')
      AND EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'lily_queue.outbox'::regclass AND contype = 'c' AND replace(pg_get_constraintdef(oid), ' ', '') = 'CHECK((publish_attempts>=0))')
      AS value
), indexes_match AS (
    SELECT
      (SELECT COUNT(*) FROM pg_catalog.pg_index WHERE indrelid IN (
          'lily_queue.schema_migrations'::regclass,
          'lily_queue.inbox'::regclass,
          'lily_queue.outbox'::regclass
       )) = 7
      AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_index WHERE indrelid IN (
          'lily_queue.schema_migrations'::regclass,
          'lily_queue.inbox'::regclass,
          'lily_queue.outbox'::regclass
       ) AND (NOT indisvalid OR NOT indisready))
      AND EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'lily_queue' AND indexname = 'lily_queue_outbox_ready' AND indexdef = 'CREATE INDEX lily_queue_outbox_ready ON lily_queue.outbox USING btree (available_at, created_at, record_id) WHERE (delivered_at IS NULL)')
      AND EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'lily_queue' AND indexname = 'lily_queue_inbox_completed' AND indexdef = 'CREATE INDEX lily_queue_inbox_completed ON lily_queue.inbox USING btree (completed_at) WHERE (state = 1)')
      AND EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'lily_queue' AND indexname = 'lily_queue_outbox_delivered' AND indexdef = 'CREATE INDEX lily_queue_outbox_delivered ON lily_queue.outbox USING btree (delivered_at) WHERE (delivered_at IS NOT NULL)')
      AS value
), no_runtime_hooks AS (
    SELECT
      NOT EXISTS (SELECT 1 FROM pg_catalog.pg_trigger WHERE tgrelid IN (
          'lily_queue.schema_migrations'::regclass,
          'lily_queue.inbox'::regclass,
          'lily_queue.outbox'::regclass
      ) AND NOT tgisinternal)
      AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_policy WHERE polrelid IN (
          'lily_queue.schema_migrations'::regclass,
          'lily_queue.inbox'::regclass,
          'lily_queue.outbox'::regclass
      )) AS value
)
SELECT (SELECT value FROM columns_match)
   AND (SELECT value FROM relations_match)
   AND (SELECT value FROM constraints_match)
   AND (SELECT value FROM indexes_match)
   AND (SELECT value FROM no_runtime_hooks) AS value
"#;

#[derive(diesel::QueryableByName)]
struct BoolRow {
    #[diesel(sql_type = diesel::sql_types::Bool)]
    value: bool,
}

#[derive(diesel::QueryableByName)]
struct VersionRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    version: i64,
}

/// Result returned by an explicit Lily inbox/outbox migration pass.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PostgresInboxOutboxMigrationReport {
    /// Schema version observed after the pass.
    pub version: i64,
    /// Whether this pass applied a migration.
    pub applied: bool,
}

/// Explicit migrator for Lily-owned PostgreSQL inbox/outbox tables.
///
/// Application and consumer startup never call this type implicitly. A
/// deployment migration command must run it before promoting a runtime that
/// selects `delivery_guarantee = "transactional_inbox"`.
pub struct PostgresInboxOutboxMigrator {
    database: Arc<PgDatabaseService>,
}

impl PostgresInboxOutboxMigrator {
    /// Binds the migrator to the application-owned PostgreSQL service.
    #[must_use]
    pub fn new(database: Arc<PgDatabaseService>) -> Self {
        Self { database }
    }

    /// Applies Lily's namespaced migrations under a transaction advisory lock.
    pub async fn migrate(
        &self,
    ) -> Result<PostgresInboxOutboxMigrationReport, PostgresReliabilityError> {
        let future_schema = Arc::new(Mutex::new(None));
        let future_schema_in_transaction = Arc::clone(&future_schema);
        let schema_drift = Arc::new(AtomicBool::new(false));
        let schema_drift_in_transaction = Arc::clone(&schema_drift);
        let result = self
            .database
            .transaction(None, |connection, _| {
                Box::pin(async move {
                    use diesel_async::RunQueryDsl as _;

                    diesel::sql_query("SELECT pg_advisory_xact_lock($1)")
                        .bind::<diesel::sql_types::BigInt, _>(MIGRATION_LOCK)
                        .execute(connection)
                        .await?;
                    diesel::sql_query(CREATE_SCHEMA).execute(connection).await?;
                    if ledger_object_exists(connection).await?
                        && !ledger_schema_matches(connection).await?
                    {
                        schema_drift_in_transaction.store(true, Ordering::Release);
                        return Err(lily_postgresql::PgError::Migration {
                            code: "schema_drift",
                        });
                    }
                    diesel::sql_query(CREATE_LEDGER).execute(connection).await?;

                    let installed = diesel::sql_query(
                        "SELECT version FROM lily_queue.schema_migrations WHERE component = $1",
                    )
                    .bind::<diesel::sql_types::Text, _>(COMPONENT)
                    .get_result::<VersionRow>(connection)
                    .await
                    .optional()?;
                    if let Some(installed) = installed {
                        if installed.version > SCHEMA_VERSION {
                            *future_schema_in_transaction
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                                Some(installed.version);
                            return Err(lily_postgresql::PgError::Migration {
                                code: "schema_too_new",
                            });
                        }
                        if installed.version == SCHEMA_VERSION {
                            if !schema_objects_exist(connection).await?
                                || !v1_schema_matches(connection).await?
                            {
                                schema_drift_in_transaction.store(true, Ordering::Release);
                                return Err(lily_postgresql::PgError::Migration {
                                    code: "schema_drift",
                                });
                            }
                            return Ok(PostgresInboxOutboxMigrationReport {
                                version: SCHEMA_VERSION,
                                applied: false,
                            });
                        }
                    }

                    // V1 has no predecessor. Any unledgered storage object is a
                    // namespace collision, not a partially supported Lily
                    // schema. Reject it before index DDL can fail with an
                    // implementation-specific database error.
                    if storage_objects_exist(connection).await? {
                        schema_drift_in_transaction.store(true, Ordering::Release);
                        return Err(lily_postgresql::PgError::Migration {
                            code: "schema_drift",
                        });
                    }

                    diesel::sql_query(CREATE_INBOX).execute(connection).await?;
                    diesel::sql_query(CREATE_OUTBOX).execute(connection).await?;
                    diesel::sql_query(CREATE_OUTBOX_READY_INDEX)
                        .execute(connection)
                        .await?;
                    diesel::sql_query(CREATE_INBOX_COMPLETED_INDEX)
                        .execute(connection)
                        .await?;
                    diesel::sql_query(CREATE_OUTBOX_DELIVERED_INDEX)
                        .execute(connection)
                        .await?;
                    if !v1_schema_matches(connection).await? {
                        schema_drift_in_transaction.store(true, Ordering::Release);
                        return Err(lily_postgresql::PgError::Migration {
                            code: "schema_drift",
                        });
                    }
                    diesel::sql_query(
                        r#"INSERT INTO lily_queue.schema_migrations (component, version)
                           VALUES ($1, $2)
                           ON CONFLICT (component) DO UPDATE
                           SET version = EXCLUDED.version, applied_at = NOW()"#,
                    )
                    .bind::<diesel::sql_types::Text, _>(COMPONENT)
                    .bind::<diesel::sql_types::BigInt, _>(SCHEMA_VERSION)
                    .execute(connection)
                    .await?;

                    Ok(PostgresInboxOutboxMigrationReport {
                        version: SCHEMA_VERSION,
                        applied: true,
                    })
                })
            })
            .await;
        match result {
            Ok(report) => Ok(report),
            Err(_) if schema_drift.load(Ordering::Acquire) => {
                Err(PostgresReliabilityError::SchemaDrift)
            }
            Err(_)
                if future_schema
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .is_some() =>
            {
                let installed = future_schema
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .expect("future schema guard was checked");
                Err(PostgresReliabilityError::SchemaTooNew {
                    installed,
                    supported: SCHEMA_VERSION,
                })
            }
            Err(error) => Err(PostgresReliabilityError::from(error)),
        }
    }
}

pub(crate) async fn ensure_schema_ready(
    database: &PgDatabaseService,
) -> Result<(), PostgresReliabilityError> {
    use diesel_async::RunQueryDsl as _;

    let ledger_exists = database
        .with_connection(
            |connection, _| {
                Box::pin(async move { ledger_object_exists(connection).await.map_err(Into::into) })
            },
            None,
        )
        .await?;
    if !ledger_exists {
        let partial_storage = database
            .with_connection(
                |connection, _| {
                    Box::pin(
                        async move { storage_objects_exist(connection).await.map_err(Into::into) },
                    )
                },
                None,
            )
            .await?;
        return if partial_storage {
            Err(PostgresReliabilityError::SchemaDrift)
        } else {
            Err(PostgresReliabilityError::SchemaMissing)
        };
    }

    // Fingerprint the ledger before any version SELECT. A foreign relation
    // occupying the canonical name must never leak a database-shape error.
    let ledger_matches = database
        .with_connection(
            |connection, _| {
                Box::pin(async move { ledger_schema_matches(connection).await.map_err(Into::into) })
            },
            None,
        )
        .await?;
    if !ledger_matches {
        return Err(PostgresReliabilityError::SchemaDrift);
    }

    let all_objects_exist = database
        .with_connection(
            |connection, _| {
                Box::pin(async move { schema_objects_exist(connection).await.map_err(Into::into) })
            },
            None,
        )
        .await?;
    if !all_objects_exist {
        return Err(PostgresReliabilityError::SchemaDrift);
    }

    let installed = database
        .with_connection(
            |connection, _| {
                Box::pin(async move {
                    diesel::sql_query(
                        "SELECT version FROM lily_queue.schema_migrations WHERE component = $1",
                    )
                    .bind::<diesel::sql_types::Text, _>(COMPONENT)
                    .get_result::<VersionRow>(connection)
                    .await
                    .optional()
                    .map_err(Into::into)
                })
            },
            None,
        )
        .await?
        .ok_or(PostgresReliabilityError::SchemaMissing)?
        .version;
    match installed.cmp(&SCHEMA_VERSION) {
        std::cmp::Ordering::Less => Err(PostgresReliabilityError::SchemaOutdated {
            installed,
            required: SCHEMA_VERSION,
        }),
        std::cmp::Ordering::Greater => Err(PostgresReliabilityError::SchemaTooNew {
            installed,
            supported: SCHEMA_VERSION,
        }),
        std::cmp::Ordering::Equal => {
            let matches = database
                .with_connection(
                    |connection, _| {
                        Box::pin(
                            async move { v1_schema_matches(connection).await.map_err(Into::into) },
                        )
                    },
                    None,
                )
                .await?;
            if matches {
                Ok(())
            } else {
                Err(PostgresReliabilityError::SchemaDrift)
            }
        }
    }
}

async fn schema_objects_exist(
    connection: &mut lily_postgresql::diesel_async::AsyncPgConnection,
) -> Result<bool, diesel::result::Error> {
    use diesel_async::RunQueryDsl as _;

    diesel::sql_query(
        r#"SELECT
            to_regclass('lily_queue.schema_migrations') IS NOT NULL
            AND to_regclass('lily_queue.inbox') IS NOT NULL
            AND to_regclass('lily_queue.outbox') IS NOT NULL AS value"#,
    )
    .get_result::<BoolRow>(connection)
    .await
    .map(|row| row.value)
}

async fn storage_objects_exist(
    connection: &mut lily_postgresql::diesel_async::AsyncPgConnection,
) -> Result<bool, diesel::result::Error> {
    use diesel_async::RunQueryDsl as _;

    diesel::sql_query(
        r#"SELECT to_regclass('lily_queue.inbox') IS NOT NULL
               OR to_regclass('lily_queue.outbox') IS NOT NULL AS value"#,
    )
    .get_result::<BoolRow>(connection)
    .await
    .map(|row| row.value)
}

async fn ledger_object_exists(
    connection: &mut lily_postgresql::diesel_async::AsyncPgConnection,
) -> Result<bool, diesel::result::Error> {
    use diesel_async::RunQueryDsl as _;

    diesel::sql_query("SELECT to_regclass('lily_queue.schema_migrations') IS NOT NULL AS value")
        .get_result::<BoolRow>(connection)
        .await
        .map(|row| row.value)
}

async fn ledger_schema_matches(
    connection: &mut lily_postgresql::diesel_async::AsyncPgConnection,
) -> Result<bool, diesel::result::Error> {
    use diesel_async::RunQueryDsl as _;

    diesel::sql_query(VALIDATE_LEDGER_SCHEMA)
        .get_result::<BoolRow>(connection)
        .await
        .map(|row| row.value)
}

async fn v1_schema_matches(
    connection: &mut lily_postgresql::diesel_async::AsyncPgConnection,
) -> Result<bool, diesel::result::Error> {
    use diesel_async::RunQueryDsl as _;

    diesel::sql_query(VALIDATE_V1_SCHEMA)
        .get_result::<BoolRow>(connection)
        .await
        .map(|row| row.value)
}

trait OptionalQueryResult<T> {
    fn optional(self) -> Result<Option<T>, diesel::result::Error>;
}

impl<T> OptionalQueryResult<T> for Result<T, diesel::result::Error> {
    fn optional(self) -> Result<Option<T>, diesel::result::Error> {
        match self {
            Ok(value) => Ok(Some(value)),
            Err(diesel::result::Error::NotFound) => Ok(None),
            Err(error) => Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_contract_is_namespaced_and_has_a_separate_ledger() {
        assert!(CREATE_LEDGER.contains("lily_queue.schema_migrations"));
        assert!(CREATE_INBOX.contains("PRIMARY KEY (handler_identity, event_id)"));
        assert!(CREATE_OUTBOX.contains("event_id UUID NOT NULL UNIQUE"));
        assert!(CREATE_OUTBOX_READY_INDEX.contains("WHERE delivered_at IS NULL"));
        assert_eq!(SCHEMA_VERSION, 1);
    }

    #[test]
    fn v1_fingerprint_covers_columns_constraints_defaults_and_partial_indexes() {
        for required in [
            "format_type(a.atttypid, a.atttypmod)",
            "a.attnotnull",
            "pg_get_expr(d.adbin, d.adrelid)",
            "PRIMARY KEY (handler_identity, event_id)",
            "UNIQUE (event_id)",
            "schema_version>=1",
            "publish_attempts>=0",
            "lily_queue_outbox_ready",
            "WHERE (delivered_at IS NULL)",
            "lily_queue_inbox_completed",
            "WHERE (state = 1)",
            "lily_queue_outbox_delivered",
            "c.relkind = 'r'",
            "NOT c.relrowsecurity",
            "NOT c.relforcerowsecurity",
            "NOT indisvalid OR NOT indisready",
            "NOT tgisinternal",
            "pg_catalog.pg_policy",
            "contype <> 'n'",
        ] {
            assert!(VALIDATE_V1_SCHEMA.contains(required), "missing {required}");
        }
        assert!(
            VALIDATE_V1_SCHEMA.contains("COUNT(*) = (SELECT COUNT(*) FROM expected_columns)"),
            "extra or missing columns must fail the fingerprint"
        );
        assert!(VALIDATE_V1_SCHEMA.contains(") = 8"));
        assert!(VALIDATE_V1_SCHEMA.contains(") = 7"));
    }

    #[test]
    fn preexisting_ledger_is_fingerprinted_before_version_access() {
        for required in [
            "schema_migrations",
            "c.relkind = 'r'",
            "COUNT(*) = 2",
            "COUNT(*) = 1",
            "i.indisvalid AND i.indisready",
            "NOT tgisinternal",
            "pg_catalog.pg_policy",
            "contype <> 'n'",
        ] {
            assert!(
                VALIDATE_LEDGER_SCHEMA.contains(required),
                "missing {required}"
            );
        }
    }
}
