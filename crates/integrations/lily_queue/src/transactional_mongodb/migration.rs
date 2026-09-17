use lily_mongo_repository::{MongoOperationContext, MongoRepositoryError};
use lily_mongodb::{
    DatabaseService, MongoMigration, MongoMigrationReport, MongoMigrationRunner,
    MongoMigrationStep, bson::doc,
};

use crate::transactional::{
    MAX_OUTBOX_TRANSPORT_IDENTITY_BYTES, MAX_TRANSACTIONAL_HANDLER_IDENTITY_BYTES,
};

pub(crate) const SCHEMA_VERSION: i64 = 1;
pub(crate) const SCHEMA_FINGERPRINT: &str = "lily-queue-mongodb-inbox-outbox-v1";
pub(crate) const MIGRATION_COMPONENT: &str = "lily.queue.transactional_inbox";
pub(crate) const SCHEMA_COLLECTION: &str = "_lily_queue_transactional_schema";
pub(crate) const INBOX_COLLECTION: &str = "_lily_queue_inbox";
pub(crate) const LEASE_COLLECTION: &str = "_lily_queue_inbox_leases";
pub(crate) const OUTBOX_COLLECTION: &str = "_lily_queue_outbox";

pub(crate) const INBOX_IDENTITY_INDEX: &str = "lily_queue_inbox_identity_unique";
pub(crate) const INBOX_COMPLETED_INDEX: &str = "lily_queue_inbox_completed";
pub(crate) const LEASE_EXPIRY_INDEX: &str = "lily_queue_inbox_lease_expiry";
pub(crate) const OUTBOX_EVENT_INDEX: &str = "lily_queue_outbox_event_unique";
pub(crate) const OUTBOX_READY_INDEX: &str = "lily_queue_outbox_ready";
pub(crate) const OUTBOX_DELIVERED_INDEX: &str = "lily_queue_outbox_delivered";
pub(crate) const MAX_FAILURE_CODE_BYTES: usize = 128;

const UUID_PATTERN: &str = "^(?!00000000-0000-0000-0000-000000000000$)[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$";
const INBOX_KEY_PATTERN: &str = "^[0-9a-f]{64}$";
const CONTENT_KIND_PATTERN: &str = "^[a-z0-9][a-z0-9._+-]*$";
const FAILURE_CODE_PATTERN: &str = "^[A-Z0-9_]+$";
const TRACEPARENT_PATTERN: &str = "^00-(?!0{32}-)[0-9a-f]{32}-(?!0{16}-)[0-9a-f]{16}-[0-9a-f]{2}$";

pub(crate) fn inbox_validator() -> lily_mongodb::bson::Document {
    doc! { "$jsonSchema": {
        "bsonType": "object",
        "additionalProperties": false,
        "required": [
            "_id", "handler_identity", "event_id", "state", "created_at",
            "updated_at", "completed_at"
        ],
        "properties": {
            "_id": {
                "bsonType": "string", "minLength": 64_i64, "maxLength": 64_i64,
                "pattern": INBOX_KEY_PATTERN,
            },
            "handler_identity": {
                "bsonType": "string", "minLength": 1_i64,
                "maxLength": MAX_TRANSACTIONAL_HANDLER_IDENTITY_BYTES as i64,
            },
            "event_id": {
                "bsonType": "string", "minLength": 36_i64, "maxLength": 36_i64,
                "pattern": UUID_PATTERN,
            },
            "state": { "enum": ["processing", "completed"] },
            "lock_token": {
                "bsonType": "string", "minLength": 36_i64, "maxLength": 36_i64,
                "pattern": UUID_PATTERN,
            },
            "locked_until": { "bsonType": "date" },
            "created_at": { "bsonType": "date" },
            "updated_at": { "bsonType": "date" },
            "completed_at": { "bsonType": ["date", "null"] },
        },
        "oneOf": [
            {
                "properties": {
                    "state": { "enum": ["processing"] },
                    "completed_at": { "bsonType": "null" },
                },
                "required": ["lock_token", "locked_until"]
            },
            {
                "properties": {
                    "state": { "enum": ["completed"] },
                    "completed_at": { "bsonType": "date" },
                },
                "required": ["completed_at"],
                "not": { "anyOf": [
                    { "required": ["lock_token"] },
                    { "required": ["locked_until"] },
                ] }
            }
        ]
    }}
}

pub(crate) fn lease_validator() -> lily_mongodb::bson::Document {
    doc! { "$jsonSchema": {
        "bsonType": "object",
        "additionalProperties": false,
        "required": ["_id", "owner", "expires_at", "updated_at"],
        "properties": {
            "_id": {
                "bsonType": "string", "minLength": 64_i64, "maxLength": 64_i64,
                "pattern": INBOX_KEY_PATTERN,
            },
            "owner": {
                "bsonType": "string", "minLength": 36_i64, "maxLength": 36_i64,
                "pattern": UUID_PATTERN,
            },
            "expires_at": { "bsonType": "date" },
            "updated_at": { "bsonType": "date" },
        }
    }}
}

pub(crate) fn outbox_validator() -> lily_mongodb::bson::Document {
    doc! { "$jsonSchema": {
        "bsonType": "object",
        "additionalProperties": false,
        "required": [
            "_id", "source_handler_identity", "source_event_id", "event_id",
            "exchange_name", "routing_key", "schema_version", "content_kind",
            "content_type", "body", "traceparent", "available_at", "claim_owner",
            "claim_token", "claimed_until", "publish_attempts", "last_failure_code",
            "created_at", "delivered_at"
        ],
        "properties": {
            "_id": {
                "bsonType": "string", "minLength": 36_i64, "maxLength": 36_i64,
                "pattern": UUID_PATTERN,
            },
            "source_handler_identity": {
                "bsonType": "string", "minLength": 1_i64,
                "maxLength": MAX_TRANSACTIONAL_HANDLER_IDENTITY_BYTES as i64,
            },
            "source_event_id": {
                "bsonType": "string", "minLength": 36_i64, "maxLength": 36_i64,
                "pattern": UUID_PATTERN,
            },
            "event_id": {
                "bsonType": "string", "minLength": 36_i64, "maxLength": 36_i64,
                "pattern": UUID_PATTERN,
            },
            "exchange_name": {
                "bsonType": "string", "minLength": 1_i64,
                "maxLength": MAX_OUTBOX_TRANSPORT_IDENTITY_BYTES as i64,
            },
            "routing_key": {
                "bsonType": "string", "minLength": 1_i64,
                "maxLength": MAX_OUTBOX_TRANSPORT_IDENTITY_BYTES as i64,
            },
            "schema_version": { "bsonType": "int", "minimum": 1, "maximum": 65535 },
            "content_kind": {
                "bsonType": "string", "minLength": 1_i64,
                "maxLength": lily_queue_client::MAX_PUBLISH_CONTENT_KIND_BYTES as i64,
                "pattern": CONTENT_KIND_PATTERN,
            },
            "content_type": {
                "bsonType": "string", "minLength": 1_i64,
                "maxLength": lily_queue_client::MAX_PUBLISH_CONTENT_TYPE_BYTES as i64,
            },
            "body": { "bsonType": "binData" },
            "traceparent": {
                "bsonType": ["string", "null"],
                "maxLength": 55_i64,
                "pattern": TRACEPARENT_PATTERN,
            },
            "available_at": { "bsonType": "date" },
            "claim_owner": {
                "bsonType": ["string", "null"], "maxLength": 36_i64,
                "pattern": UUID_PATTERN,
            },
            "claim_token": {
                "bsonType": ["string", "null"], "maxLength": 36_i64,
                "pattern": UUID_PATTERN,
            },
            "claimed_until": { "bsonType": ["date", "null"] },
            "publish_attempts": { "bsonType": "long", "minimum": 0 },
            "last_failure_code": {
                "bsonType": ["string", "null"],
                "maxLength": MAX_FAILURE_CODE_BYTES as i64,
                "pattern": FAILURE_CODE_PATTERN,
            },
            "created_at": { "bsonType": "date" },
            "delivered_at": { "bsonType": ["date", "null"] },
        },
        "allOf": [
            { "oneOf": [
                { "properties": {
                    "claim_owner": { "bsonType": "null" },
                    "claim_token": { "bsonType": "null" },
                    "claimed_until": { "bsonType": "null" },
                } },
                { "properties": {
                    "claim_owner": { "bsonType": "string" },
                    "claim_token": { "bsonType": "string" },
                    "claimed_until": { "bsonType": "date" },
                } },
            ] },
            { "oneOf": [
                { "properties": { "delivered_at": { "bsonType": "null" } } },
                { "properties": {
                    "delivered_at": { "bsonType": "date" },
                    "claim_owner": { "bsonType": "null" },
                    "claim_token": { "bsonType": "null" },
                    "claimed_until": { "bsonType": "null" },
                    "last_failure_code": { "bsonType": "null" },
                    "publish_attempts": { "minimum": 1 },
                } },
            ] },
            { "oneOf": [
                { "properties": { "last_failure_code": { "bsonType": "null" } } },
                { "properties": {
                    "last_failure_code": { "bsonType": "string" },
                    "publish_attempts": { "minimum": 1 },
                } },
            ] },
        ]
    }}
}

/// Outcome of applying Lily's MongoDB transactional inbox/outbox migration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MongoInboxOutboxMigrationReport {
    inner: MongoMigrationReport,
}

impl MongoInboxOutboxMigrationReport {
    /// Number of matching component migrations already present in history.
    #[must_use]
    pub const fn previously_applied(&self) -> usize {
        self.inner.previously_applied()
    }

    /// Application migration versions applied by this invocation.
    #[must_use]
    pub fn newly_applied(&self) -> &[i64] {
        self.inner.newly_applied()
    }
}

/// Explicit deployment boundary for Lily-owned MongoDB inbox/outbox storage.
///
/// Constructing this value performs no database I/O. Lily owns the component
/// namespace and its internal migration version, so an application release
/// number cannot accidentally fork or invalidate framework history.
pub struct MongoInboxOutboxMigrator {
    runner: MongoMigrationRunner,
}

impl MongoInboxOutboxMigrator {
    /// Builds the component-scoped, idempotent migration plan.
    pub fn new(database: &DatabaseService) -> Result<Self, MongoRepositoryError> {
        let migration = MongoMigration::new(
            SCHEMA_VERSION,
            "lily_queue_transactional_inbox_v1",
            migration_steps()?,
        )?;
        Ok(Self {
            runner: MongoMigrationRunner::for_component(
                database,
                MIGRATION_COMPONENT,
                vec![migration],
            )?,
        })
    }

    /// Applies the explicit plan under the caller's operation policy.
    pub async fn apply(
        &self,
        operation: &MongoOperationContext<'_>,
    ) -> Result<MongoInboxOutboxMigrationReport, MongoRepositoryError> {
        self.runner
            .apply(operation)
            .await
            .map(|inner| MongoInboxOutboxMigrationReport { inner })
    }
}

fn migration_steps() -> Result<Vec<MongoMigrationStep>, MongoRepositoryError> {
    let mut steps = vec![
        MongoMigrationStep::ensure_collection(SCHEMA_COLLECTION)?,
        MongoMigrationStep::ensure_collection(INBOX_COLLECTION)?,
        MongoMigrationStep::ensure_collection(LEASE_COLLECTION)?,
        MongoMigrationStep::ensure_collection(OUTBOX_COLLECTION)?,
        MongoMigrationStep::ensure_index(
            INBOX_COLLECTION,
            INBOX_IDENTITY_INDEX,
            doc! { "handler_identity": 1, "event_id": 1 },
            true,
        )?,
        MongoMigrationStep::ensure_index(
            INBOX_COLLECTION,
            INBOX_COMPLETED_INDEX,
            doc! { "completed_at": 1 },
            false,
        )?,
        MongoMigrationStep::ensure_index(
            LEASE_COLLECTION,
            LEASE_EXPIRY_INDEX,
            doc! { "expires_at": 1 },
            false,
        )?,
        MongoMigrationStep::ensure_index(
            OUTBOX_COLLECTION,
            OUTBOX_EVENT_INDEX,
            doc! { "event_id": 1 },
            true,
        )?,
        MongoMigrationStep::ensure_index(
            OUTBOX_COLLECTION,
            OUTBOX_READY_INDEX,
            doc! {
                "delivered_at": 1,
                "available_at": 1,
                "claimed_until": 1,
                "created_at": 1,
                "_id": 1,
            },
            false,
        )?,
        MongoMigrationStep::ensure_index(
            OUTBOX_COLLECTION,
            OUTBOX_DELIVERED_INDEX,
            doc! { "delivered_at": 1 },
            false,
        )?,
    ];

    // Validators are deliberately explicit migration operations. Runtime
    // readiness only inspects the resulting fingerprint and never issues DDL.
    steps.extend([
        MongoMigrationStep::run_command(doc! {
            "collMod": INBOX_COLLECTION,
            "validator": inbox_validator(),
            "validationLevel": "strict",
            "validationAction": "error",
        })?,
        MongoMigrationStep::run_command(doc! {
            "collMod": LEASE_COLLECTION,
            "validator": lease_validator(),
            "validationLevel": "strict",
            "validationAction": "error",
        })?,
        MongoMigrationStep::run_command(doc! {
            "collMod": OUTBOX_COLLECTION,
            "validator": outbox_validator(),
            "validationLevel": "strict",
            "validationAction": "error",
        })?,
        MongoMigrationStep::run_command(doc! {
            "update": SCHEMA_COLLECTION,
            "updates": [{
                "q": { "_id": MIGRATION_COMPONENT },
                "u": { "$setOnInsert": {
                    "version": SCHEMA_VERSION,
                    "fingerprint": SCHEMA_FINGERPRINT
                }},
                "upsert": true,
            }],
        })?,
    ]);

    Ok(steps)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_plan_is_explicit_bounded_and_versioned() {
        let steps = migration_steps().expect("framework migration steps");
        assert_eq!(steps.len(), 14);
        assert_eq!(SCHEMA_VERSION, 1);
        assert_eq!(MIGRATION_COMPONENT, "lily.queue.transactional_inbox");
    }

    #[test]
    fn validators_are_closed_bounded_and_cross_field_consistent() {
        let inbox = inbox_validator();
        let inbox_schema = inbox
            .get_document("$jsonSchema")
            .expect("inbox JSON schema");
        assert_eq!(inbox_schema.get_bool("additionalProperties"), Ok(false));
        assert_eq!(inbox_schema.get_array("oneOf").map(Vec::len), Ok(2));

        let lease = lease_validator();
        let lease_schema = lease
            .get_document("$jsonSchema")
            .expect("lease JSON schema");
        assert_eq!(lease_schema.get_bool("additionalProperties"), Ok(false));

        let outbox = outbox_validator();
        let outbox_schema = outbox
            .get_document("$jsonSchema")
            .expect("outbox JSON schema");
        assert_eq!(outbox_schema.get_bool("additionalProperties"), Ok(false));
        assert_eq!(outbox_schema.get_array("allOf").map(Vec::len), Ok(3));
        let properties = outbox_schema
            .get_document("properties")
            .expect("outbox properties");
        assert_eq!(
            properties
                .get_document("source_handler_identity")
                .and_then(|property| property.get_i64("maxLength")),
            Ok(MAX_TRANSACTIONAL_HANDLER_IDENTITY_BYTES as i64)
        );
        assert_eq!(
            properties
                .get_document("content_kind")
                .and_then(|property| property.get_i64("maxLength")),
            Ok(lily_queue_client::MAX_PUBLISH_CONTENT_KIND_BYTES as i64)
        );
        assert_eq!(
            properties
                .get_document("last_failure_code")
                .and_then(|property| property.get_i64("maxLength")),
            Ok(MAX_FAILURE_CODE_BYTES as i64)
        );
    }
}
