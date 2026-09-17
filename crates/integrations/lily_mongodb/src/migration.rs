use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use lily_mongo_repository::{MongoOperationContext, MongoRepositoryError};
use mongodb::bson::{Bson, DateTime, Document, doc};
use mongodb::error::{ErrorKind, WriteFailure};
use mongodb::options::{IndexOptions, ReturnDocument};
use mongodb::{Database, IndexModel};
use sha2::{Digest, Sha256};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::DatabaseService;

const MAX_MIGRATIONS: usize = 1_000;
const MAX_STEPS_PER_MIGRATION: usize = 1_000;
const LOCK_LEASE_MILLIS: i64 = 360_000;
const LOCK_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(LOCK_LEASE_MILLIS as u64 / 3);
const LOCK_COLLECTION: &str = "_lily_migration_lock";
const VERSION_COLLECTION: &str = "_lily_schema_migrations";
const MAX_COMPONENT_NAMESPACE_LEN: usize = 128;

/// One validated, idempotent or resumable MongoDB schema step.
///
/// Steps are opaque so callers cannot bypass the validation performed by
/// [`MongoMigrationStep::ensure_collection`],
/// [`MongoMigrationStep::ensure_index`] and
/// [`MongoMigrationStep::run_command`]. A collection derive returns these
/// values from its generated `migration_steps()` function; creating the values
/// never performs database I/O.
#[derive(Debug, Clone, PartialEq)]
pub struct MongoMigrationStep {
    kind: MongoMigrationStepKind,
}

#[derive(Debug, Clone, PartialEq)]
enum MongoMigrationStepKind {
    EnsureCollection {
        name: String,
    },
    EnsureIndex {
        collection: String,
        name: String,
        keys: Document,
        unique: bool,
    },
    /// A source-controlled command for schema features not covered by the
    /// typed collection/index helpers. It must be idempotent or resumable.
    RunCommand {
        command: Document,
    },
}

impl MongoMigrationStep {
    /// Validates a collection name and creates an idempotent ensure step.
    pub fn ensure_collection(name: impl Into<String>) -> Result<Self, MongoRepositoryError> {
        let name = name.into();
        validate_collection_name(&name)?;
        Ok(Self {
            kind: MongoMigrationStepKind::EnsureCollection { name },
        })
    }

    /// Creates an index step after validating its collection, stable name and
    /// bounded key document.
    ///
    /// Keys accept ascending/descending integer directions and MongoDB string
    /// index kinds. The migration runner uses `name` to make repeated execution
    /// converge on the same index definition.
    pub fn ensure_index(
        collection: impl Into<String>,
        name: impl Into<String>,
        keys: Document,
        unique: bool,
    ) -> Result<Self, MongoRepositoryError> {
        let collection = collection.into();
        let name = name.into();
        validate_collection_name(&collection)?;
        if name.is_empty() || name.len() > 127 || name.contains('\0') || name.contains('$') {
            return Err(MongoRepositoryError::InvalidConfiguration(
                "MongoDB migration index name is invalid".into(),
            ));
        }
        if keys.is_empty() || keys.len() > 16 {
            return Err(MongoRepositoryError::InvalidConfiguration(
                "MongoDB migration index must contain between 1 and 16 keys".into(),
            ));
        }
        for (field, direction) in &keys {
            if !valid_field_path(field)
                || !matches!(direction, Bson::Int32(1 | -1) | Bson::String(_))
            {
                return Err(MongoRepositoryError::InvalidConfiguration(
                    "MongoDB migration index key is invalid".into(),
                ));
            }
        }
        Ok(Self {
            kind: MongoMigrationStepKind::EnsureIndex {
                collection,
                name,
                keys,
                unique,
            },
        })
    }

    /// Wraps a source-controlled MongoDB command not represented by the typed
    /// helpers.
    ///
    /// The caller is responsible for making the command idempotent or
    /// resumable. Empty commands are rejected, but command semantics are left
    /// to MongoDB.
    pub fn run_command(command: Document) -> Result<Self, MongoRepositoryError> {
        if command.is_empty() {
            return Err(MongoRepositoryError::InvalidConfiguration(
                "MongoDB migration command must not be empty".into(),
            ));
        }
        Ok(Self {
            kind: MongoMigrationStepKind::RunCommand { command },
        })
    }
}

/// One immutable, positive-versioned MongoDB migration.
///
/// Construction validates the version, name and bounded non-empty step list.
/// Fields remain private so an accepted migration cannot later be mutated into
/// a plan that bypasses those invariants.
#[derive(Debug, Clone, PartialEq)]
pub struct MongoMigration {
    version: i64,
    name: String,
    steps: Vec<MongoMigrationStep>,
}

impl MongoMigration {
    /// Validates and creates a migration.
    ///
    /// Versions must be positive and are required to be strictly increasing in
    /// the vector passed to [`MongoMigrationRunner::new`]. Names contain 1 to
    /// 128 characters and each migration contains 1 to 1,000 steps.
    pub fn new(
        version: i64,
        name: impl Into<String>,
        steps: Vec<MongoMigrationStep>,
    ) -> Result<Self, MongoRepositoryError> {
        let migration = Self {
            version,
            name: name.into(),
            steps,
        };
        validate_migration(&migration)?;
        Ok(migration)
    }

    /// Returns the immutable migration version.
    pub const fn version(&self) -> i64 {
        self.version
    }

    /// Returns the descriptive migration name stored in history.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Borrows the validated migration steps in execution order.
    pub fn steps(&self) -> &[MongoMigrationStep] {
        &self.steps
    }

    fn checksum(&self) -> Result<String, MongoRepositoryError> {
        let mut digest = Sha256::new();
        digest.update(self.version.to_be_bytes());
        digest.update([0]);
        digest.update(self.name.as_bytes());
        for step in &self.steps {
            let document = match &step.kind {
                MongoMigrationStepKind::EnsureCollection { name } => {
                    doc! { "kind": "collection", "name": name }
                }
                MongoMigrationStepKind::EnsureIndex {
                    collection,
                    name,
                    keys,
                    unique,
                } => doc! {
                    "kind": "index",
                    "collection": collection,
                    "name": name,
                    "keys": keys,
                    "unique": unique,
                },
                MongoMigrationStepKind::RunCommand { command } => {
                    doc! { "kind": "command", "command": command }
                }
            };
            digest.update(mongodb::bson::to_vec(&document)?);
        }
        Ok(format!("{:x}", digest.finalize()))
    }
}

/// Outcome of applying a validated migration plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MongoMigrationReport {
    previously_applied: usize,
    newly_applied: Vec<i64>,
}

impl MongoMigrationReport {
    /// Returns the number of matching migrations already present in history.
    pub const fn previously_applied(&self) -> usize {
        self.previously_applied
    }

    /// Returns versions applied by this invocation in ascending order.
    pub fn newly_applied(&self) -> &[i64] {
        &self.newly_applied
    }
}

/// Applies one immutable MongoDB migration plan under a distributed lease.
///
/// The runner records content checksums in `_lily_schema_migrations`, rejects
/// changed or missing history, and serializes concurrent deployers through
/// `_lily_migration_lock`. [`Self::new`] preserves the application-default
/// legacy ledger, while [`Self::for_component`] isolates a framework or
/// application component through a validated compound identity and lease.
/// Lease acquisition and renewal use the MongoDB server clock, so deployer
/// host clock skew cannot steal or indefinitely extend ownership. This
/// requires MongoDB 4.2 or newer. Constructing either runner performs no
/// database I/O.
#[derive(Clone)]
pub struct MongoMigrationRunner {
    database: Database,
    migrations: Arc<[MongoMigration]>,
    namespace: MigrationNamespace,
}

#[derive(Clone)]
enum MigrationNamespace {
    LegacyApplication,
    Component(Arc<str>),
}

impl MigrationNamespace {
    fn lock_id(&self) -> String {
        match self {
            Self::LegacyApplication => "global".to_owned(),
            Self::Component(component) => format!("component:{component}"),
        }
    }

    fn history_filter(&self) -> Document {
        match self {
            Self::LegacyApplication => doc! { "component": { "$exists": false } },
            Self::Component(component) => doc! { "component": component.as_ref() },
        }
    }

    fn apply_history_identity(&self, row: &mut Document, version: i64) {
        match self {
            Self::LegacyApplication => {
                row.insert("_id", version);
            }
            Self::Component(component) => {
                row.insert(
                    "_id",
                    doc! { "component": component.as_ref(), "version": version },
                );
                row.insert("component", component.as_ref());
            }
        }
    }
}

impl MongoMigrationRunner {
    /// Validates a strictly increasing, bounded migration plan for `database`.
    pub fn new(
        database: &DatabaseService,
        migrations: Vec<MongoMigration>,
    ) -> Result<Self, MongoRepositoryError> {
        validate_plan(&migrations)?;
        Ok(Self {
            database: database.raw_database()?,
            migrations: migrations.into(),
            namespace: MigrationNamespace::LegacyApplication,
        })
    }

    /// Validates a component-owned migration namespace and plan.
    ///
    /// Component runners share Lily's migration collections but isolate both
    /// lease ownership and version identity from the application-default
    /// ledger used by [`Self::new`]. Constructing the runner performs no I/O;
    /// callers must still invoke [`Self::apply`] explicitly during deployment.
    pub fn for_component(
        database: &DatabaseService,
        component: impl Into<String>,
        migrations: Vec<MongoMigration>,
    ) -> Result<Self, MongoRepositoryError> {
        validate_plan(&migrations)?;
        let component = validate_component_namespace(component.into())?;
        Ok(Self {
            database: database.raw_database()?,
            migrations: migrations.into(),
            namespace: MigrationNamespace::Component(component.into()),
        })
    }

    /// Applies every pending migration under the caller's operation context.
    ///
    /// A competing deployment may return
    /// `MongoRepositoryError::MigrationLockUnavailable`. Applied versions are
    /// checksummed; editing a previously applied migration fails closed instead
    /// of silently changing history.
    pub async fn apply(
        &self,
        operation: &MongoOperationContext<'_>,
    ) -> Result<MongoMigrationReport, MongoRepositoryError> {
        let owner = uuid::Uuid::new_v4().to_string();
        operation.execute(self.acquire_lock(&owner)).await?;
        let mut heartbeat = MigrationLeaseHeartbeat::start(self.clone(), owner.clone());
        let mut heartbeat_finished = false;
        let mut result = tokio::select! {
            biased;
            heartbeat_result = heartbeat.join() => {
                heartbeat_finished = true;
                Err(heartbeat_result)
            }
            result = operation.execute(self.apply_locked(&owner)) => result,
        };
        if !heartbeat_finished
            && let Err(error) = heartbeat.stop().await
            && result.is_ok()
        {
            result = Err(error);
        }
        let release = operation.execute(self.release_lock(&owner)).await;
        match (result, release) {
            (Ok(report), Ok(())) => Ok(report),
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
        }
    }

    async fn acquire_lock(&self, owner: &str) -> Result<(), MongoRepositoryError> {
        let locks = self.database.collection::<Document>(LOCK_COLLECTION);
        let lock_id = self.lock_id();
        let can_acquire = doc! { "$or": [
            { "$eq": [{ "$type": "$lease_until" }, "missing"] },
            { "$lte": ["$lease_until", "$$NOW"] },
            { "$eq": ["$owner", { "$literal": owner }] },
        ] };
        let claimed = locks
            .find_one_and_update(
                doc! { "_id": &lock_id },
                vec![doc! { "$set": {
                    "owner": { "$cond": [
                        can_acquire.clone(),
                        { "$literal": owner },
                        "$owner",
                    ] },
                    "lease_until": { "$cond": [
                        can_acquire,
                        { "$add": ["$$NOW", LOCK_LEASE_MILLIS] },
                        "$lease_until",
                    ] },
                }}],
            )
            .upsert(true)
            .return_document(ReturnDocument::After)
            .await;
        match claimed {
            Ok(Some(document)) if document.get_str("owner") == Ok(owner) => Ok(()),
            Ok(Some(_)) => Err(MongoRepositoryError::MigrationLockUnavailable),
            Ok(None) => Err(MongoRepositoryError::MigrationLockUnavailable),
            Err(error) if is_duplicate_key(&error) => {
                Err(MongoRepositoryError::MigrationLockUnavailable)
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn renew_lock(&self, owner: &str) -> Result<(), MongoRepositoryError> {
        let lock_id = self.lock_id();
        let result = self
            .database
            .collection::<Document>(LOCK_COLLECTION)
            .update_one(
                doc! {
                    "_id": &lock_id,
                    "owner": owner,
                    "$expr": { "$gt": ["$lease_until", "$$NOW"] },
                },
                vec![doc! { "$set": {
                    "lease_until": { "$add": ["$$NOW", LOCK_LEASE_MILLIS] },
                }}],
            )
            .await?;
        if result.matched_count != 1 {
            return Err(MongoRepositoryError::MigrationLockLost);
        }
        Ok(())
    }

    async fn release_lock(&self, owner: &str) -> Result<(), MongoRepositoryError> {
        let lock_id = self.lock_id();
        let result = self
            .database
            .collection::<Document>(LOCK_COLLECTION)
            .delete_one(doc! { "_id": &lock_id, "owner": owner })
            .await?;
        if result.deleted_count != 1 {
            return Err(MongoRepositoryError::MigrationLockLost);
        }
        Ok(())
    }

    async fn apply_locked(
        &self,
        owner: &str,
    ) -> Result<MongoMigrationReport, MongoRepositoryError> {
        use futures::TryStreamExt;

        let versions = self.database.collection::<Document>(VERSION_COLLECTION);
        let rows = versions
            .find(self.history_filter())
            .limit((MAX_MIGRATIONS + 1) as i64)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        if rows.len() > MAX_MIGRATIONS {
            return Err(MongoRepositoryError::InvalidConfiguration(
                "MongoDB migration history exceeds the supported bound".into(),
            ));
        }
        let applied = rows
            .into_iter()
            .map(|document| {
                let version = document.get_i64("version").map_err(|_| {
                    MongoRepositoryError::InvalidConfiguration(
                        "MongoDB migration history contains an invalid version".into(),
                    )
                })?;
                let checksum = document.get_str("checksum").map_err(|_| {
                    MongoRepositoryError::InvalidConfiguration(
                        "MongoDB migration history contains an invalid checksum".into(),
                    )
                })?;
                Ok((version, checksum.to_owned()))
            })
            .collect::<Result<BTreeMap<_, _>, MongoRepositoryError>>()?;
        let expected = self
            .migrations
            .iter()
            .map(|migration| (migration.version, migration))
            .collect::<BTreeMap<_, _>>();
        for (version, checksum) in &applied {
            let migration = expected
                .get(version)
                .ok_or(MongoRepositoryError::MigrationHistoryDiverged(*version))?;
            if migration.checksum()? != *checksum {
                return Err(MongoRepositoryError::MigrationChecksumMismatch(*version));
            }
        }

        let mut report = MongoMigrationReport {
            previously_applied: applied.len(),
            newly_applied: Vec::new(),
        };
        for migration in self.migrations.iter() {
            if applied.contains_key(&migration.version) {
                continue;
            }
            for step in &migration.steps {
                self.renew_lock(owner).await?;
                self.apply_step(step).await?;
                // A long-running command may outlive the lease despite the
                // heartbeat. Fence the result before advancing to another
                // step or writing immutable migration history.
                self.renew_lock(owner).await?;
            }
            // History is authoritative. A runner that lost ownership must
            // never claim that its migration completed.
            self.renew_lock(owner).await?;
            versions.insert_one(self.history_row(migration)?).await?;
            report.newly_applied.push(migration.version);
        }
        Ok(report)
    }

    fn lock_id(&self) -> String {
        self.namespace.lock_id()
    }

    fn history_filter(&self) -> Document {
        self.namespace.history_filter()
    }

    fn history_row(&self, migration: &MongoMigration) -> Result<Document, MongoRepositoryError> {
        let mut row = doc! {
            "version": migration.version,
            "name": &migration.name,
            "checksum": migration.checksum()?,
            "applied_at": DateTime::now(),
        };
        self.namespace
            .apply_history_identity(&mut row, migration.version);
        Ok(row)
    }

    async fn apply_step(&self, step: &MongoMigrationStep) -> Result<(), MongoRepositoryError> {
        match &step.kind {
            MongoMigrationStepKind::EnsureCollection { name } => {
                let exists = self
                    .database
                    .list_collection_names()
                    .filter(doc! { "name": name })
                    .await?
                    .into_iter()
                    .any(|existing| existing == *name);
                if !exists {
                    self.database.create_collection(name).await?;
                }
            }
            MongoMigrationStepKind::EnsureIndex {
                collection,
                name,
                keys,
                unique,
            } => {
                let options = IndexOptions::builder()
                    .name(name.clone())
                    .unique(*unique)
                    .build();
                self.database
                    .collection::<Document>(collection)
                    .create_index(
                        IndexModel::builder()
                            .keys(keys.clone())
                            .options(options)
                            .build(),
                    )
                    .await?;
            }
            MongoMigrationStepKind::RunCommand { command } => {
                self.database.run_command(command.clone()).await?;
            }
        }
        Ok(())
    }
}

struct MigrationLeaseHeartbeat {
    stop: CancellationToken,
    task: Option<JoinHandle<Result<(), MongoRepositoryError>>>,
}

impl MigrationLeaseHeartbeat {
    fn start(runner: MongoMigrationRunner, owner: String) -> Self {
        let stop = CancellationToken::new();
        let task = spawn_lease_heartbeat(stop.clone(), LOCK_HEARTBEAT_INTERVAL, move || {
            let runner = runner.clone();
            let owner = owner.clone();
            async move { runner.renew_lock(&owner).await }
        });
        Self {
            stop,
            task: Some(task),
        }
    }

    async fn join(&mut self) -> MongoRepositoryError {
        let task = self
            .task
            .as_mut()
            .expect("migration lease heartbeat task must exist");
        match task.await {
            Ok(Err(error)) => error,
            Ok(Ok(())) | Err(_) => MongoRepositoryError::MigrationLockLost,
        }
    }

    async fn stop(mut self) -> Result<(), MongoRepositoryError> {
        self.stop.cancel();
        let task = self
            .task
            .take()
            .expect("migration lease heartbeat task must exist");
        match task.await {
            Ok(result) => result,
            Err(_) => Err(MongoRepositoryError::MigrationLockLost),
        }
    }
}

impl Drop for MigrationLeaseHeartbeat {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

fn spawn_lease_heartbeat<Renew, RenewFuture>(
    stop: CancellationToken,
    interval: Duration,
    mut renew: Renew,
) -> JoinHandle<Result<(), MongoRepositoryError>>
where
    Renew: FnMut() -> RenewFuture + Send + 'static,
    RenewFuture: Future<Output = Result<(), MongoRepositoryError>> + Send + 'static,
{
    tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                _ = stop.cancelled() => return Ok(()),
                _ = tokio::time::sleep(interval) => {}
            }
            tokio::select! {
                biased;
                _ = stop.cancelled() => return Ok(()),
                result = renew() => result?,
            }
        }
    })
}

fn validate_plan(migrations: &[MongoMigration]) -> Result<(), MongoRepositoryError> {
    if migrations.len() > MAX_MIGRATIONS {
        return Err(MongoRepositoryError::InvalidConfiguration(
            "MongoDB migration plan exceeds the supported bound".into(),
        ));
    }
    let mut previous = 0_i64;
    for migration in migrations {
        validate_migration(migration)?;
        if migration.version <= previous {
            return Err(MongoRepositoryError::InvalidConfiguration(
                "MongoDB migration versions must be unique and strictly increasing".into(),
            ));
        }
        previous = migration.version;
    }
    Ok(())
}

fn validate_component_namespace(component: String) -> Result<String, MongoRepositoryError> {
    let valid = !component.is_empty()
        && component.len() <= MAX_COMPONENT_NAMESPACE_LEN
        && component
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_lowercase)
        && component.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"._-".contains(&byte)
        })
        && component
            .as_bytes()
            .last()
            .is_some_and(|byte| !b"._-".contains(byte))
        && !component.contains("..")
        && !component.contains("__")
        && !component.contains("--");
    if !valid {
        return Err(MongoRepositoryError::InvalidConfiguration(
            "MongoDB migration component namespace is invalid".into(),
        ));
    }
    Ok(component)
}

fn validate_migration(migration: &MongoMigration) -> Result<(), MongoRepositoryError> {
    if migration.version <= 0 {
        return Err(MongoRepositoryError::InvalidConfiguration(
            "MongoDB migration version must be positive".into(),
        ));
    }
    if migration.name.trim().is_empty() || migration.name.len() > 128 {
        return Err(MongoRepositoryError::InvalidConfiguration(
            "MongoDB migration name must contain 1 to 128 characters".into(),
        ));
    }
    if migration.steps.is_empty() || migration.steps.len() > MAX_STEPS_PER_MIGRATION {
        return Err(MongoRepositoryError::InvalidConfiguration(
            "MongoDB migration must contain between 1 and 1000 steps".into(),
        ));
    }
    Ok(())
}

fn validate_collection_name(name: &str) -> Result<(), MongoRepositoryError> {
    if name.is_empty()
        || name.len() > 120
        || name.starts_with("system.")
        || name.contains('$')
        || name.contains('\0')
    {
        return Err(MongoRepositoryError::InvalidCollectionName(
            "MongoDB migration collection name is invalid".into(),
        ));
    }
    Ok(())
}

fn valid_field_path(field: &str) -> bool {
    !field.is_empty()
        && !field.starts_with('.')
        && !field.ends_with('.')
        && field.split('.').all(|part| {
            !part.is_empty()
                && !part.starts_with('$')
                && part
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        })
}

fn is_duplicate_key(error: &mongodb::error::Error) -> bool {
    matches!(
        error.kind.as_ref(),
        ErrorKind::Write(WriteFailure::WriteError(error)) if error.code == 11_000
    )
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[test]
    fn migration_plan_is_bounded_ordered_and_content_addressed() {
        let first = MongoMigration::new(
            1,
            "users",
            vec![MongoMigrationStep::ensure_collection("users").unwrap()],
        )
        .unwrap();
        let changed = MongoMigration::new(
            1,
            "users",
            vec![
                MongoMigrationStep::ensure_index(
                    "users",
                    "email_unique",
                    doc! { "email": 1 },
                    true,
                )
                .unwrap(),
            ],
        )
        .unwrap();
        assert_eq!(first.version(), 1);
        assert_eq!(first.name(), "users");
        assert_eq!(first.steps().len(), 1);
        assert_ne!(first.checksum().unwrap(), changed.checksum().unwrap());
        assert!(validate_plan(std::slice::from_ref(&first)).is_ok());
        assert!(validate_plan(&[first.clone(), first]).is_err());
        assert!(MongoMigrationStep::ensure_collection("system.users").is_err());
        assert!(MongoMigrationStep::ensure_index("users", "bad", doc! {}, false).is_err());
    }

    #[test]
    fn component_namespace_is_bounded_and_queue_namespace_is_canonical() {
        assert_eq!(
            validate_component_namespace("lily.queue.transactional_inbox".into()).unwrap(),
            "lily.queue.transactional_inbox"
        );
        for invalid in [
            "",
            "Application",
            ".application",
            "application.",
            "application..queue",
            "application/queue",
        ] {
            assert!(validate_component_namespace(invalid.into()).is_err());
        }
        assert!(validate_component_namespace("a".repeat(MAX_COMPONENT_NAMESPACE_LEN)).is_ok());
        assert!(validate_component_namespace("a".repeat(MAX_COMPONENT_NAMESPACE_LEN + 1)).is_err());

        let legacy = MigrationNamespace::LegacyApplication;
        assert_eq!(legacy.lock_id(), "global");
        assert_eq!(
            legacy.history_filter(),
            doc! { "component": { "$exists": false } }
        );
        let mut legacy_row = Document::new();
        legacy.apply_history_identity(&mut legacy_row, 7);
        assert_eq!(legacy_row, doc! { "_id": 7_i64 });

        let component =
            MigrationNamespace::Component(Arc::<str>::from("lily.queue.transactional_inbox"));
        assert_eq!(
            component.lock_id(),
            "component:lily.queue.transactional_inbox"
        );
        assert_eq!(
            component.history_filter(),
            doc! { "component": "lily.queue.transactional_inbox" }
        );
        let mut component_row = Document::new();
        component.apply_history_identity(&mut component_row, 7);
        assert_eq!(
            component_row,
            doc! {
                "_id": {
                    "component": "lily.queue.transactional_inbox",
                    "version": 7_i64,
                },
                "component": "lily.queue.transactional_inbox",
            }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn lease_heartbeat_renews_while_a_migration_step_is_blocked() {
        let renewals = Arc::new(AtomicUsize::new(0));
        let observed = renewals.clone();
        let stop = CancellationToken::new();
        let task = spawn_lease_heartbeat(stop.clone(), Duration::from_secs(10), move || {
            let observed = observed.clone();
            async move {
                observed.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        });

        tokio::task::yield_now().await;
        for _ in 0..3 {
            tokio::time::advance(Duration::from_secs(10)).await;
            tokio::task::yield_now().await;
        }
        assert_eq!(renewals.load(Ordering::SeqCst), 3);

        stop.cancel();
        assert!(task.await.expect("heartbeat task must join").is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn lease_heartbeat_fails_closed_when_ownership_is_lost() {
        let renewals = Arc::new(AtomicUsize::new(0));
        let observed = renewals.clone();
        let stop = CancellationToken::new();
        let task = spawn_lease_heartbeat(stop, Duration::from_secs(10), move || {
            let observed = observed.clone();
            async move {
                if observed.fetch_add(1, Ordering::SeqCst) == 0 {
                    Ok(())
                } else {
                    Err(MongoRepositoryError::MigrationLockLost)
                }
            }
        });

        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(10)).await;
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(10)).await;
        let error = task
            .await
            .expect("heartbeat task must join")
            .expect_err("lost ownership must terminate the supervisor");
        assert!(matches!(error, MongoRepositoryError::MigrationLockLost));
        assert_eq!(renewals.load(Ordering::SeqCst), 2);
    }
}
