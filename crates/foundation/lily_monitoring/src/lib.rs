//! Bounded, application-owned health, readiness, and resource observations.
//!
//! [`HealthRegistry`] stores the latest result of checks registered by an
//! application. It does **not** run dependency probes, create HTTP endpoints,
//! or own provider clients. The application updates checks when its existing
//! services observe state and may expose [`HealthRegistry::snapshot`] through
//! any transport it chooses.
//!
//! Readiness is fail-closed: every [`HealthCriticality::Critical`] check must
//! be [`HealthStatus::Healthy`], and the exact shared
//! [`lily_shutdown::ShutdownState`] must still be ready and accepting work.
//! Public snapshots retain only bounded safe-ASCII names and reason codes;
//! raw provider errors and secrets must stay in private logs or traces.
//!
//! # Example
//!
//! ```
//! use std::sync::Arc;
//! use lily_monitoring::{
//!     HealthCheckKind, HealthCriticality, HealthRegistry, HealthStatus,
//! };
//! use lily_shutdown::ShutdownState;
//!
//! let health = HealthRegistry::new(Arc::new(ShutdownState::new()));
//! health.register(
//!     "postgres.primary",
//!     HealthCheckKind::Dependency,
//!     HealthCriticality::Critical,
//! )?;
//! health.update("postgres.primary", HealthStatus::Healthy, "connected")?;
//! assert!(health.snapshot()?.ready);
//! # Ok::<(), lily_monitoring::HealthRegistryError>(())
//! ```
#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use lily_shutdown::{FrameworkShutdownReport, ShutdownState};
use serde::{Deserialize, Serialize};

const DEFAULT_MAX_CHECKS: usize = 128;
const MAX_CHECK_NAME_BYTES: usize = 96;
const MAX_REASON_CODE_BYTES: usize = 64;

/// A platform-derived resource value. Unsupported signals are explicit; the
/// framework never emits a fabricated zero.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", content = "value", rename_all = "snake_case")]
pub enum ResourceSignal<T> {
    /// The platform exposed a real measured value.
    Available(T),
    /// The platform cannot provide this signal; the string is a stable reason
    /// code rather than a fabricated numeric value.
    Unsupported(String),
}

/// Cheap process resource snapshot used by qualification workloads.
///
/// Linux values come from procfs. Tokio does not expose a stable global task
/// count, so adapters must publish their existing framework-owned task
/// ledgers instead of guessing that value here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessResourceSnapshot {
    /// Sum of process user and system CPU ticks on Linux.
    pub cpu_time_ticks: ResourceSignal<u64>,
    /// Resident memory observed for this process, in bytes.
    pub resident_memory_bytes: ResourceSignal<u64>,
    /// Number of open file descriptors owned by this process.
    pub open_file_descriptors: ResourceSignal<u64>,
    /// Global Tokio task count, currently reported as unsupported because
    /// Tokio has no stable process-wide counter.
    pub global_tokio_tasks: ResourceSignal<u64>,
}

impl ProcessResourceSnapshot {
    /// Captures cheap process signals available on the current platform.
    ///
    /// Linux values are read from procfs. Unsupported or unavailable signals
    /// remain explicit through [`ResourceSignal::Unsupported`].
    pub fn capture() -> Self {
        #[cfg(target_os = "linux")]
        {
            Self {
                cpu_time_ticks: read_linux_cpu_ticks()
                    .map(ResourceSignal::Available)
                    .unwrap_or_else(|| {
                        ResourceSignal::Unsupported("procfs_cpu_unavailable".into())
                    }),
                resident_memory_bytes: read_linux_rss_bytes()
                    .map(ResourceSignal::Available)
                    .unwrap_or_else(|| {
                        ResourceSignal::Unsupported("procfs_rss_unavailable".into())
                    }),
                open_file_descriptors: std::fs::read_dir("/proc/self/fd")
                    .ok()
                    .and_then(|entries| u64::try_from(entries.count()).ok())
                    .map(ResourceSignal::Available)
                    .unwrap_or_else(|| ResourceSignal::Unsupported("procfs_fd_unavailable".into())),
                global_tokio_tasks: ResourceSignal::Unsupported(
                    "tokio_global_task_count_unavailable_use_framework_ledgers".into(),
                ),
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            Self {
                cpu_time_ticks: ResourceSignal::Unsupported("platform_not_supported".into()),
                resident_memory_bytes: ResourceSignal::Unsupported("platform_not_supported".into()),
                open_file_descriptors: ResourceSignal::Unsupported("platform_not_supported".into()),
                global_tokio_tasks: ResourceSignal::Unsupported(
                    "tokio_global_task_count_unavailable_use_framework_ledgers".into(),
                ),
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn read_linux_cpu_ticks() -> Option<u64> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    let after_name = stat.rsplit_once(')')?.1.trim();
    let fields = after_name.split_whitespace().collect::<Vec<_>>();
    // `fields[0]` is process state (procfs field 3); utime/stime are 14/15.
    let user = fields.get(11)?.parse::<u64>().ok()?;
    let system = fields.get(12)?.parse::<u64>().ok()?;
    Some(user.saturating_add(system))
}

#[cfg(target_os = "linux")]
fn read_linux_rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let kib = status.lines().find_map(|line| {
        let value = line.strip_prefix("VmRSS:")?.trim();
        value.split_whitespace().next()?.parse::<u64>().ok()
    })?;
    kib.checked_mul(1024)
}

/// Whether a check participates in aggregate readiness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthCriticality {
    /// The check must be healthy for the application to be ready.
    Critical,
    /// The check remains visible but does not block readiness.
    NonCritical,
}

/// Latest bounded status of a health check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthStatus {
    /// The observed subsystem is operating normally.
    Healthy,
    /// The subsystem is impaired but has not failed.
    Degraded,
    /// The subsystem cannot satisfy its health contract.
    Unhealthy,
}

/// Semantic category used to group a health check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthCheckKind {
    /// External database, broker, cache, or other dependency.
    Dependency,
    /// Bounded application-owned task, scope, or capacity ledger.
    Resource,
    /// Trace, metric, or log export pipeline.
    Telemetry,
    /// Listener, startup, shutdown, or another lifecycle boundary.
    Lifecycle,
}

/// Serializable point-in-time result for one registered check.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HealthCheckSnapshot {
    /// Stable safe-ASCII check name.
    pub name: String,
    /// Semantic category.
    pub kind: HealthCheckKind,
    /// Readiness criticality.
    pub criticality: HealthCriticality,
    /// Latest observed status.
    pub status: HealthStatus,
    /// Stable safe-ASCII reason code without provider error text.
    pub reason_code: String,
    /// Monotonic registry generation at the latest update.
    pub generation: u64,
}

/// Serializable aggregate application health snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HealthSnapshot {
    /// Whether no fatal lifecycle/resource failure has been retained.
    pub live: bool,
    /// Whether shutdown state and all critical checks currently permit
    /// traffic.
    pub ready: bool,
    /// Whether the shared shutdown lifecycle is ready and accepting new work.
    pub accepting_new_work: bool,
    /// Latest monotonic registry generation.
    pub generation: u64,
    /// Registered checks ordered by name.
    pub checks: Vec<HealthCheckSnapshot>,
}

/// Validation, capacity, lookup, or synchronization failure from a health
/// registry operation.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum HealthRegistryError {
    /// Check names must be bounded safe-ASCII tokens.
    #[error("health check name must be 1..={MAX_CHECK_NAME_BYTES} safe ASCII bytes")]
    InvalidName,
    /// Reason codes must be bounded safe-ASCII tokens.
    #[error("health reason code must be 1..={MAX_REASON_CODE_BYTES} safe ASCII bytes")]
    InvalidReasonCode,
    /// A check with the same name was already registered.
    #[error("health check `{0}` is already registered")]
    Duplicate(String),
    /// The configured check capacity has been reached.
    #[error("health registry capacity of {0} checks is exhausted")]
    CapacityExceeded(usize),
    /// The requested check has not been registered.
    #[error("health check `{0}` is not registered")]
    Unknown(String),
    /// An earlier panic poisoned the internal registry lock.
    #[error("health registry lock is poisoned")]
    Poisoned,
}

struct HealthRegistryInner {
    shutdown: Arc<ShutdownState>,
    checks: RwLock<BTreeMap<String, HealthCheckSnapshot>>,
    max_checks: usize,
    generation: AtomicU64,
    fatal: AtomicBool,
}

/// Cloneable health facade tied to the exact application shutdown state.
#[derive(Clone)]
pub struct HealthRegistry(Arc<HealthRegistryInner>);

impl HealthRegistry {
    /// Creates a registry with capacity for 128 checks.
    pub fn new(shutdown: Arc<ShutdownState>) -> Self {
        Self::with_capacity(shutdown, DEFAULT_MAX_CHECKS)
    }

    /// Creates a registry tied to `shutdown` with a bounded check capacity.
    ///
    /// `max_checks` is clamped to the inclusive range `1..=4096`.
    pub fn with_capacity(shutdown: Arc<ShutdownState>, max_checks: usize) -> Self {
        Self(Arc::new(HealthRegistryInner {
            shutdown,
            checks: RwLock::new(BTreeMap::new()),
            max_checks: max_checks.clamp(1, 4_096),
            generation: AtomicU64::new(0),
            fatal: AtomicBool::new(false),
        }))
    }

    /// Registers a uniquely named check in the initial `unhealthy` /
    /// `not_observed` state.
    ///
    /// Names are limited to 96 ASCII alphanumeric, `.`, `_`, or `-` bytes.
    pub fn register(
        &self,
        name: impl Into<String>,
        kind: HealthCheckKind,
        criticality: HealthCriticality,
    ) -> Result<(), HealthRegistryError> {
        let name = name.into();
        validate_token(&name, MAX_CHECK_NAME_BYTES)
            .map_err(|_| HealthRegistryError::InvalidName)?;
        let mut checks = self
            .0
            .checks
            .write()
            .map_err(|_| HealthRegistryError::Poisoned)?;
        if checks.contains_key(&name) {
            return Err(HealthRegistryError::Duplicate(name));
        }
        if checks.len() >= self.0.max_checks {
            return Err(HealthRegistryError::CapacityExceeded(self.0.max_checks));
        }
        let generation = self.next_generation();
        checks.insert(
            name.clone(),
            HealthCheckSnapshot {
                name,
                kind,
                criticality,
                status: HealthStatus::Unhealthy,
                reason_code: "not_observed".to_string(),
                generation,
            },
        );
        Ok(())
    }

    /// Replaces the latest status and stable reason code of a registered
    /// check.
    ///
    /// Reason codes are limited to 64 ASCII alphanumeric, `.`, `_`, or `-`
    /// bytes. Do not pass raw provider errors or configuration values.
    pub fn update(
        &self,
        name: &str,
        status: HealthStatus,
        reason_code: impl Into<String>,
    ) -> Result<(), HealthRegistryError> {
        let reason_code = reason_code.into();
        validate_token(&reason_code, MAX_REASON_CODE_BYTES)
            .map_err(|_| HealthRegistryError::InvalidReasonCode)?;
        let generation = self.next_generation();
        let mut checks = self
            .0
            .checks
            .write()
            .map_err(|_| HealthRegistryError::Poisoned)?;
        let check = checks
            .get_mut(name)
            .ok_or_else(|| HealthRegistryError::Unknown(name.to_string()))?;
        check.status = status;
        check.reason_code = reason_code;
        check.generation = generation;
        Ok(())
    }

    /// Reconciles application-owned task/scope resources. A non-zero failure
    /// count is unhealthy; full capacity is degraded; otherwise the resource
    /// is healthy. Counts are intentionally absent from the public snapshot
    /// and belong in metrics, avoiding a high-cardinality health API.
    pub fn record_resource_ledger(
        &self,
        name: &str,
        in_use: u64,
        capacity: u64,
        failed: u64,
    ) -> Result<(), HealthRegistryError> {
        if capacity == 0 || in_use > capacity || failed != 0 {
            self.update(name, HealthStatus::Unhealthy, "resource_failed")
        } else if in_use == capacity {
            self.update(name, HealthStatus::Degraded, "resource_saturated")
        } else {
            self.update(name, HealthStatus::Healthy, "resource_available")
        }
    }

    /// Verifies the terminal DI/task ledger after drain. Any retained scope or
    /// task makes lifecycle health fail closed.
    pub fn record_terminal_resource_ledger(
        &self,
        name: &str,
        open_scopes: u64,
        active_tasks: u64,
    ) -> Result<(), HealthRegistryError> {
        if open_scopes == 0 && active_tasks == 0 {
            self.update(name, HealthStatus::Healthy, "resources_closed")
        } else {
            self.0.fatal.store(true, Ordering::Release);
            self.update(name, HealthStatus::Unhealthy, "resource_leak")
        }
    }

    /// Records exporter loss without making telemetry a readiness dependency
    /// unless the caller explicitly registered that check as critical.
    pub fn record_exporter_loss(
        &self,
        name: &str,
        dropped: u64,
        rejected: u64,
    ) -> Result<(), HealthRegistryError> {
        if dropped == 0 && rejected == 0 {
            self.update(name, HealthStatus::Healthy, "export_healthy")
        } else {
            self.update(name, HealthStatus::Degraded, "telemetry_loss")
        }
    }

    /// Retains only a stable lifecycle reason code. Component error strings
    /// stay in the private shutdown evidence report and never enter health JSON.
    pub fn record_shutdown_report(&self, report: &FrameworkShutdownReport) {
        if !report.is_terminal_complete() || !report.reconciles() {
            self.0.fatal.store(true, Ordering::Release);
        }
    }

    /// Records a stable lifecycle failure without retaining its provider or
    /// operating-system error text in the public health snapshot.
    pub fn record_lifecycle_failure(
        &self,
        name: &str,
        reason_code: &'static str,
    ) -> Result<(), HealthRegistryError> {
        self.0.fatal.store(true, Ordering::Release);
        self.update(name, HealthStatus::Unhealthy, reason_code)
    }

    /// Returns a point-in-time aggregate snapshot ordered by check name.
    pub fn snapshot(&self) -> Result<HealthSnapshot, HealthRegistryError> {
        let checks = self
            .0
            .checks
            .read()
            .map_err(|_| HealthRegistryError::Poisoned)?
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let accepting_new_work = self.0.shutdown.is_ready()
            && self.0.shutdown.is_accepting_connections()
            && !self.0.shutdown.is_shutdown_initiated();
        let critical_ready = checks.iter().all(|check| {
            check.criticality != HealthCriticality::Critical
                || check.status == HealthStatus::Healthy
        });
        Ok(HealthSnapshot {
            live: !self.0.fatal.load(Ordering::Acquire),
            ready: accepting_new_work && critical_ready,
            accepting_new_work,
            generation: self.0.generation.load(Ordering::Acquire),
            checks,
        })
    }

    fn next_generation(&self) -> u64 {
        self.0.generation.fetch_add(1, Ordering::AcqRel) + 1
    }
}

fn validate_token(value: &str, maximum: usize) -> Result<(), ()> {
    if value.is_empty()
        || value.len() > maximum
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use lily_shutdown::{
        FrameworkShutdownCompletion, FrameworkShutdownMetricSnapshot, ShutdownSignal,
    };
    use std::time::Duration;

    #[test]
    fn critical_dependency_and_shutdown_state_drive_readiness() {
        let shutdown = Arc::new(ShutdownState::new());
        let health = HealthRegistry::new(Arc::clone(&shutdown));
        health
            .register(
                "postgres.primary",
                HealthCheckKind::Dependency,
                HealthCriticality::Critical,
            )
            .unwrap();
        assert!(!health.snapshot().unwrap().ready);

        health
            .update("postgres.primary", HealthStatus::Healthy, "connected")
            .unwrap();
        assert!(health.snapshot().unwrap().ready);

        shutdown.initiate_shutdown(ShutdownSignal::Manual).unwrap();
        assert!(!health.snapshot().unwrap().ready);
        assert!(!health.snapshot().unwrap().accepting_new_work);
    }

    #[test]
    fn telemetry_loss_is_visible_but_noncritical_by_default() {
        let health = HealthRegistry::new(Arc::new(ShutdownState::new()));
        health
            .register(
                "otlp.export",
                HealthCheckKind::Telemetry,
                HealthCriticality::NonCritical,
            )
            .unwrap();
        health.record_exporter_loss("otlp.export", 1, 2).unwrap();
        let snapshot = health.snapshot().unwrap();
        assert!(snapshot.ready);
        assert_eq!(snapshot.checks[0].reason_code, "telemetry_loss");
    }

    #[test]
    fn arbitrary_error_or_secret_text_cannot_enter_public_health_output() {
        let health = HealthRegistry::new(Arc::new(ShutdownState::new()));
        health
            .register(
                "redis.primary",
                HealthCheckKind::Dependency,
                HealthCriticality::Critical,
            )
            .unwrap();
        assert_eq!(
            health.update(
                "redis.primary",
                HealthStatus::Unhealthy,
                "password=secret connection refused"
            ),
            Err(HealthRegistryError::InvalidReasonCode)
        );
        assert!(
            !serde_json::to_string(&health.snapshot().unwrap())
                .unwrap()
                .contains("secret")
        );
    }

    #[test]
    fn failed_lifecycle_report_changes_liveness_without_exposing_error_text() {
        let shutdown = Arc::new(ShutdownState::new());
        let health = HealthRegistry::new(Arc::clone(&shutdown));
        let report = FrameworkShutdownReport {
            signal: ShutdownSignal::Manual,
            forced: false,
            completion: FrameworkShutdownCompletion::Incomplete,
            deadline: Duration::from_secs(30),
            force_reserve: Duration::from_secs(2),
            elapsed: Duration::ZERO,
            phases: Vec::new(),
            metrics: FrameworkShutdownMetricSnapshot {
                failed: 1,
                ..FrameworkShutdownMetricSnapshot::default()
            },
        };
        health.record_shutdown_report(&report);
        assert!(!health.snapshot().unwrap().live);
    }

    #[test]
    fn lifecycle_failure_is_fatal_and_uses_a_stable_reason_code() {
        let health = HealthRegistry::new(Arc::new(ShutdownState::new()));
        health
            .register(
                "http.listener",
                HealthCheckKind::Lifecycle,
                HealthCriticality::Critical,
            )
            .unwrap();
        health
            .update("http.listener", HealthStatus::Healthy, "listening")
            .unwrap();
        assert!(health.snapshot().unwrap().ready);

        health
            .record_lifecycle_failure("http.listener", "listener_failed")
            .unwrap();
        let snapshot = health.snapshot().unwrap();
        assert!(!snapshot.live);
        assert!(!snapshot.ready);
        assert_eq!(snapshot.checks[0].reason_code, "listener_failed");
    }

    #[test]
    fn forced_terminal_completion_remains_live_and_distinct_from_graceful() {
        let shutdown = Arc::new(ShutdownState::new());
        let health = HealthRegistry::new(Arc::clone(&shutdown));
        let report = FrameworkShutdownReport {
            signal: ShutdownSignal::Quit,
            forced: true,
            completion: FrameworkShutdownCompletion::ForcedCompleted,
            deadline: Duration::from_secs(30),
            force_reserve: Duration::from_secs(2),
            elapsed: Duration::ZERO,
            phases: Vec::new(),
            metrics: FrameworkShutdownMetricSnapshot::default(),
        };

        assert!(report.is_terminal_complete());
        assert!(!report.is_graceful());
        assert!(report.reconciles());
        health.record_shutdown_report(&report);
        assert!(health.snapshot().unwrap().live);
    }

    #[test]
    fn dependency_and_resource_ledgers_use_bounded_reason_codes() {
        let health = HealthRegistry::new(Arc::new(ShutdownState::new()));
        health
            .register(
                "queue.broker",
                HealthCheckKind::Dependency,
                HealthCriticality::Critical,
            )
            .unwrap();
        health
            .register(
                "runtime.tasks",
                HealthCheckKind::Resource,
                HealthCriticality::Critical,
            )
            .unwrap();

        health
            .update("queue.broker", HealthStatus::Healthy, "connected")
            .unwrap();
        health
            .record_resource_ledger("runtime.tasks", 8, 8, 0)
            .unwrap();
        assert!(!health.snapshot().unwrap().ready);
        health
            .record_resource_ledger("runtime.tasks", 2, 8, 0)
            .unwrap();
        assert!(health.snapshot().unwrap().ready);

        health
            .record_terminal_resource_ledger("runtime.tasks", 0, 1)
            .unwrap();
        let snapshot = health.snapshot().unwrap();
        assert!(!snapshot.live);
        assert_eq!(snapshot.checks[1].reason_code, "resource_leak");
    }

    #[test]
    fn process_resources_are_real_or_explicitly_unsupported() {
        let snapshot = ProcessResourceSnapshot::capture();
        #[cfg(target_os = "linux")]
        {
            assert!(matches!(
                snapshot.cpu_time_ticks,
                ResourceSignal::Available(_)
            ));
            assert!(matches!(
                snapshot.resident_memory_bytes,
                ResourceSignal::Available(value) if value > 0
            ));
            assert!(matches!(
                snapshot.open_file_descriptors,
                ResourceSignal::Available(value) if value > 0
            ));
        }
        assert!(matches!(
            snapshot.global_tokio_tasks,
            ResourceSignal::Unsupported(_)
        ));
    }
}
