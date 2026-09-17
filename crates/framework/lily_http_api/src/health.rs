use lily_injection::Injectable;
use lily_injection::ServiceTrait;
use lily_monitoring::{HealthCheckKind, HealthCriticality, HealthRegistry, HealthStatus};
pub use lily_monitoring::{HealthRegistryError, HealthSnapshot};
use lily_shutdown::{FrameworkShutdownReport, ShutdownState};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, OnceLock,
};

const HTTP_LISTENER_HEALTH_CHECK: &str = "http.listener";
const HTTP_LISTENER_NOT_STARTED: &str = "not_started";
const HTTP_LISTENER_LISTENING: &str = "listening";
const HTTP_LISTENER_SHUTTING_DOWN: &str = "shutting_down";
const HTTP_LISTENER_BIND_FAILED: &str = "bind_failed";
const HTTP_LISTENER_FAILED: &str = "listener_failed";

/// Read-only HTTP lifecycle state available through the application DI graph.
///
/// The framework owns every mutation. Applications may resolve this singleton
/// from `Extensions` and expose any endpoint shape they choose.
#[derive(Injectable)]
#[service(lifetime = "Singleton")]
pub struct HttpHealthService {
    shutdown_state: Arc<ShutdownState>,
    registry: HealthRegistry,
    attached: AtomicBool,
    shutdown_observer: OnceLock<Box<dyn Fn() + Send + Sync>>,
    shutdown_report: OnceLock<crate::shutdown_report::HttpShutdownReport>,
}

impl Default for HttpHealthService {
    fn default() -> Self {
        let shutdown_state = Arc::new(ShutdownState::new_not_ready());
        Self {
            registry: HealthRegistry::new(Arc::clone(&shutdown_state)),
            shutdown_state,
            attached: AtomicBool::new(false),
            shutdown_observer: OnceLock::new(),
            shutdown_report: OnceLock::new(),
        }
    }
}

impl ServiceTrait for HttpHealthService {}

impl HttpHealthService {
    /// Returns the current bounded HTTP lifecycle snapshot.
    pub fn snapshot(&self) -> Result<HealthSnapshot, HealthRegistryError> {
        if let Some(observer) = self.shutdown_observer.get() {
            observer();
        }
        self.registry.snapshot()
    }

    pub(crate) fn observe_shutdown_with(&self, observer: impl Fn() + Send + Sync + 'static) {
        let _ = self.shutdown_observer.set(Box::new(observer));
    }

    pub(crate) fn attach_http_lifecycle(
        &self,
        telemetry_owned: bool,
    ) -> Result<(), HttpHealthAttachmentError> {
        self.attached
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| HttpHealthAttachmentError::AlreadyAttached)?;

        self.registry.register(
            HTTP_LISTENER_HEALTH_CHECK,
            HealthCheckKind::Lifecycle,
            HealthCriticality::Critical,
        )?;
        self.registry.update(
            HTTP_LISTENER_HEALTH_CHECK,
            HealthStatus::Unhealthy,
            HTTP_LISTENER_NOT_STARTED,
        )?;
        self.registry.register(
            "di.container",
            HealthCheckKind::Lifecycle,
            HealthCriticality::Critical,
        )?;
        self.registry
            .update("di.container", HealthStatus::Healthy, "initialized")?;
        self.registry.register(
            "http.shutdown",
            HealthCheckKind::Lifecycle,
            HealthCriticality::Critical,
        )?;
        self.registry
            .update("http.shutdown", HealthStatus::Healthy, "not_observed")?;
        if telemetry_owned {
            self.registry.register(
                "telemetry.export",
                HealthCheckKind::Telemetry,
                HealthCriticality::NonCritical,
            )?;
            self.registry
                .update("telemetry.export", HealthStatus::Healthy, "export_healthy")?;
        }
        Ok(())
    }

    pub(crate) fn shutdown_state(&self) -> Arc<ShutdownState> {
        Arc::clone(&self.shutdown_state)
    }

    pub(crate) fn record_listener_listening(&self) -> Result<(), HealthRegistryError> {
        self.registry.update(
            HTTP_LISTENER_HEALTH_CHECK,
            HealthStatus::Healthy,
            HTTP_LISTENER_LISTENING,
        )
    }

    pub(crate) fn record_listener_shutting_down(&self) -> Result<(), HealthRegistryError> {
        self.registry.update(
            HTTP_LISTENER_HEALTH_CHECK,
            HealthStatus::Unhealthy,
            HTTP_LISTENER_SHUTTING_DOWN,
        )
    }

    pub(crate) fn record_listener_bind_failure(&self) -> Result<(), HealthRegistryError> {
        self.registry
            .record_lifecycle_failure(HTTP_LISTENER_HEALTH_CHECK, HTTP_LISTENER_BIND_FAILED)
    }

    pub(crate) fn record_listener_failure(&self) -> Result<(), HealthRegistryError> {
        self.registry
            .record_lifecycle_failure(HTTP_LISTENER_HEALTH_CHECK, HTTP_LISTENER_FAILED)
    }

    pub(crate) fn record_exporter_loss(
        &self,
        dropped: u64,
        rejected: u64,
    ) -> Result<(), HealthRegistryError> {
        self.registry
            .record_exporter_loss("telemetry.export", dropped, rejected)
    }

    pub(crate) fn record_shutdown_report(&self, report: &FrameworkShutdownReport) {
        self.registry.record_shutdown_report(report);
    }

    pub(crate) fn record_http_shutdown(&self, report: &crate::shutdown_report::HttpShutdownReport) {
        // Immutable attempt publication, including health generation. Replay
        // and late actual joins cannot upgrade an earlier failure.
        if self.shutdown_report.set(*report).is_err() {
            return;
        }
        use crate::shutdown_report::DependencyDisposition as D;
        let di = report.dependencies.di;
        let (status, reason) = if di.disposition == D::NotOwned {
            (HealthStatus::Healthy, "caller_owned")
        } else if !di.terminal {
            (HealthStatus::Unhealthy, "termination_unconfirmed")
        } else if di.succeeded() {
            (HealthStatus::Healthy, "disposed")
        } else {
            (HealthStatus::Unhealthy, "disposal_failed")
        };
        let _ = self.registry.update("di.container", status, reason);
        let telemetry = report.dependencies.telemetry;
        if telemetry.dependency.disposition != D::NotOwned {
            let (status, reason) = if !telemetry.dependency.terminal {
                (HealthStatus::Degraded, "termination_unconfirmed")
            } else if !telemetry.dependency.succeeded() || telemetry.workers.failed != 0 {
                (HealthStatus::Degraded, "shutdown_failed")
            } else if telemetry.dropped != 0 || telemetry.rejected != 0 {
                (HealthStatus::Degraded, "telemetry_loss")
            } else {
                (HealthStatus::Healthy, "shutdown_completed")
            };
            let _ = self.registry.update("telemetry.export", status, reason);
        }
        if report.succeeded() {
            let _ = self.registry.update(
                "http.shutdown",
                HealthStatus::Healthy,
                report.completion.reason(),
            );
        } else {
            let _ = self
                .registry
                .record_lifecycle_failure("http.shutdown", report.completion.reason());
        }
        // Owned telemetry has already closed; don't create new exporter loss
        // to announce its own completion. Its last event was preliminary.
        if report.dependencies.telemetry.dependency.disposition
            == crate::shutdown_report::DependencyDisposition::NotOwned
        {
            report.emit("attempt_frozen");
        }
    }
}

#[derive(Debug)]
pub(crate) enum HttpHealthAttachmentError {
    AlreadyAttached,
    Registry(HealthRegistryError),
}

impl std::fmt::Display for HttpHealthAttachmentError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyAttached => formatter
                .write_str("the application container is already attached to an HTTP lifecycle"),
            Self::Registry(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for HttpHealthAttachmentError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::AlreadyAttached => None,
            Self::Registry(error) => Some(error),
        }
    }
}

impl From<HealthRegistryError> for HttpHealthAttachmentError {
    fn from(error: HealthRegistryError) -> Self {
        Self::Registry(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attachment_initializes_one_fail_closed_http_snapshot() {
        let health = HttpHealthService::default();
        assert!(!health.snapshot().unwrap().ready);

        health.attach_http_lifecycle(false).unwrap();
        let snapshot = health.snapshot().unwrap();
        assert!(snapshot.live);
        assert!(!snapshot.ready);
        assert_eq!(snapshot.checks.len(), 3);
        assert!(matches!(
            health.attach_http_lifecycle(false),
            Err(HttpHealthAttachmentError::AlreadyAttached)
        ));
    }

    #[test]
    fn framework_updates_are_visible_through_the_service_snapshot() {
        let health = HttpHealthService::default();
        health.attach_http_lifecycle(false).unwrap();

        health.record_listener_listening().unwrap();
        health.shutdown_state().publish_ready().unwrap();
        assert!(health.snapshot().unwrap().ready);

        health.record_listener_failure().unwrap();
        let snapshot = health.snapshot().unwrap();
        assert!(!snapshot.live);
        assert!(!snapshot.ready);
        assert_eq!(snapshot.checks[1].reason_code, HTTP_LISTENER_FAILED);
    }
}
