//! Runtime component identity for services hosted inside the same process.

use crate::runtime::TraceCellConfig;
use std::future::Future;
use std::sync::Arc;

/// Resolved identity attached to worker spans and log records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComponentIdentity {
    /// Stable, process-unique component identifier.
    pub id: String,
    /// Rust worker or component type that selected this identity.
    pub worker_type: String,
    /// Low-cardinality configured component name.
    pub worker_name: String,
    /// Low-cardinality component category.
    pub kind: String,
    /// Derived OpenTelemetry service name for this component.
    pub service_name: String,
}

impl ComponentIdentity {
    /// Resolves a runtime identity from one validated trace cell.
    pub fn from_cell(cell: &TraceCellConfig) -> Self {
        Self {
            id: cell.id.clone(),
            worker_type: cell.worker_type.clone(),
            worker_name: cell.worker_name.clone(),
            kind: cell.kind.clone(),
            service_name: format!("{}_{}.{}", cell.id, cell.kind, cell.worker_name),
        }
    }
}

tokio::task_local! {
    static CURRENT_COMPONENT: Arc<ComponentIdentity>;
}

/// Runs a future with an optional component identity.
pub async fn scope_component<F>(component: Option<Arc<ComponentIdentity>>, future: F) -> F::Output
where
    F: Future,
{
    match component {
        Some(component) => CURRENT_COMPONENT.scope(component, future).await,
        None => future.await,
    }
}

/// Returns the component associated with the current async task, if any.
pub fn current_component() -> Option<Arc<ComponentIdentity>> {
    CURRENT_COMPONENT.try_with(Arc::clone).ok()
}

/// Records the current component on a span that declares the Lily component fields.
pub fn record_component_identity(span: &tracing::Span, component: &ComponentIdentity) {
    span.record("lily.component.id", component.id.as_str());
    span.record("lily.component.kind", component.kind.as_str());
    span.record("lily.component.name", component.worker_name.as_str());
    span.record(
        "lily.component.service.name",
        component.service_name.as_str(),
    );
    span.record("lily.worker.type", component.worker_type.as_str());
}

/// Records the task-local component identity on `span`, when one is present.
pub fn record_component(span: &tracing::Span) {
    let Some(component) = current_component() else {
        return;
    };
    record_component_identity(span, &component);
}

/// Records the current component on the currently entered span.
pub fn record_current_component() {
    record_component(&tracing::Span::current());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cell() -> TraceCellConfig {
        TraceCellConfig {
            id: "product-worker-id".to_string(),
            worker_type: "ProductManageWorker".to_string(),
            worker_name: "product".to_string(),
            kind: "manage_worker".to_string(),
        }
    }

    #[test]
    fn builds_component_service_name_from_config() {
        let identity = ComponentIdentity::from_cell(&cell());
        assert_eq!(
            identity.service_name,
            "product-worker-id_manage_worker.product"
        );
    }

    #[tokio::test]
    async fn task_local_component_is_isolated() {
        let first = Arc::new(ComponentIdentity::from_cell(&cell()));
        let mut second_cell = cell();
        second_cell.id = "order-worker-id".to_string();
        second_cell.worker_name = "order".to_string();
        let second = Arc::new(ComponentIdentity::from_cell(&second_cell));

        let (first_id, second_id) = tokio::join!(
            scope_component(Some(first), async {
                tokio::task::yield_now().await;
                current_component().unwrap().id.clone()
            }),
            scope_component(Some(second), async {
                tokio::task::yield_now().await;
                current_component().unwrap().id.clone()
            })
        );

        assert_eq!(first_id, "product-worker-id");
        assert_eq!(second_id, "order-worker-id");
        assert!(current_component().is_none());
    }
}
