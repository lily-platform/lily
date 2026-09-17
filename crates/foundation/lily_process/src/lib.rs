//! # Lily Process Context
//!
//! Task-local process context management for async multi-request systems.
//! Context follows the future across executor thread migrations and is removed
//! automatically when the scoped future completes, panics or is cancelled.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};

/// Process context for managing request/process-specific state in async systems
#[derive(Debug, Clone)]
pub struct ProcessContext {
    /// Unique request/job identifier for context isolation
    pub process_id: u64,
    /// Additional metadata for context (extensible)
    pub metadata: HashMap<String, String>,
}

// Global atomic counter for generating unique process IDs
static PROCESS_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

impl ProcessContext {
    /// Create a new process context with unique process ID
    pub fn new() -> Self {
        Self {
            process_id: PROCESS_ID_COUNTER.fetch_add(1, Ordering::SeqCst),
            metadata: HashMap::new(),
        }
    }

    /// Create a process context with custom process ID
    pub fn with_process_id(process_id: u64) -> Self {
        Self {
            process_id,
            metadata: HashMap::new(),
        }
    }

    /// Add metadata to the context
    pub fn with_metadata(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value);
        self
    }

    /// Get process ID as string for DI system compatibility
    pub fn process_id_string(&self) -> String {
        self.process_id.to_string()
    }
}

impl Default for ProcessContext {
    fn default() -> Self {
        Self::new()
    }
}

tokio::task_local! {
    /// Context attached to the currently-polled async task.
    static CURRENT_CONTEXT: ProcessContext;
}

/// Context management functions
impl ProcessContext {
    /// Get the context attached to the current async task.
    ///
    /// This accessor is synchronous because reading a Tokio task-local does
    /// not block. It returns `None` outside [`ProcessContext::scope`].
    pub fn current() -> Option<ProcessContext> {
        CURRENT_CONTEXT.try_with(Clone::clone).ok()
    }

    /// Get the current process context for async contexts
    /// This is the primary method used by macro-generated accessors
    pub async fn current_async() -> Option<ProcessContext> {
        Self::current()
    }

    /// Get current process ID as string (for DI system)
    pub fn current_process_id() -> Option<String> {
        Self::current().map(|ctx| ctx.process_id_string())
    }

    /// Poll a future with an isolated context attached to that future.
    ///
    /// Tokio restores an outer context after nested scopes and clears this
    /// value when the future is dropped, including cancellation and panic.
    pub async fn scope<F>(context: ProcessContext, future: F) -> F::Output
    where
        F: Future,
    {
        CURRENT_CONTEXT.scope(context, future).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_process_context_creation() {
        let ctx = ProcessContext::new();
        assert!(ctx.process_id > 0);
        assert!(ctx.metadata.is_empty());
    }

    #[test]
    fn test_process_context_with_metadata() {
        let ctx =
            ProcessContext::new().with_metadata("request_id".to_string(), "req-123".to_string());

        assert_eq!(ctx.metadata.get("request_id"), Some(&"req-123".to_string()));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn task_local_context_survives_yields_and_restores_nested_scope() {
        assert!(ProcessContext::current().is_none());

        ProcessContext::scope(ProcessContext::with_process_id(100), async {
            for _ in 0..100 {
                tokio::task::yield_now().await;
                assert_eq!(ProcessContext::current().unwrap().process_id, 100);
            }

            ProcessContext::scope(ProcessContext::with_process_id(200), async {
                tokio::task::yield_now().await;
                assert_eq!(ProcessContext::current().unwrap().process_id, 200);
            })
            .await;

            assert_eq!(ProcessContext::current().unwrap().process_id, 100);
        })
        .await;

        assert!(ProcessContext::current().is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn one_thousand_concurrent_tasks_never_observe_another_context() {
        let mut tasks = Vec::with_capacity(1_000);
        for process_id in 1..=1_000 {
            tasks.push(tokio::spawn(ProcessContext::scope(
                ProcessContext::with_process_id(process_id),
                async move {
                    for _ in 0..10 {
                        tokio::task::yield_now().await;
                        assert_eq!(ProcessContext::current().unwrap().process_id, process_id);
                    }
                },
            )));
        }

        for task in tasks {
            task.await.unwrap();
        }
        assert!(ProcessContext::current().is_none());
    }
}
