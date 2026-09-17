use async_trait::async_trait;
use lily_middleware::__private::HttpNextService;
use lily_middleware::{
    HttpExchange, HttpMiddleware, HttpMiddlewareError, HttpMiddlewareFailureKind, HttpNext,
    MiddlewareDescriptor, MiddlewareErrorCode,
};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Bounded outcome emitted once for every entered middleware frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HttpMiddlewareObservationOutcome {
    Completed,
    Rejected,
    Timeout,
    Internal,
    Aborted,
}

impl HttpMiddlewareObservationOutcome {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Rejected => "rejected",
            Self::Timeout => "timeout",
            Self::Internal => "internal",
            Self::Aborted => "aborted",
        }
    }

    fn from_error(error: &HttpMiddlewareError) -> Self {
        match error.kind() {
            HttpMiddlewareFailureKind::Rejected => Self::Rejected,
            HttpMiddlewareFailureKind::Timeout => Self::Timeout,
            // `HttpMiddlewareFailureKind` is non-exhaustive. Every current or
            // future framework-owned failure outside the two public decision
            // categories is deliberately normalized to the bounded internal
            // outcome.
            _ => Self::Internal,
        }
    }
}

/// Adapter-owned observability seam. Implementations receive only a validated
/// static descriptor and a framework-owned bounded outcome.
pub(crate) trait HttpMiddlewareObserver: Send + Sync {
    fn observe(
        &self,
        descriptor: MiddlewareDescriptor,
        outcome: HttpMiddlewareObservationOutcome,
        duration: Duration,
    );
}

#[cfg(any(test, feature = "fuzzing"))]
pub(crate) struct NoopHttpMiddlewareObserver;

#[cfg(any(test, feature = "fuzzing"))]
impl HttpMiddlewareObserver for NoopHttpMiddlewareObserver {
    fn observe(
        &self,
        _descriptor: MiddlewareDescriptor,
        _outcome: HttpMiddlewareObservationOutcome,
        _duration: Duration,
    ) {
    }
}

/// Drop-aware frame observation. If a request deadline, task abort or unwind
/// drops a pending middleware future, the entered frame still emits one
/// bounded `aborted` outcome and retains no request data.
struct HttpMiddlewareObservationGuard<'observer> {
    observer: &'observer dyn HttpMiddlewareObserver,
    descriptor: MiddlewareDescriptor,
    started: Instant,
    finished: bool,
}

impl<'observer> HttpMiddlewareObservationGuard<'observer> {
    fn new(
        observer: &'observer dyn HttpMiddlewareObserver,
        descriptor: MiddlewareDescriptor,
    ) -> Self {
        Self {
            observer,
            descriptor,
            started: Instant::now(),
            finished: false,
        }
    }

    fn finish(mut self, outcome: HttpMiddlewareObservationOutcome) {
        self.finished = true;
        self.observer
            .observe(self.descriptor, outcome, self.started.elapsed());
    }
}

impl Drop for HttpMiddlewareObservationGuard<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.observer.observe(
                self.descriptor,
                HttpMiddlewareObservationOutcome::Aborted,
                self.started.elapsed(),
            );
        }
    }
}

/// Bounded stack-owned evidence that a typed middleware failure was converted
/// into a safe response by the innermost executor boundary.
///
/// The private type prevents user middleware from forging or consuming the
/// framework's telemetry state. It intentionally retains no dynamic source or
/// public response text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MaterializedMiddlewareFailure {
    origin: &'static str,
    code: MiddlewareErrorCode,
    status: u16,
}

impl MaterializedMiddlewareFailure {
    fn from_error(origin: &'static str, error: &HttpMiddlewareError) -> Self {
        Self {
            origin,
            code: error.diagnostic_code(),
            status: error.http_status(),
        }
    }

    pub(crate) const fn origin(self) -> &'static str {
        self.origin
    }

    pub(crate) const fn code(self) -> MiddlewareErrorCode {
        self.code
    }

    pub(crate) const fn status(self) -> u16 {
        self.status
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MiddlewareChainOutcome {
    None,
    Materialized(MaterializedMiddlewareFailure),
    WriterFailed(MaterializedMiddlewareFailure),
}

/// Private stack-owned outcome slot shared by recursive executor nodes.
///
/// Unlike request extensions, user middleware cannot name, clear or forge
/// this state. The lock is held only for a bounded copy/replace operation and
/// never across middleware or response-writer code.
#[derive(Debug)]
pub(crate) struct MiddlewareChainOutcomeSlot(Mutex<MiddlewareChainOutcome>);

impl Default for MiddlewareChainOutcomeSlot {
    fn default() -> Self {
        Self(Mutex::new(MiddlewareChainOutcome::None))
    }
}

impl MiddlewareChainOutcomeSlot {
    fn record_materialized(&self, failure: MaterializedMiddlewareFailure) {
        *self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) =
            MiddlewareChainOutcome::Materialized(failure);
    }

    fn record_writer_failure(&self, error: &HttpMiddlewareError) {
        *self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) =
            MiddlewareChainOutcome::WriterFailed(MaterializedMiddlewareFailure::from_error(
                "http_error_writer",
                error,
            ));
    }

    fn writer_failed(&self) -> bool {
        matches!(
            *self
                .0
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            MiddlewareChainOutcome::WriterFailed(_)
        )
    }

    fn response_materialized(&self) -> bool {
        matches!(
            *self
                .0
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            MiddlewareChainOutcome::Materialized(_)
        )
    }

    pub(crate) fn take(&self) -> MiddlewareChainOutcome {
        std::mem::replace(
            &mut *self
                .0
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            MiddlewareChainOutcome::None,
        )
    }
}

/// Middleware and its descriptor frozen together during application build.
#[derive(Clone)]
pub(crate) struct CompiledHttpMiddleware {
    middleware: Arc<dyn HttpMiddleware>,
    descriptor: MiddlewareDescriptor,
}

/// Adapter-owned safe response materializer for typed chain failures.
#[async_trait]
pub(crate) trait HttpMiddlewareErrorWriter: Send + Sync {
    async fn write_error_response(
        &self,
        exchange: &mut HttpExchange<'_>,
        error: &HttpMiddlewareError,
    ) -> Result<(), HttpMiddlewareError>;
}

impl CompiledHttpMiddleware {
    pub(crate) fn new(
        middleware: Arc<dyn HttpMiddleware>,
        descriptor: MiddlewareDescriptor,
    ) -> Self {
        Self {
            middleware,
            descriptor,
        }
    }

    pub(crate) const fn descriptor(&self) -> MiddlewareDescriptor {
        self.descriptor
    }
}

/// Borrow-only view of the immutable application middleware chain.
///
/// The application bypasses this type entirely when the chain is empty. A
/// configured chain borrows the compiled middleware list. Its request owner
/// retains distinct invocation state/evidence outside these stack values.
pub(crate) struct HttpMiddlewareChain<'chain> {
    middlewares: &'chain [CompiledHttpMiddleware],
    terminal: &'chain dyn HttpNextService,
    error_writer: &'chain dyn HttpMiddlewareErrorWriter,
    outcome_slot: &'chain MiddlewareChainOutcomeSlot,
    observer: &'chain dyn HttpMiddlewareObserver,
}

impl<'chain> HttpMiddlewareChain<'chain> {
    pub(crate) const fn new(
        middlewares: &'chain [CompiledHttpMiddleware],
        terminal: &'chain dyn HttpNextService,
        error_writer: &'chain dyn HttpMiddlewareErrorWriter,
        outcome_slot: &'chain MiddlewareChainOutcomeSlot,
        observer: &'chain dyn HttpMiddlewareObserver,
    ) -> Self {
        Self {
            middlewares,
            terminal,
            error_writer,
            outcome_slot,
            observer,
        }
    }

    async fn materialize_error(
        &self,
        exchange: &mut HttpExchange<'_>,
        origin: &'static str,
        error: HttpMiddlewareError,
    ) -> Result<(), HttpMiddlewareError> {
        if self
            .error_writer
            .write_error_response(exchange, &error)
            .await
            .is_err()
        {
            let bounded_writer_error = HttpMiddlewareError::internal(MiddlewareErrorCode::INTERNAL);
            self.outcome_slot
                .record_writer_failure(&bounded_writer_error);
            return Err(bounded_writer_error);
        }

        self.outcome_slot
            .record_materialized(MaterializedMiddlewareFailure::from_error(origin, &error));
        Ok(())
    }
}

#[async_trait]
impl HttpNextService for HttpMiddlewareChain<'_> {
    async fn run(&self, exchange: &mut HttpExchange<'_>) -> Result<(), HttpMiddlewareError> {
        let Some((middleware, remaining)) = self.middlewares.split_first() else {
            return match self.terminal.run(exchange).await {
                Ok(()) => Ok(()),
                Err(error) => {
                    self.materialize_error(exchange, "http_terminal", error)
                        .await
                }
            };
        };

        let next = Self::new(
            remaining,
            self.terminal,
            self.error_writer,
            self.outcome_slot,
            self.observer,
        );
        let invocation = crate::request_lifecycle::middleware::MiddlewareLedger::register_current(
            middleware.middleware.clone(),
            middleware.descriptor,
        );
        let tracked_next = InvocationNext {
            next: &next,
            invocation: invocation.as_deref(),
        };
        let observation = HttpMiddlewareObservationGuard::new(self.observer, middleware.descriptor);
        let cancellation = exchange.execution_cancellation();
        let result = if let Some(invocation) = &invocation {
            let mut state = invocation.state.lock().await;
            let (request, response) = exchange.parts_mut();
            let mut exchange = HttpExchange::with_termination_state(
                request,
                response,
                &mut state,
                invocation.id(),
            );
            // This is inside the callback's first poll, before any user code.
            // Construction of an unpolled chain/next never reaches this point.
            invocation.enter();
            let result = middleware
                .middleware
                .handle(&mut exchange, HttpNext::new(&tracked_next), cancellation)
                .await;
            invocation.normal_returned(); // Err is also a normal return.
            result
        } else {
            middleware
                .middleware
                .handle(exchange, HttpNext::new(&tracked_next), cancellation)
                .await
        };
        let observation_outcome = match &result {
            Ok(()) => HttpMiddlewareObservationOutcome::Completed,
            Err(error) => HttpMiddlewareObservationOutcome::from_error(error),
        };
        observation.finish(observation_outcome);

        match result {
            Ok(()) => Ok(()),
            Err(error) if self.outcome_slot.writer_failed() => Err(error),
            // The innermost failure owns the response. A secondary failure
            // raised while an outer middleware unwinds must not invoke the
            // adapter writer again or replace the first failure's telemetry.
            Err(_) if self.outcome_slot.response_materialized() => Ok(()),
            Err(error) => {
                self.materialize_error(exchange, middleware.descriptor.name(), error)
                    .await
            }
        }
    }
}

struct InvocationNext<'a> {
    next: &'a dyn HttpNextService,
    invocation: Option<&'a crate::request_lifecycle::middleware::Invocation>,
}

#[async_trait]
impl HttpNextService for InvocationNext<'_> {
    async fn run(&self, exchange: &mut HttpExchange<'_>) -> Result<(), HttpMiddlewareError> {
        if let Some(invocation) = self.invocation {
            invocation.stage(lily_middleware::HttpMiddlewareStage::DelegatingToNext);
        }
        let result = self.next.run(exchange).await;
        if let Some(invocation) = self.invocation {
            invocation.stage(lily_middleware::HttpMiddlewareStage::After);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lily_injection::Extensions;
    use lily_middleware::{
        HttpMiddlewareInitError, MiddlewareDescriptor, MiddlewareErrorCode, MiddlewareKind,
    };
    use lily_web_core::{Request, Response};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Phase {
        Before,
        Handler,
        After,
        ShortCircuit,
    }

    type Event = (&'static str, Phase);
    type Observation = (&'static str, HttpMiddlewareObservationOutcome);

    #[derive(Default)]
    struct RecordingObserver {
        observations: Mutex<Vec<Observation>>,
    }

    impl HttpMiddlewareObserver for RecordingObserver {
        fn observe(
            &self,
            descriptor: MiddlewareDescriptor,
            outcome: HttpMiddlewareObservationOutcome,
            _duration: Duration,
        ) {
            self.observations
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push((descriptor.name(), outcome));
        }
    }

    #[test]
    fn observation_outcome_labels_and_error_mapping_are_stable() {
        assert_eq!(
            [
                HttpMiddlewareObservationOutcome::Completed.as_str(),
                HttpMiddlewareObservationOutcome::Rejected.as_str(),
                HttpMiddlewareObservationOutcome::Timeout.as_str(),
                HttpMiddlewareObservationOutcome::Internal.as_str(),
                HttpMiddlewareObservationOutcome::Aborted.as_str(),
            ],
            ["completed", "rejected", "timeout", "internal", "aborted"]
        );

        let rejected: HttpMiddlewareError = lily_middleware::HttpMiddlewareRejection::new(
            403,
            MiddlewareErrorCode::new("TEST_MIDDLEWARE_REJECTED").unwrap(),
        )
        .unwrap()
        .into();
        let timed_out = HttpMiddlewareError::timeout(MiddlewareErrorCode::TIMEOUT);
        let internal = HttpMiddlewareError::internal(MiddlewareErrorCode::INTERNAL);

        assert_eq!(
            HttpMiddlewareObservationOutcome::from_error(&rejected),
            HttpMiddlewareObservationOutcome::Rejected
        );
        assert_eq!(
            HttpMiddlewareObservationOutcome::from_error(&timed_out),
            HttpMiddlewareObservationOutcome::Timeout
        );
        assert_eq!(
            HttpMiddlewareObservationOutcome::from_error(&internal),
            HttpMiddlewareObservationOutcome::Internal
        );
    }

    struct Terminal {
        events: Arc<Mutex<Vec<Event>>>,
    }

    struct ErrorWriter;

    #[async_trait]
    impl HttpMiddlewareErrorWriter for ErrorWriter {
        async fn write_error_response(
            &self,
            exchange: &mut HttpExchange<'_>,
            error: &HttpMiddlewareError,
        ) -> Result<(), HttpMiddlewareError> {
            exchange
                .response_mut()
                .status_code(usize::from(error.http_status()), "Error");
            Ok(())
        }
    }

    struct CountingErrorWriter(Arc<AtomicUsize>);

    #[async_trait]
    impl HttpMiddlewareErrorWriter for CountingErrorWriter {
        async fn write_error_response(
            &self,
            exchange: &mut HttpExchange<'_>,
            error: &HttpMiddlewareError,
        ) -> Result<(), HttpMiddlewareError> {
            self.0.fetch_add(1, Ordering::AcqRel);
            exchange
                .response_mut()
                .status_code(usize::from(error.http_status()), "Error");
            Ok(())
        }
    }

    #[async_trait]
    impl HttpNextService for Terminal {
        async fn run(&self, _exchange: &mut HttpExchange<'_>) -> Result<(), HttpMiddlewareError> {
            self.events
                .lock()
                .unwrap()
                .push(("terminal", Phase::Handler));
            Ok(())
        }
    }

    #[derive(Clone, Copy)]
    enum Behavior {
        Around,
        AroundAndClearRequestLocal,
        ShortCircuit,
        Rejection,
        Timeout,
        BeforeError,
        ErrorAfterSuccessResponse,
        AfterError,
    }

    struct RecordingMiddleware {
        name: &'static str,
        behavior: Behavior,
        events: Arc<Mutex<Vec<Event>>>,
    }

    #[async_trait]
    impl HttpMiddleware for RecordingMiddleware {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, HttpMiddlewareInitError> {
            Ok(Self {
                name: "recording",
                behavior: Behavior::Around,
                events: Arc::new(Mutex::new(Vec::new())),
            })
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            MiddlewareDescriptor::new(self.name, MiddlewareKind::Custom)
        }

        async fn handle(
            &self,
            exchange: &mut HttpExchange<'_>,
            next: HttpNext<'_>,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), HttpMiddlewareError> {
            self.events.lock().unwrap().push((self.name, Phase::Before));
            let test_error = || {
                HttpMiddlewareError::internal(
                    MiddlewareErrorCode::new("TEST_MIDDLEWARE_FAILURE").unwrap(),
                )
            };

            match self.behavior {
                Behavior::Around => {
                    next.run(exchange).await?;
                    self.events.lock().unwrap().push((self.name, Phase::After));
                    Ok(())
                }
                Behavior::AroundAndClearRequestLocal => {
                    next.run(exchange).await?;
                    exchange.request_mut().local_mut().clear();
                    self.events.lock().unwrap().push((self.name, Phase::After));
                    Ok(())
                }
                Behavior::ShortCircuit => {
                    exchange.response_mut().status_code(204, "No Content");
                    self.events
                        .lock()
                        .unwrap()
                        .push((self.name, Phase::ShortCircuit));
                    Ok(())
                }
                Behavior::Rejection => Err(lily_middleware::HttpMiddlewareRejection::new(
                    403,
                    MiddlewareErrorCode::new("TEST_MIDDLEWARE_REJECTED").unwrap(),
                )
                .unwrap()
                .into()),
                Behavior::Timeout => {
                    Err(HttpMiddlewareError::timeout(MiddlewareErrorCode::TIMEOUT))
                }
                Behavior::BeforeError => Err(test_error()),
                Behavior::ErrorAfterSuccessResponse => {
                    exchange.response_mut().status_code(200, "OK");
                    Err(test_error())
                }
                Behavior::AfterError => {
                    next.run(exchange).await?;
                    self.events.lock().unwrap().push((self.name, Phase::After));
                    Err(test_error())
                }
            }
        }
    }

    fn middleware(
        name: &'static str,
        behavior: Behavior,
        events: &Arc<Mutex<Vec<Event>>>,
    ) -> CompiledHttpMiddleware {
        let middleware: Arc<dyn HttpMiddleware> = Arc::new(RecordingMiddleware {
            name,
            behavior,
            events: Arc::clone(events),
        });
        CompiledHttpMiddleware::new(Arc::clone(&middleware), middleware.descriptor())
    }

    async fn exchange_parts() -> (Request, Response) {
        let request =
            Request::from_transport_parts("GET".to_string(), "/".to_string(), Vec::new(), &[])
                .await
                .unwrap();
        let response = Response::new().await.unwrap();
        (request, response)
    }

    #[tokio::test]
    async fn enters_forward_and_unwinds_in_reverse_order() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let middlewares = [
            middleware("a", Behavior::Around, &events),
            middleware("b", Behavior::Around, &events),
        ];
        let terminal = Terminal {
            events: Arc::clone(&events),
        };
        let (mut request, mut response) = exchange_parts().await;
        let mut exchange = HttpExchange::new(&mut request, &mut response);
        let outcome = MiddlewareChainOutcomeSlot::default();

        HttpMiddlewareChain::new(
            &middlewares,
            &terminal,
            &ErrorWriter,
            &outcome,
            &NoopHttpMiddlewareObserver,
        )
        .run(&mut exchange)
        .await
        .unwrap();
        assert_eq!(outcome.take(), MiddlewareChainOutcome::None);

        assert_eq!(
            *events.lock().unwrap(),
            [
                ("a", Phase::Before),
                ("b", Phase::Before),
                ("terminal", Phase::Handler),
                ("b", Phase::After),
                ("a", Phase::After),
            ]
        );
    }

    #[tokio::test]
    async fn short_circuit_enters_only_the_prefix_and_still_unwinds_outer_layers() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let middlewares = [
            middleware("a", Behavior::Around, &events),
            middleware("b", Behavior::ShortCircuit, &events),
            middleware("c", Behavior::Around, &events),
        ];
        let terminal = Terminal {
            events: Arc::clone(&events),
        };
        let (mut request, mut response) = exchange_parts().await;
        let mut exchange = HttpExchange::new(&mut request, &mut response);
        let outcome = MiddlewareChainOutcomeSlot::default();

        HttpMiddlewareChain::new(
            &middlewares,
            &terminal,
            &ErrorWriter,
            &outcome,
            &NoopHttpMiddlewareObserver,
        )
        .run(&mut exchange)
        .await
        .unwrap();
        assert_eq!(outcome.take(), MiddlewareChainOutcome::None);

        assert_eq!(exchange.response().status_code_value(), 204);
        assert_eq!(
            *events.lock().unwrap(),
            [
                ("a", Phase::Before),
                ("b", Phase::Before),
                ("b", Phase::ShortCircuit),
                ("a", Phase::After),
            ]
        );
    }

    #[tokio::test]
    async fn typed_failures_materialize_once_then_natural_question_mark_unwinds_outer_layers() {
        for (behavior, expected_status, expected_code, expected_events) in [
            (
                Behavior::Rejection,
                403,
                "TEST_MIDDLEWARE_REJECTED",
                vec![
                    ("a", Phase::Before),
                    ("b", Phase::Before),
                    ("a", Phase::After),
                ],
            ),
            (
                Behavior::BeforeError,
                500,
                "TEST_MIDDLEWARE_FAILURE",
                vec![
                    ("a", Phase::Before),
                    ("b", Phase::Before),
                    ("a", Phase::After),
                ],
            ),
            (
                Behavior::ErrorAfterSuccessResponse,
                500,
                "TEST_MIDDLEWARE_FAILURE",
                vec![
                    ("a", Phase::Before),
                    ("b", Phase::Before),
                    ("a", Phase::After),
                ],
            ),
            (
                Behavior::AfterError,
                500,
                "TEST_MIDDLEWARE_FAILURE",
                vec![
                    ("a", Phase::Before),
                    ("b", Phase::Before),
                    ("terminal", Phase::Handler),
                    ("b", Phase::After),
                    ("a", Phase::After),
                ],
            ),
        ] {
            let events = Arc::new(Mutex::new(Vec::new()));
            let middlewares = [
                middleware("a", Behavior::Around, &events),
                middleware("b", behavior, &events),
            ];
            let terminal = Terminal {
                events: Arc::clone(&events),
            };
            let (mut request, mut response) = exchange_parts().await;
            let mut exchange = HttpExchange::new(&mut request, &mut response);
            let outcome = MiddlewareChainOutcomeSlot::default();

            HttpMiddlewareChain::new(
                &middlewares,
                &terminal,
                &ErrorWriter,
                &outcome,
                &NoopHttpMiddlewareObserver,
            )
            .run(&mut exchange)
            .await
            .unwrap();

            let MiddlewareChainOutcome::Materialized(failure) = outcome.take() else {
                panic!("a materialized failure must retain bounded telemetry");
            };
            assert_eq!(failure.origin(), "b");
            assert_eq!(failure.code().as_str(), expected_code);
            assert_eq!(failure.status(), expected_status);
            assert_eq!(exchange.response().status_code_value(), expected_status);
            assert_eq!(*events.lock().unwrap(), expected_events);
        }
    }

    #[tokio::test]
    async fn observations_use_only_static_names_and_bounded_outcomes() {
        for (behavior, expected) in [
            (
                Behavior::Rejection,
                HttpMiddlewareObservationOutcome::Rejected,
            ),
            (Behavior::Timeout, HttpMiddlewareObservationOutcome::Timeout),
            (
                Behavior::BeforeError,
                HttpMiddlewareObservationOutcome::Internal,
            ),
        ] {
            let events = Arc::new(Mutex::new(Vec::new()));
            let middlewares = [middleware("bounded_probe", behavior, &events)];
            let terminal = Terminal { events };
            let observer = RecordingObserver::default();
            let mut request = Request::from_transport_parts(
                "GET".to_string(),
                "/tenant/token=super-secret".to_string(),
                Vec::new(),
                &[],
            )
            .await
            .unwrap();
            let mut response = Response::new().await.unwrap();
            let mut exchange = HttpExchange::new(&mut request, &mut response);
            let outcome = MiddlewareChainOutcomeSlot::default();

            HttpMiddlewareChain::new(&middlewares, &terminal, &ErrorWriter, &outcome, &observer)
                .run(&mut exchange)
                .await
                .unwrap();

            let observations = observer
                .observations
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            assert_eq!(*observations, [("bounded_probe", expected)]);
            assert!(!format!("{observations:?}").contains("super-secret"));
        }
    }

    #[tokio::test]
    async fn outer_after_failure_does_not_rewrite_an_inner_materialized_failure() {
        for (inner_behavior, expected_status, expected_code, expected_events) in [
            (
                Behavior::Rejection,
                403,
                "TEST_MIDDLEWARE_REJECTED",
                vec![
                    ("outer", Phase::Before),
                    ("inner", Phase::Before),
                    ("outer", Phase::After),
                ],
            ),
            (
                Behavior::AfterError,
                500,
                "TEST_MIDDLEWARE_FAILURE",
                vec![
                    ("outer", Phase::Before),
                    ("inner", Phase::Before),
                    ("terminal", Phase::Handler),
                    ("inner", Phase::After),
                    ("outer", Phase::After),
                ],
            ),
        ] {
            let events = Arc::new(Mutex::new(Vec::new()));
            let middlewares = [
                middleware("outer", Behavior::AfterError, &events),
                middleware("inner", inner_behavior, &events),
            ];
            let terminal = Terminal {
                events: Arc::clone(&events),
            };
            let writes = Arc::new(AtomicUsize::new(0));
            let writer = CountingErrorWriter(Arc::clone(&writes));
            let (mut request, mut response) = exchange_parts().await;
            let mut exchange = HttpExchange::new(&mut request, &mut response);
            let outcome = MiddlewareChainOutcomeSlot::default();

            HttpMiddlewareChain::new(
                &middlewares,
                &terminal,
                &writer,
                &outcome,
                &NoopHttpMiddlewareObserver,
            )
            .run(&mut exchange)
            .await
            .unwrap();

            assert_eq!(writes.load(Ordering::Acquire), 1);
            let MiddlewareChainOutcome::Materialized(failure) = outcome.take() else {
                panic!("the first materialized failure must remain authoritative");
            };
            assert_eq!(failure.origin(), "inner");
            assert_eq!(failure.code().as_str(), expected_code);
            assert_eq!(failure.status(), expected_status);
            assert_eq!(exchange.response().status_code_value(), expected_status);
            assert_eq!(*events.lock().unwrap(), expected_events);
        }
    }

    #[tokio::test]
    async fn private_outcome_survives_user_request_local_clear() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let middlewares = [
            middleware("clearer", Behavior::AroundAndClearRequestLocal, &events),
            middleware("rejecter", Behavior::Rejection, &events),
        ];
        let terminal = Terminal {
            events: Arc::clone(&events),
        };
        let (mut request, mut response) = exchange_parts().await;
        let mut exchange = HttpExchange::new(&mut request, &mut response);
        let outcome = MiddlewareChainOutcomeSlot::default();

        HttpMiddlewareChain::new(
            &middlewares,
            &terminal,
            &ErrorWriter,
            &outcome,
            &NoopHttpMiddlewareObserver,
        )
        .run(&mut exchange)
        .await
        .unwrap();

        let MiddlewareChainOutcome::Materialized(failure) = outcome.take() else {
            panic!("private executor outcome must not live in user request-local state");
        };
        assert_eq!(failure.origin(), "rejecter");
        assert_eq!(failure.code().as_str(), "TEST_MIDDLEWARE_REJECTED");
        assert!(exchange.request().local().is_empty());
        assert_eq!(exchange.response().status_code_value(), 403);
        assert_eq!(
            *events.lock().unwrap(),
            [
                ("clearer", Phase::Before),
                ("rejecter", Phase::Before),
                ("clearer", Phase::After),
            ]
        );
    }

    struct RejectingErrorWriter {
        writes: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl HttpMiddlewareErrorWriter for RejectingErrorWriter {
        async fn write_error_response(
            &self,
            _exchange: &mut HttpExchange<'_>,
            _error: &HttpMiddlewareError,
        ) -> Result<(), HttpMiddlewareError> {
            self.writes.fetch_add(1, Ordering::AcqRel);
            Err(lily_middleware::HttpMiddlewareRejection::new(
                418,
                MiddlewareErrorCode::new("UNTRUSTED_WRITER_REJECTION").unwrap(),
            )
            .unwrap()
            .into())
        }
    }

    #[tokio::test]
    async fn writer_failure_is_normalized_to_one_bounded_internal_outcome() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let middlewares = [
            middleware("outer", Behavior::Around, &events),
            middleware("failing", Behavior::BeforeError, &events),
        ];
        let terminal = Terminal {
            events: Arc::clone(&events),
        };
        let (mut request, mut response) = exchange_parts().await;
        let mut exchange = HttpExchange::new(&mut request, &mut response);
        let outcome = MiddlewareChainOutcomeSlot::default();
        let writes = Arc::new(AtomicUsize::new(0));
        let writer = RejectingErrorWriter {
            writes: Arc::clone(&writes),
        };

        let error = HttpMiddlewareChain::new(
            &middlewares,
            &terminal,
            &writer,
            &outcome,
            &NoopHttpMiddlewareObserver,
        )
        .run(&mut exchange)
        .await
        .unwrap_err();

        assert_eq!(error.diagnostic_code(), MiddlewareErrorCode::INTERNAL);
        assert_eq!(error.http_status(), 500);
        let MiddlewareChainOutcome::WriterFailed(failure) = outcome.take() else {
            panic!("writer failure must retain a private bounded outcome");
        };
        assert_eq!(failure.origin(), "http_error_writer");
        assert_eq!(failure.code(), MiddlewareErrorCode::INTERNAL);
        assert_eq!(failure.status(), 500);
        assert_eq!(writes.load(Ordering::Acquire), 1);
        assert!(!format!("{error:?} {error}").contains("UNTRUSTED_WRITER_REJECTION"));
        assert_eq!(
            *events.lock().unwrap(),
            [("outer", Phase::Before), ("failing", Phase::Before)]
        );
    }

    #[tokio::test]
    async fn supported_chain_depths_complete_once_without_stack_or_order_failure() {
        for depth in [1_usize, 32, 64] {
            let events = Arc::new(Mutex::new(Vec::new()));
            let middlewares: Vec<_> = (0..depth)
                .map(|_| middleware("layer", Behavior::Around, &events))
                .collect();
            let terminal = Terminal {
                events: Arc::clone(&events),
            };
            let (mut request, mut response) = exchange_parts().await;
            let mut exchange = HttpExchange::new(&mut request, &mut response);
            let outcome = MiddlewareChainOutcomeSlot::default();

            HttpMiddlewareChain::new(
                &middlewares,
                &terminal,
                &ErrorWriter,
                &outcome,
                &NoopHttpMiddlewareObserver,
            )
            .run(&mut exchange)
            .await
            .unwrap();
            assert_eq!(outcome.take(), MiddlewareChainOutcome::None);

            let events = events.lock().unwrap();
            assert_eq!(events.len(), depth * 2 + 1);
            assert_eq!(
                events
                    .iter()
                    .filter(|(_, phase)| *phase == Phase::Handler)
                    .count(),
                1
            );
        }
    }

    struct ActiveGuard(Arc<AtomicUsize>);

    impl Drop for ActiveGuard {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::AcqRel);
        }
    }

    struct PendingMiddleware {
        semaphore: Arc<tokio::sync::Semaphore>,
        active: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl HttpMiddleware for PendingMiddleware {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, HttpMiddlewareInitError> {
            Ok(Self {
                semaphore: Arc::new(tokio::sync::Semaphore::new(1)),
                active: Arc::new(AtomicUsize::new(0)),
            })
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            MiddlewareDescriptor::new("pending", MiddlewareKind::Custom)
        }

        async fn handle(
            &self,
            _exchange: &mut HttpExchange<'_>,
            _next: HttpNext<'_>,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), HttpMiddlewareError> {
            let _permit = Arc::clone(&self.semaphore).acquire_owned().await.unwrap();
            self.active.fetch_add(1, Ordering::AcqRel);
            let _active = ActiveGuard(Arc::clone(&self.active));
            std::future::pending::<()>().await;
            Ok(())
        }
    }

    #[tokio::test]
    async fn hard_timeout_drops_the_active_frame_and_reconciles_its_permit() {
        let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
        let active = Arc::new(AtomicUsize::new(0));
        let middleware: Arc<dyn HttpMiddleware> = Arc::new(PendingMiddleware {
            semaphore: Arc::clone(&semaphore),
            active: Arc::clone(&active),
        });
        let middleware =
            CompiledHttpMiddleware::new(Arc::clone(&middleware), middleware.descriptor());
        let terminal = Terminal {
            events: Arc::new(Mutex::new(Vec::new())),
        };
        let (mut request, mut response) = exchange_parts().await;
        let mut exchange = HttpExchange::new(&mut request, &mut response);
        let outcome = MiddlewareChainOutcomeSlot::default();
        let observer = RecordingObserver::default();

        let timed_out = tokio::time::timeout(
            std::time::Duration::from_millis(25),
            HttpMiddlewareChain::new(&[middleware], &terminal, &ErrorWriter, &outcome, &observer)
                .run(&mut exchange),
        )
        .await;

        assert!(timed_out.is_err());
        assert_eq!(active.load(Ordering::Acquire), 0);
        assert_eq!(semaphore.available_permits(), 1);
        assert_eq!(
            *observer
                .observations
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            [("pending", HttpMiddlewareObservationOutcome::Aborted)]
        );
    }
}
