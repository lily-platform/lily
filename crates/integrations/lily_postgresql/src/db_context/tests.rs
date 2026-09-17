use std::time::Duration;

use lily_cancellation::__private::ExecutionCancellationSource;

use super::*;

#[tokio::test]
async fn inert_context_and_precancelled_calls_never_invoke_work() {
    let context = PgDbContext::default();
    let source = ExecutionCancellationSource::default();
    source.cancel();
    assert_eq!(
        context
            .with_connection(|_, _| panic!("must not run"), Some(source.view()))
            .await,
        Err::<(), _>(PgError::OperationCancelled)
    );
    assert_eq!(
        context
            .transaction(|_| async { panic!("must not run") }, Some(source.view()))
            .await,
        Err::<(), _>(PgError::TransactionCancelled)
    );
    assert_eq!(
        context
            .with_connection(|_, _| panic!("must not run"), None)
            .await,
        Err::<(), _>(PgError::NotInitialized)
    );
    context.dispose().await.unwrap();
    context.dispose().await.unwrap();
    assert_eq!(
        context.transaction(|_| async { Ok(()) }, None).await,
        Err(PgError::ContextClosed)
    );
}

#[tokio::test]
async fn database_precancellation_covers_acquisition_and_callback() {
    let database = PgDatabaseService::default();
    let source = ExecutionCancellationSource::default();
    source.cancel();
    assert!(matches!(
        database.acquire_connection(Some(source.view())).await,
        Err(PgError::OperationCancelled)
    ));
    assert_eq!(
        database
            .with_connection(|_, _| panic!("must not run"), Some(source.view()))
            .await,
        Err::<(), _>(PgError::OperationCancelled)
    );
}

#[tokio::test]
async fn starting_and_finalizing_never_admit_queries_or_nested_transactions() {
    let runtime = Arc::new(ContextRuntime::default());
    let completion = runtime
        .transaction(None, || Ok(Duration::from_secs(1)))
        .unwrap();
    let Admission::Transaction(control) = runtime
        .operation(None, || panic!("must reuse transaction"))
        .unwrap()
    else {
        panic!("must not acquire another connection")
    };
    assert!(matches!(control.admit(), Err(PgError::ContextBusy)));
    assert!(matches!(
        runtime.transaction(None, || panic!("already reserved")),
        Err(PgError::ContextTransactionActive)
    ));
    control.activate();
    let query = control.admit().unwrap();
    assert!(matches!(control.admit(), Err(PgError::ContextBusy)));
    query.complete(None, false);
    assert_eq!(control.finalize(Ok::<_, PgError>(())), Ok(()));
    assert!(matches!(control.admit(), Err(PgError::ContextBusy)));
    completion.finish(Ok(()));
}

// Intentionally neither Clone, Sync, Debug nor std::error::Error. Only the
// boundary's From<PgError> + Send contract is required from application errors.
struct ApplicationError(PgError, std::cell::Cell<()>);

impl From<PgError> for ApplicationError {
    fn from(error: PgError) -> Self {
        Self(error, std::cell::Cell::new(()))
    }
}

async fn expect_application_error(
    context: &PgDbContext,
    token: Option<ExecutionCancellation>,
    expected: PgError,
) {
    let result: Result<(), ApplicationError> = context
        .transaction(|_| async { panic!("rejected work must not run") }, token)
        .await;
    match result {
        Err(ApplicationError(error, _)) => assert_eq!(error, expected),
        Ok(()) => panic!("transaction unexpectedly entered work"),
    }
}

#[tokio::test]
async fn application_error_converts_admission_failures_without_extra_trait_bounds() {
    let context = PgDbContext::default();
    expect_application_error(&context, None, PgError::NotInitialized).await;
    let cancelled = ExecutionCancellationSource::default();
    cancelled.cancel();
    expect_application_error(
        &context,
        Some(cancelled.view()),
        PgError::TransactionCancelled,
    )
    .await;

    let completion = context
        .runtime
        .transaction(None, || Ok(Duration::from_secs(1)))
        .unwrap();
    expect_application_error(&context, None, PgError::ContextTransactionActive).await;
    completion.finish(Ok(()));

    let Admission::Pooled(completion) = context
        .runtime
        .operation(None, || Ok(Duration::from_secs(1)))
        .unwrap()
    else {
        panic!("an idle context must admit ordinary work");
    };
    expect_application_error(&context, None, PgError::ContextBusy).await;
    completion.finish(Ok(()));

    context.dispose().await.unwrap();
    expect_application_error(&context, None, PgError::ContextClosed).await;
}

async fn expect_connection_application_error(
    context: &PgDbContext,
    token: Option<ExecutionCancellation>,
    expected: PgError,
) {
    fn send<T: Send>(value: T) -> T {
        value
    }
    // The callback may borrow its caller; only E needs to be 'static.
    let mut invoked = false;
    let result: Result<(), ApplicationError> = send(context.with_connection(
        |_, _| {
            invoked = true;
            panic!("rejected callback must not run")
        },
        token,
    ))
    .await;
    assert!(!invoked);
    match result {
        Err(ApplicationError(error, _)) => assert_eq!(error, expected),
        Ok(()) => panic!("connection unexpectedly entered callback"),
    }
}

#[tokio::test]
async fn connection_application_error_converts_admission_without_extra_trait_bounds() {
    let context = PgDbContext::default();
    expect_connection_application_error(&context, None, PgError::NotInitialized).await;
    let cancelled = ExecutionCancellationSource::default();
    cancelled.cancel();
    expect_connection_application_error(
        &context,
        Some(cancelled.view()),
        PgError::OperationCancelled,
    )
    .await;

    let Admission::Pooled(completion) = context
        .runtime
        .operation(None, || Ok(Duration::from_secs(1)))
        .unwrap()
    else {
        panic!("idle context must admit ordinary work");
    };
    expect_connection_application_error(&context, None, PgError::ContextBusy).await;
    completion.finish(Ok(()));

    let completion = context
        .runtime
        .transaction(None, || Ok(Duration::from_secs(1)))
        .unwrap();
    expect_connection_application_error(&context, None, PgError::ContextBusy).await;
    completion.control.activate();
    let permit = completion.control.admit().unwrap();
    expect_connection_application_error(&context, None, PgError::ContextBusy).await;
    permit.complete(Some(PgError::TransactionRollbackOnly), false);
    expect_connection_application_error(&context, None, PgError::TransactionRollbackOnly).await;
    completion.finish(Ok(()));

    context.dispose().await.unwrap();
    expect_connection_application_error(&context, None, PgError::ContextClosed).await;
}

#[tokio::test(start_paused = true)]
async fn repeated_disposal_shares_one_deadline_and_retains_failure() {
    let runtime = Arc::new(ContextRuntime::default());
    let completion = runtime
        .transaction(None, || Ok(Duration::from_secs(1)))
        .unwrap();
    let disposal = runtime.dispose();
    tokio::pin!(disposal);
    assert!(futures_util::poll!(&mut disposal).is_pending());
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(disposal.await, Err(PgError::ContextCleanupTimeout));
    completion.finish(Ok(()));
    assert_eq!(runtime.dispose().await, Err(PgError::ContextCleanupTimeout));
}
