use super::*;

fn returned_execution() -> ExecutionEvidence {
    let mut execution = ExecutionEvidence::default();
    assert!(execution.observe_first_poll());
    assert!(execution.observe_termination(SlotTermination::Returned));
    execution
}

fn released_response(outcome: ResponseBodyOutcome) -> ResponseEvidence {
    let mut response = ResponseEvidence::default();
    assert!(response.observe_body_outcome(outcome));
    response.observe_source_release();
    response.observe_bridge_detached();
    response
}

fn request<'a>(
    execution: &'a ExecutionEvidence,
    response: &'a ResponseEvidence,
    middleware: &'a [MiddlewareCleanupEvidence],
    helpers: &'a [TaskJoinEvidence],
) -> RequestTerminationEvidence<'a> {
    RequestTerminationEvidence {
        execution,
        request_body_released: true,
        response,
        middleware,
        scope: ScopeCleanupEvidence::Outstanding,
        helpers,
    }
}

#[test]
fn cancellation_preserves_first_reason_and_does_not_prove_termination() {
    for reason in [
        ExecutionStopReason::GracefulDeadline,
        ExecutionStopReason::ForcedShutdown,
        ExecutionStopReason::RequestTimeout,
        ExecutionStopReason::PeerDisconnect,
        ExecutionStopReason::TransportFailure,
    ] {
        let mut execution = ExecutionEvidence::default();
        assert!(execution.observe_first_poll());
        assert!(execution.request_cancellation(reason));
        assert!(!execution.request_cancellation(ExecutionStopReason::ForcedShutdown));
        assert_eq!(execution.cancellation_requested(), Some(reason));
        assert!(!execution.is_terminal());

        // The cooperative execution can still return normally after the signal.
        assert!(execution.observe_termination(SlotTermination::Returned));
        assert_eq!(execution.termination(), Some(SlotTermination::Returned));
        assert!(!execution.observe_termination(SlotTermination::Dropped));
        assert!(!execution.observe_first_poll());
        assert!(!execution.request_cancellation(reason));
    }
}

#[test]
fn unpolled_drop_is_terminal_without_inventing_execution() {
    let mut execution = ExecutionEvidence::default();
    assert!(!execution.observe_termination(SlotTermination::Returned));
    assert!(!execution.observe_termination(SlotTermination::ReturnedError));
    assert!(!execution.is_terminal());
    assert!(execution.observe_termination(SlotTermination::Dropped));
    assert!(!execution.started());
    assert!(execution.is_terminal());
    assert!(!execution.observe_first_poll());
}

#[test]
fn terminal_failure_cannot_be_replaced_with_success() {
    for outcome in [SlotTermination::ReturnedError, SlotTermination::Panicked] {
        let mut execution = ExecutionEvidence::default();
        assert!(execution.observe_first_poll());
        assert!(!execution.observe_first_poll());
        assert!(execution.observe_termination(outcome));
        assert!(!execution.observe_termination(SlotTermination::Returned));
        assert_eq!(execution.termination(), Some(outcome));
        assert!(execution.is_terminal());
    }
}

#[test]
fn abort_request_requires_a_join_and_does_not_predict_its_result() {
    for outcome in [
        TaskJoinOutcome::Completed,
        TaskJoinOutcome::Failed,
        TaskJoinOutcome::Cancelled,
        TaskJoinOutcome::Panicked,
    ] {
        let mut task = TaskJoinEvidence::default();
        assert!(task.request_abort());
        assert!(!task.request_abort());
        assert!(task.abort_requested());
        assert_eq!(task.outcome(), None);
        assert!(!task.is_terminal());

        // Completion can win an abort race. Preserve the actual joined result.
        assert!(task.observe_join(outcome));
        assert!(task.is_terminal());
        assert!(!task.observe_join(TaskJoinOutcome::Completed));
        assert!(!task.request_abort());
        assert_eq!(task.outcome(), Some(outcome));
    }
}

#[test]
fn execution_completion_and_body_eof_do_not_authorize_scope_close() {
    let execution = returned_execution();
    let mut response = ResponseEvidence::default();
    let middleware = [MiddlewareCleanupEvidence::NormalReturned];
    assert!(!request(&execution, &response, &middleware, &[]).may_close_scope());

    response.observe_service_handoff();
    assert_eq!(response.head(), ResponseHeadEvidence::HandedOffToService);
    assert!(!request(&execution, &response, &middleware, &[]).may_close_scope());
    assert!(response.observe_body_outcome(ResponseBodyOutcome::Completed));
    assert!(!request(&execution, &response, &middleware, &[]).may_close_scope());
    response.observe_source_release();
    assert!(!request(&execution, &response, &middleware, &[]).may_close_scope());
    response.observe_bridge_detached();
    let evidence = request(&execution, &response, &middleware, &[]);
    assert!(evidence.may_close_scope());
    assert!(!evidence.is_terminal()); // The exact DI receipt is still outstanding.
}

#[test]
fn response_outcome_and_resource_release_are_independent_of_delivery() {
    for outcome in [
        ResponseBodyOutcome::Completed,
        ResponseBodyOutcome::NotStarted,
        ResponseBodyOutcome::Interrupted,
        ResponseBodyOutcome::Failed,
        ResponseBodyOutcome::Panicked,
    ] {
        let mut response = ResponseEvidence::default();
        response.observe_source_release();
        response.observe_bridge_detached();
        assert!(!response.resources_terminal()); // No producer disposition yet.
        assert!(response.observe_body_outcome(outcome));
        assert!(!response.observe_body_outcome(ResponseBodyOutcome::Completed));
        assert_eq!(response.body(), Some(outcome));
        assert!(response.resources_terminal());
        assert_eq!(response.head(), ResponseHeadEvidence::NotHandedOff);
    }
}

#[test]
fn pending_input_or_unjoined_helper_blocks_parent_cleanup() {
    let execution = returned_execution();
    let response = released_response(ResponseBodyOutcome::Completed);
    let middleware = [MiddlewareCleanupEvidence::NormalReturned];
    let mut helpers = [TaskJoinEvidence::default(), TaskJoinEvidence::default()];
    assert!(helpers[0].observe_join(TaskJoinOutcome::Completed));
    assert!(helpers[1].request_abort());
    assert!(!request(&execution, &response, &middleware, &helpers).may_begin_termination_cleanup());

    assert!(helpers[1].observe_join(TaskJoinOutcome::Cancelled));
    let mut evidence = request(&execution, &response, &middleware, &helpers);
    evidence.request_body_released = false;
    assert!(!evidence.may_begin_termination_cleanup());
    evidence.request_body_released = true;
    assert!(evidence.may_begin_termination_cleanup());
    assert!(evidence.may_close_scope());
    assert!(!evidence.is_terminal());
}

#[test]
fn pending_execution_or_interrupted_normal_exit_remains_a_barrier() {
    let mut execution = ExecutionEvidence::default();
    assert!(execution.observe_first_poll());
    assert!(execution.request_cancellation(ExecutionStopReason::ForcedShutdown));
    let response = released_response(ResponseBodyOutcome::NotStarted);
    let mut middleware = [
        MiddlewareCleanupEvidence::Outstanding,
        MiddlewareCleanupEvidence::NormalReturned,
    ];
    assert!(!request(&execution, &response, &middleware, &[]).may_begin_termination_cleanup());

    assert!(execution.observe_termination(SlotTermination::Dropped));
    let evidence = request(&execution, &response, &middleware, &[]);
    assert!(evidence.may_begin_termination_cleanup());
    assert!(!evidence.may_close_scope());
    middleware[0] = MiddlewareCleanupEvidence::TerminationSettled(CleanupOutcome::Succeeded);
    let mut evidence = request(&execution, &response, &middleware, &[]);
    assert!(evidence.may_close_scope());
    assert!(!evidence.is_terminal());
    evidence.scope = ScopeCleanupEvidence::Terminated(CleanupOutcome::Succeeded);
    assert!(evidence.is_terminal());
    assert!(evidence.cleanup_succeeded()); // Forced stop is not HTTP completion.
}

#[test]
fn settled_cleanup_failure_is_terminal_but_never_successful() {
    let execution = returned_execution();
    let response = released_response(ResponseBodyOutcome::Interrupted);
    for outcome in [
        CleanupOutcome::Failed,
        CleanupOutcome::TimedOut,
        CleanupOutcome::Cancelled,
        CleanupOutcome::Panicked,
        CleanupOutcome::NotStarted,
        CleanupOutcome::Unknown,
    ] {
        let middleware = [MiddlewareCleanupEvidence::TerminationSettled(outcome)];
        let mut evidence = request(&execution, &response, &middleware, &[]);
        evidence.scope = ScopeCleanupEvidence::Terminated(CleanupOutcome::Succeeded);
        assert!(evidence.is_terminal());
        assert!(!evidence.cleanup_succeeded());

        let middleware = [MiddlewareCleanupEvidence::NormalReturned];
        let mut evidence = request(&execution, &response, &middleware, &[]);
        evidence.scope = ScopeCleanupEvidence::Terminated(outcome);
        assert!(evidence.is_terminal());
        assert!(!evidence.cleanup_succeeded());
    }
}

#[test]
fn no_scope_created_and_scope_receipt_missing_are_different() {
    let execution = returned_execution();
    let response = released_response(ResponseBodyOutcome::NotStarted);
    let mut evidence = request(&execution, &response, &[], &[]);
    assert!(!evidence.is_terminal());
    evidence.scope = ScopeCleanupEvidence::NotCreated;
    assert!(evidence.is_terminal());
    assert!(evidence.cleanup_succeeded());
}
