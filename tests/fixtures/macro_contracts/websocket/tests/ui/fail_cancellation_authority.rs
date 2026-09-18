#![allow(dead_code, deprecated)]

use lily_websocket::{Cancellation, CleanupCancellation, ExecutionCancellation};
use tokio_util::sync::CancellationToken;

fn mutate_execution(signal: ExecutionCancellation) {
    signal.cancel();
    signal.child_token();
}

fn mutate_cleanup(signal: CleanupCancellation) {
    signal.cancel();
    signal.child_token();
}

fn legacy_source(signal: Cancellation) {
    signal.cancel();
    let _ = signal.0;
}

fn execution_field(signal: ExecutionCancellation) { let _ = signal.token; }
fn cleanup_field(signal: CleanupCancellation) { let _ = signal.token; }
fn execution_inner(signal: ExecutionCancellation) { let _ = signal.into_inner(); }
fn cleanup_inner(signal: CleanupCancellation) { let _ = signal.into_inner(); }
fn execution_deref(signal: ExecutionCancellation) { let _: &CancellationToken = &signal; }
fn cleanup_deref(signal: CleanupCancellation) { let _: &CancellationToken = &signal; }
fn execution_into_raw(signal: ExecutionCancellation) { let _: CancellationToken = signal.into(); }
fn cleanup_into_raw(signal: CleanupCancellation) { let _: CancellationToken = signal.into(); }
fn raw_into_execution(token: CancellationToken) { let _: ExecutionCancellation = token.into(); }
fn raw_into_cleanup(token: CancellationToken) { let _: CleanupCancellation = token.into(); }
fn execution_into_cleanup(signal: ExecutionCancellation) { let _: CleanupCancellation = signal.into(); }
fn cleanup_into_execution(signal: CleanupCancellation) { let _: ExecutionCancellation = signal.into(); }
fn construct_execution(token: CancellationToken) { let _ = ExecutionCancellation::new(token); }
fn construct_cleanup(token: CancellationToken) { let _ = CleanupCancellation::new(token); }

fn handshake_context(exchange: &lily_websocket::middleware::WsHandshakeExchange) {
    exchange.cancellation().cancel();
}
fn message_exchange(exchange: &lily_websocket::middleware::WsMessageExchange) {
    exchange.cancellation().cancel();
}
fn message_invocation(invocation: &lily_websocket::WebSocketMessageInvocation) {
    invocation.cancellation().cancel();
}
fn message_context(context: &lily_websocket::WebSocketMessageContext) {
    context.cancellation().cancel();
}
fn connected_invocation(invocation: &lily_websocket::WebSocketLifecycleInvocation) {
    invocation.execution_cancellation().unwrap().cancel();
}
fn disconnected_invocation(invocation: &lily_websocket::WebSocketLifecycleInvocation) {
    invocation.cleanup_cancellation().unwrap().cancel();
}

fn termination_context(context: &lily_websocket::middleware::WsMessageTerminationContext<'_>) {
    context.cancellation().cancel();
    let _: &ExecutionCancellation = context.cancellation();
    let _ = &context.exchange;
}

fn main() {}
