#![deny(warnings)]
extern crate trace_runtime as lily_trace;

use async_trait::async_trait;
use lily_trace::{lily_trace, TraceFailure, TraceResultError};
use std::{future::Future, pin::Pin, rc::Rc};

// No formatting, cloning or standard Error implementation is required.
struct Failure;
impl TraceResultError for Failure {
    fn trace_failure(&self) -> TraceFailure {
        TraceFailure::Rejected { code: "invalid_input" }
    }
}
type AppResult<T> = Result<T, Failure>;

#[lily_trace(result, fields(id), skip(value), env = ["development", "test"])]
async fn generic<T>(id: u64, value: T) -> AppResult<T> {
    let _ = id;
    Ok(value)
}

#[async_trait]
trait Service {
    async fn read<'a>(&self, value: &'a str) -> AppResult<&'a str>;
}
struct Implementation;
#[async_trait]
impl Service for Implementation {
    #[lily_trace(result)]
    async fn read<'a>(&self, value: &'a str) -> AppResult<&'a str> {
        if value.is_empty() { return Err(Failure); }
        Ok(value)
    }
}

#[async_trait(?Send)]
trait LocalService {
    async fn read(&self, value: Rc<u8>) -> AppResult<Rc<u8>>;
}
#[async_trait(?Send)]
impl LocalService for Implementation {
    #[lily_trace(result)]
    async fn read(&self, value: Rc<u8>) -> AppResult<Rc<u8>> { Ok(value) }
}

#[lily_trace(result)]
fn boxed(value: &str) -> Pin<Box<dyn Future<Output = AppResult<&str>> + Send + '_>> {
    Box::pin(async move { Ok(value) })
}

fn assert_send(_: impl Send) {}
fn main() {
    assert_send(Service::read(&Implementation, "borrowed"));
    drop(LocalService::read(&Implementation, Rc::new(1)));
    drop(generic(1, Rc::new(1)));
    assert_send(boxed("value"));
}
