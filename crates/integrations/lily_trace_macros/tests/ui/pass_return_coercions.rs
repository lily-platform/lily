#![deny(warnings)]
extern crate trace_runtime as lily_trace;

use lily_trace::lily_trace;
use std::{convert::Infallible, fmt::Display};

#[lily_trace]
fn erased(flag: bool) -> Box<dyn Display> {
    if flag { Box::new(1_u8) } else { Box::new("two") }
}

#[lily_trace(result)]
async fn erased_result(flag: bool) -> Result<Box<dyn Display>, Infallible> {
    if flag { Ok(Box::new(1_u8)) } else { Ok(Box::new("two")) }
}

#[lily_trace(result)]
async fn erased_early_return(flag: bool) -> Result<Box<dyn Display>, Infallible> {
    if flag { return Ok(Box::new(1_u8)); }
    Ok(Box::new("two"))
}

#[lily_trace(result)]
async fn nested_opaque(value: u8) -> Result<impl Display, Infallible> { Ok(value) }

#[lily_trace]
fn slice(flag: bool) -> &'static [u8] {
    if flag { &[1] } else { &[1, 2] }
}

#[lily_trace]
fn opaque(value: u8) -> impl Display { value }

fn main() {
    assert_eq!(erased(true).to_string(), "1");
    assert_eq!(erased(false).to_string(), "two");
    assert_eq!(slice(true), &[1]);
    assert_eq!(opaque(4).to_string(), "4");
    drop(erased_result(false));
    drop(erased_early_return(true));
    drop(nested_opaque(4));
}
