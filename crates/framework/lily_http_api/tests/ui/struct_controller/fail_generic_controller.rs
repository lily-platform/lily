mod support;

use support::*;

#[derive(Controller)]
#[base_path("/api/generic")]
struct GenericController<T>(T);

fn main() {}
