mod support;

use support::*;

#[derive(Controller)]
#[base_path("/api/missing-trait")]
struct MissingTraitController;

fn main() {}
