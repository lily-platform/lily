mod support;

use support::*;

#[derive(Controller)]
#[base_path("api/invalid")]
struct InvalidBasePathController;

impl_controller_trait!(InvalidBasePathController);

fn main() {}
