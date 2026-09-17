mod support;

use support::*;

#[derive(Controller)]
struct MissingBasePathController;

impl_controller_trait!(MissingBasePathController);

fn main() {}
