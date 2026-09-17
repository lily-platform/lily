mod support;

use support::*;

struct RawBody;
struct TestParts;

#[derive(Controller)]
#[base_path("/api/parts-after-body")]
struct PartsAfterBodyController;

impl_controller_trait!(PartsAfterBodyController);

#[controller]
impl PartsAfterBodyController {
    #[post("/")]
    async fn index(&self, _body: RawBody, _parts: TestParts) -> Result<(), HttpApiError> {
        Ok(())
    }
}

fn main() {}
