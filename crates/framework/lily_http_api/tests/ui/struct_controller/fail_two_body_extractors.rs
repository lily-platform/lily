mod support;

use support::*;

struct Json<T>(T);
struct RawBody;

#[derive(Controller)]
#[base_path("/api/two-body")]
struct TwoBodyController;

impl_controller_trait!(TwoBodyController);

#[controller]
impl TwoBodyController {
    #[post("/")]
    async fn index(&self, _json: Json<String>, _raw: RawBody) -> Result<(), HttpApiError> {
        Ok(())
    }
}

fn main() {}
