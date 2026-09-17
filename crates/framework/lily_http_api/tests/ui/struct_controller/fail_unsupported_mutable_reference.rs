mod support;

use support::*;

#[derive(Controller)]
#[base_path("/api/unsupported-reference")]
struct UnsupportedReferenceController;

impl_controller_trait!(UnsupportedReferenceController);

#[controller]
impl UnsupportedReferenceController {
    #[get("/")]
    async fn index(&self, _value: &mut String) -> Result<(), HttpApiError> {
        Ok(())
    }
}

fn main() {}
