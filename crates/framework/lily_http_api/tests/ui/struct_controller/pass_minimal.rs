mod support;

use support::*;

#[derive(Controller)]
#[base_path("/api/minimal")]
struct MinimalController;

impl_controller_trait!(MinimalController);

#[controller]
impl MinimalController {
    #[get("/")]
    async fn index(&self) -> Result<(), HttpApiError> {
        Ok(())
    }
}

fn main() {
    let _ = lily_http_api::__private::get_struct_controller_registrations();
    let _ = lily_http_api::__private::get_pending_controller_routes();
}
