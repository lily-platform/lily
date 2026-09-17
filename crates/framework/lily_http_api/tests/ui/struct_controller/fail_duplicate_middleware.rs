mod support;

use support::*;

struct FirstMiddleware;
struct SecondMiddleware;

#[derive(Controller)]
#[base_path("/api/duplicate-middleware")]
struct DuplicateMiddlewareController;

impl_controller_trait!(DuplicateMiddlewareController);

#[controller]
impl DuplicateMiddlewareController {
    #[get("/")]
    #[middleware(FirstMiddleware)]
    #[middleware(SecondMiddleware)]
    async fn index(&self) -> Result<(), HttpApiError> {
        Ok(())
    }
}

fn main() {}
