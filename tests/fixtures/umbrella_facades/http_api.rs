use provider::{
    Controller, ControllerInitError, ControllerTrait, Extensions, FormFile, HttpApiError,
    MultipartForm, controller,
};
use std::sync::Arc;

#[derive(Controller)]
#[base_path("/umbrella")]
struct HttpController;
#[lifecycle]
impl ControllerTrait for HttpController {
    async fn new(_: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        Ok(Self)
    }
}
#[derive(MultipartForm)]
struct Upload {
    title: String,
    #[form_file]
    attachment: FormFile,
}
#[controller]
impl HttpController {
    #[get("/ping")]
    async fn ping(&self) -> Result<(), HttpApiError> {
        Ok(())
    }
    #[post("/upload")]
    async fn upload(
        &self,
        MultipartForm(_form): MultipartForm<Upload>,
    ) -> Result<(), HttpApiError> {
        Ok(())
    }
}
#[test]
fn http_macros_share_the_facades_route_registry() {
    let controllers = provider::__private::get_struct_controller_registrations();
    assert_eq!(controllers.len(), 1);
    assert_eq!(
        controllers[0].type_id(),
        std::any::TypeId::of::<HttpController>()
    );
    let routes = provider::__private::get_pending_controller_routes();
    assert_eq!(routes.len(), 2);
    let mut actual: Vec<_> = routes
        .iter()
        .map(|route| (route.method(), route.path()))
        .collect();
    actual.sort();
    assert_eq!(
        actual,
        [("GET", "/umbrella/ping"), ("POST", "/umbrella/upload")]
    );
}
