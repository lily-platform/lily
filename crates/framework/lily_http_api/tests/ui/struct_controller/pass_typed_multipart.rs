mod support;

use lily_http_api::{FormFile, MultipartForm};
use support::*;

#[derive(MultipartForm)]
struct Upload {
    title: String,
    note: Option<String>,
    tag: Vec<String>,
    #[form_file]
    avatar: FormFile,
    #[form_file]
    preview: Option<FormFile>,
    #[form_file]
    attachments: Vec<FormFile>,
}

#[derive(Controller)]
#[base_path("/api/typed-multipart")]
struct TypedMultipartController;

impl_controller_trait!(TypedMultipartController);

#[controller]
impl TypedMultipartController {
    #[post("/upload")]
    async fn upload(
        &self,
        MultipartForm(_upload): MultipartForm<Upload>,
    ) -> Result<(), HttpApiError> {
        Ok(())
    }
}

fn main() {}
