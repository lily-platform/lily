use lily_http_api::MultipartForm;

#[derive(MultipartForm)]
struct Upload {
    #[form_file]
    title: String,
}

fn main() {}
