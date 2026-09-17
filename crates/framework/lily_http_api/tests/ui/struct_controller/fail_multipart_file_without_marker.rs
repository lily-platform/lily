use lily_http_api::{FormFile, MultipartForm};

#[derive(MultipartForm)]
struct Upload {
    file: FormFile,
}

fn main() {}
