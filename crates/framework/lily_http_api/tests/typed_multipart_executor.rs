use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::time::Duration;

use lily_http_api::AppBuilder;
use lily_http_api::{
    controller, CancellationToken, Controller, ControllerInitError, ControllerTrait, Extensions,
    FormFile, HttpApiError, HttpHealthService, HttpTransportConfig, MultipartForm, PlainText,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const BOUNDARY: &str = "LILY-TAE06-BOUNDARY";

#[derive(MultipartForm)]
struct UploadInput {
    title: String,
    note: Option<String>,
    tag: Vec<String>,
    #[form_file]
    avatar: FormFile,
    #[form_file]
    preview: Option<FormFile>,
    #[form_file]
    attachment: Vec<FormFile>,
}

#[derive(Controller)]
#[base_path("/typed-multipart")]
struct TypedMultipartController;

#[lily_http_api::async_trait::async_trait]
impl ControllerTrait for TypedMultipartController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        Ok(Self)
    }
}

#[controller]
impl TypedMultipartController {
    #[post("/upload")]
    async fn upload(
        &self,
        MultipartForm(input): MultipartForm<UploadInput>,
    ) -> Result<PlainText, HttpApiError> {
        let attachment_names = input
            .attachment
            .iter()
            .map(|file| file.filename().unwrap_or("<none>"))
            .collect::<Vec<_>>()
            .join(",");
        Ok(PlainText(format!(
            "{}|{}|{}|{}:{}:{}|{}|{}",
            input.title,
            input.note.as_deref().unwrap_or("<none>"),
            input.tag.join(","),
            input.avatar.filename().unwrap_or("<none>"),
            input.avatar.content_type().unwrap_or("<none>"),
            String::from_utf8_lossy(input.avatar.as_bytes()),
            input
                .preview
                .as_ref()
                .and_then(FormFile::filename)
                .unwrap_or("<none>"),
            attachment_names,
        )))
    }
}

fn text_part(body: &mut Vec<u8>, name: &str, value: &[u8]) {
    body.extend_from_slice(
        format!("--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n")
            .as_bytes(),
    );
    body.extend_from_slice(value);
    body.extend_from_slice(b"\r\n");
}

fn file_part(body: &mut Vec<u8>, name: &str, filename: &str, content_type: &str, value: &[u8]) {
    body.extend_from_slice(format!(
        "--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"{name}\"; filename=\"{filename}\"\r\nContent-Type: {content_type}\r\n\r\n"
    ).as_bytes());
    body.extend_from_slice(value);
    body.extend_from_slice(b"\r\n");
}

fn finish_multipart(body: &mut Vec<u8>) {
    body.extend_from_slice(format!("--{BOUNDARY}--\r\n").as_bytes());
}

fn request(headers: &[(&str, &str)], body: &[u8]) -> Vec<u8> {
    let mut request = format!(
        "POST /typed-multipart/upload HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    )
    .into_bytes();
    for (name, value) in headers {
        request.extend_from_slice(name.as_bytes());
        request.extend_from_slice(b": ");
        request.extend_from_slice(value.as_bytes());
        request.extend_from_slice(b"\r\n");
    }
    request.extend_from_slice(b"\r\n");
    request.extend_from_slice(body);
    request
}

fn multipart_request(body: &[u8]) -> Vec<u8> {
    request(
        &[(
            "Content-Type",
            "multipart/form-data; boundary=LILY-TAE06-BOUNDARY",
        )],
        body,
    )
}

async fn send(address: SocketAddr, request: &[u8]) -> String {
    let mut connection = tokio::net::TcpStream::connect(address)
        .await
        .expect("typed multipart listener accepts a connection");
    connection
        .write_all(request)
        .await
        .expect("HTTP request is written");
    let mut response = Vec::new();
    tokio::time::timeout(
        Duration::from_secs(5),
        connection.read_to_end(&mut response),
    )
    .await
    .expect("HTTP response is bounded")
    .expect("HTTP response is readable");
    String::from_utf8(response).expect("fixture response is UTF-8")
}

fn assert_status(response: &str, status: u16) {
    assert!(
        response.starts_with(&format!("HTTP/1.1 {status}")),
        "{response}"
    );
}

fn required_fields(body: &mut Vec<u8>) {
    text_part(body, "title", b"bounded");
    file_part(
        body,
        "avatar",
        "sender-avatar.bin",
        "application/octet-stream",
        b"avatar",
    );
}

#[tokio::test]
async fn generated_adapter_binds_typed_multipart_and_rejects_schema_failures() {
    let reservation = TcpListener::bind("127.0.0.1:0").expect("reserve a loopback address");
    let address = reservation
        .local_addr()
        .expect("reserved listener has an address");
    drop(reservation);

    let transport = HttpTransportConfig {
        max_request_body_bytes: 4096,
        max_multipart_part_bytes: 16,
        max_multipart_parts: 16,
        max_multipart_metadata_bytes: 1024,
        ..HttpTransportConfig::default()
    };
    let app = AppBuilder::new(&address.to_string())
        .transport_config(transport)
        .build()
        .await
        .expect("typed multipart application builds");
    let health = app
        .extensions()
        .get_service::<HttpHealthService>(None)
        .await
        .expect("HTTP health service resolves");
    let cancellation = CancellationToken::new();
    let cancel = cancellation.clone();
    let server = tokio::spawn(async move { app.start_with_cancellation(cancellation).await });

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if health
                .snapshot()
                .expect("health snapshot remains readable")
                .ready
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("typed multipart listener becomes ready");

    let mut body = Vec::new();
    required_fields(&mut body);
    text_part(&mut body, "tag", b"first");
    text_part(&mut body, "tag", b"second");
    file_part(&mut body, "attachment", "one.bin", "text/plain", b"one");
    file_part(&mut body, "attachment", "two.bin", "text/plain", b"two");
    finish_multipart(&mut body);
    let response = send(address, &multipart_request(&body)).await;
    assert_status(&response, 200);
    assert!(response.ends_with(
        "bounded|<none>|first,second|sender-avatar.bin:application/octet-stream:avatar|<none>|one.bin,two.bin"
    ));

    let mut body = Vec::new();
    required_fields(&mut body);
    text_part(&mut body, "note", b"present");
    file_part(&mut body, "preview", "preview.bin", "image/png", b"png");
    finish_multipart(&mut body);
    let response = send(address, &multipart_request(&body)).await;
    assert_status(&response, 200);
    assert!(response.ends_with(
        "bounded|present||sender-avatar.bin:application/octet-stream:avatar|preview.bin|"
    ));

    let mut duplicate = Vec::new();
    required_fields(&mut duplicate);
    text_part(&mut duplicate, "title", b"again");
    finish_multipart(&mut duplicate);

    let mut missing = Vec::new();
    text_part(&mut missing, "title", b"bounded");
    finish_multipart(&mut missing);

    let mut unknown = Vec::new();
    required_fields(&mut unknown);
    text_part(&mut unknown, "unexpected", b"value");
    finish_multipart(&mut unknown);

    let mut invalid_text = Vec::new();
    file_part(
        &mut invalid_text,
        "avatar",
        "avatar.bin",
        "application/octet-stream",
        b"avatar",
    );
    text_part(&mut invalid_text, "title", b"\xff");
    finish_multipart(&mut invalid_text);

    for body in [duplicate, missing, unknown, invalid_text] {
        assert_status(&send(address, &multipart_request(&body)).await, 400);
    }

    let wrong_media = request(&[("Content-Type", "application/json")], b"{}");
    assert_status(&send(address, &wrong_media).await, 415);

    let malformed = request(
        &[("Content-Type", "multipart/form-data; boundary=missing")],
        b"not-a-multipart-representation",
    );
    assert_status(&send(address, &malformed).await, 400);

    let mut oversized = Vec::new();
    text_part(&mut oversized, "title", b"bounded");
    file_part(
        &mut oversized,
        "avatar",
        "large.bin",
        "application/octet-stream",
        &[b'x'; 17],
    );
    finish_multipart(&mut oversized);
    assert_status(&send(address, &multipart_request(&oversized)).await, 413);

    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("typed multipart server shutdown is bounded")
        .expect("typed multipart server task does not panic")
        .expect("typed multipart server shuts down cleanly");
}
