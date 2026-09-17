use std::{
    fmt,
    io::{self, Write},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use bytes::Bytes;
use lily_http_client::{
    body::{BinaryBody, Body, FormBody, JsonBody, MultipartBody, TextBody},
    client::{ClientConfig, HttpClient, HttpClientBuilder},
    error::HttpClientError,
    header::HeaderParser,
    request::{RequestBuilder, RequestConfig},
    response::{ResponseBuilder, ResponseMetadata},
    StatusCode,
};
use tracing::instrument::WithSubscriber as _;
use tracing_subscriber::fmt::{format::FmtSpan, MakeWriter};
use url::Url;

const URL_USERINFO: &str = "LILY_SECRET_IN_URL_USERINFO";
const QUERY: &str = "LILY_SECRET_IN_QUERY";
const AUTH_HEADER: &str = "LILY_SECRET_IN_AUTH_HEADER";
const COOKIE: &str = "LILY_SECRET_IN_COOKIE";
const CUSTOM_HEADER: &str = "LILY_SECRET_IN_CUSTOM_HEADER";
const REQUEST_BODY: &str = "LILY_SECRET_IN_REQUEST_BODY";
const RESPONSE_BODY: &str = "LILY_SECRET_IN_RESPONSE_BODY";
// Proxy configuration is intentionally absent from the production API. Keep
// its sentinel in the error matrix so lower-level proxy-shaped diagnostics
// cannot regress the safe error representations.
const PROXY_URL: &str = "LILY_SECRET_IN_PROXY_URL";

const SENTINELS: [&str; 8] = [
    URL_USERINFO,
    QUERY,
    AUTH_HEADER,
    COOKIE,
    CUSTOM_HEADER,
    REQUEST_BODY,
    RESPONSE_BODY,
    PROXY_URL,
];

fn assert_contains_no_sentinel(surface: &str) {
    for sentinel in SENTINELS {
        assert!(
            !surface.contains(sentinel),
            "safe representation leaked {sentinel}: {surface}"
        );
    }
}

#[tokio::test]
async fn request_safe_representations_preserve_explicit_and_wire_access() {
    let credential_url =
        format!("https://{URL_USERINFO}:password@example.test/private?token={QUERY}");
    let error = RequestBuilder::post(&credential_url).unwrap_err();
    assert_eq!(error.diagnostic_code(), "REQUEST_URL_CREDENTIALS");
    assert_contains_no_sentinel(&format!("{error:?}"));
    assert_contains_no_sentinel(&error.to_string());

    let url = format!("https://example.test/private?token={QUERY}");
    let mut builder = RequestBuilder::post(&url).unwrap();
    builder
        .header("Authorization", AUTH_HEADER)
        .unwrap()
        .header("Cookie", COOKIE)
        .unwrap()
        .header("X-Custom", CUSTOM_HEADER)
        .unwrap()
        .text(REQUEST_BODY);

    assert_contains_no_sentinel(&format!("{builder:?}"));

    let mut request = builder.build().unwrap();
    assert_contains_no_sentinel(&format!("{request:?}"));

    assert!(request.url().username().is_empty());
    let expected_query = format!("token={QUERY}");
    assert_eq!(request.url().query(), Some(expected_query.as_str()));
    assert_eq!(request.headers().get("Authorization"), Some(AUTH_HEADER));
    assert_eq!(request.headers().get("Cookie"), Some(COOKIE));
    assert_eq!(request.headers().get("X-Custom"), Some(CUSTOM_HEADER));

    let wire_headers = HeaderParser::format_headers(request.headers());
    assert!(wire_headers.contains(AUTH_HEADER));
    assert!(wire_headers.contains(COOKIE));
    assert!(wire_headers.contains(CUSTOM_HEADER));

    let body = request.take_body().unwrap().to_bytes().await.unwrap();
    assert_eq!(body, Bytes::from_static(REQUEST_BODY.as_bytes()));
}

#[tokio::test]
async fn response_safe_representations_preserve_explicit_access() {
    let final_url = Url::parse(&format!(
        "https://{URL_USERINFO}:password@example.test/private?token={QUERY}"
    ))
    .unwrap();
    let metadata = ResponseMetadata::new()
        .with_remote_addr(RESPONSE_BODY.to_string())
        .with_local_addr(CUSTOM_HEADER.to_string());
    let builder = ResponseBuilder::new()
        .status(StatusCode::OK)
        .header("Set-Cookie", COOKIE)
        .unwrap()
        .text(RESPONSE_BODY)
        .url(final_url)
        .metadata(metadata);

    assert_contains_no_sentinel(&format!("{builder:?}"));

    let mut response = builder.build();
    assert_contains_no_sentinel(&format!("{response:?}"));
    assert_eq!(response.headers().get("Set-Cookie"), Some(COOKIE));
    assert_eq!(response.url().unwrap().username(), URL_USERINFO);
    assert_eq!(response.text().await.unwrap(), RESPONSE_BODY);
}

#[test]
fn config_and_client_builder_debug_are_redacted() {
    let request_config = RequestConfig::production();
    assert_contains_no_sentinel(&format!("{request_config:?}"));

    let client_config = ClientConfig::default()
        .with_base_address(format!(
            "https://{URL_USERINFO}:password@example.test/?token={QUERY}"
        ))
        .try_with_default_header("Authorization", AUTH_HEADER)
        .unwrap()
        .try_with_user_agent(CUSTOM_HEADER)
        .unwrap();
    assert_contains_no_sentinel(&format!("{client_config:?}"));

    let client_builder = HttpClientBuilder::new()
        .base_address(format!(
            "https://{URL_USERINFO}:password@example.test/?token={QUERY}"
        ))
        .user_agent(CUSTOM_HEADER)
        .unwrap()
        .default_header("Authorization", AUTH_HEADER)
        .unwrap();
    assert_contains_no_sentinel(&format!("{client_builder:?}"));

    let client = HttpClient::try_with_config(
        ClientConfig::default()
            .with_base_address("https://api.example.test/root")
            .try_with_default_header("Authorization", AUTH_HEADER)
            .unwrap(),
    )
    .unwrap();
    let request_builder = client.get("/safe").unwrap();
    assert_contains_no_sentinel(&format!("{request_builder:?}"));
}

#[test]
fn built_in_body_debug_never_contains_payload_or_custom_metadata() {
    let bodies = [
        format!("{:?}", TextBody::with_content_type(REQUEST_BODY, QUERY)),
        format!(
            "{:?}",
            BinaryBody::with_content_type(Bytes::from_static(REQUEST_BODY.as_bytes()), QUERY)
        ),
        format!("{:?}", JsonBody::from_string(REQUEST_BODY)),
        format!("{:?}", FormBody::new().field(CUSTOM_HEADER, REQUEST_BODY)),
        format!(
            "{:?}",
            MultipartBody::with_boundary(QUERY)
                .unwrap()
                .text_field(CUSTOM_HEADER, REQUEST_BODY)
                .unwrap()
                .file_field(
                    AUTH_HEADER,
                    COOKIE,
                    format!("application/{CUSTOM_HEADER}"),
                    Bytes::from_static(RESPONSE_BODY.as_bytes()),
                )
                .unwrap()
        ),
    ];

    for body in bodies {
        assert_contains_no_sentinel(&body);
    }
}

#[derive(Debug)]
struct CountedLengthBody {
    length_calls: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl Body for CountedLengthBody {
    fn content_type(&self) -> Option<&str> {
        None
    }

    fn content_length(&self) -> Option<usize> {
        let previous = self.length_calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(previous, 0, "Debug queried the body length");
        Some(0)
    }

    async fn to_bytes(&mut self) -> lily_http_client::Result<Bytes> {
        Ok(Bytes::new())
    }
}

#[test]
fn request_and_response_debug_never_query_dynamic_body_metadata() {
    let request_calls = Arc::new(AtomicUsize::new(0));
    let mut request_builder = RequestBuilder::post("https://example.test").unwrap();
    request_builder.body(Box::new(CountedLengthBody {
        length_calls: Arc::clone(&request_calls),
    }));

    let _ = format!("{request_builder:?}");
    assert_eq!(request_calls.load(Ordering::SeqCst), 0);
    let request = request_builder.build().unwrap();
    assert_eq!(request_calls.load(Ordering::SeqCst), 1);
    let _ = format!("{request:?}");
    assert_eq!(request_calls.load(Ordering::SeqCst), 1);

    let response_calls = Arc::new(AtomicUsize::new(0));
    let response_builder = ResponseBuilder::new().body(Box::new(CountedLengthBody {
        length_calls: Arc::clone(&response_calls),
    }));
    let _ = format!("{response_builder:?}");
    assert_eq!(response_calls.load(Ordering::SeqCst), 0);
    let response = response_builder.build();
    let _ = format!("{response:?}");
    assert_eq!(response_calls.load(Ordering::SeqCst), 0);
}

fn error_chain(error: &HttpClientError) -> String {
    let mut chain = String::new();
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(error) = current {
        chain.push_str(&error.to_string());
        current = error.source();
    }
    chain
}

#[test]
fn error_display_debug_chain_and_conversions_are_secret_safe() {
    let payload = SENTINELS.join("|");
    let errors = vec![
        HttpClientError::Connection(payload.clone()),
        HttpClientError::ConnectTimeout,
        HttpClientError::Tls(payload.clone()),
        HttpClientError::InvalidUrl(payload.clone()),
        HttpClientError::HttpParsing(payload.clone()),
        HttpClientError::RequestBuilding(payload.clone()),
        HttpClientError::ResponseParsing(payload.clone()),
        HttpClientError::Json(payload.clone()),
        HttpClientError::InvalidHeader(payload.clone()),
        HttpClientError::InvalidMethod(payload.clone()),
        HttpClientError::BodyReading(payload.clone()),
        HttpClientError::Body(payload.clone()),
        HttpClientError::Client(payload.clone()),
        HttpClientError::Server {
            status: 500,
            message: payload.clone(),
        },
        HttpClientError::ClientStatus {
            status: 401,
            message: payload.clone(),
        },
        HttpClientError::Io(payload.clone()),
        HttpClientError::Parse(payload.clone()),
        HttpClientError::Configuration(payload.clone()),
        HttpClientError::LimitExceeded {
            resource: payload.clone(),
            limit: 42,
        },
        HttpClientError::UnsupportedConfiguration(payload.clone()),
        HttpClientError::ProtocolNegotiation {
            requested: payload.clone(),
            negotiated: payload.clone(),
        },
        HttpClientError::RedirectRejected(payload),
    ];

    for error in errors {
        assert_contains_no_sentinel(error.diagnostic_code());
        assert_contains_no_sentinel(&error.to_string());
        assert_contains_no_sentinel(&format!("{error:?}"));
        assert_contains_no_sentinel(&error_chain(&error));

        let api_error: lily_error::application::http_api::HttpApiError = error.into();
        assert_contains_no_sentinel(&api_error.to_string());
        assert_contains_no_sentinel(&format!("{api_error:?}"));
    }
}

#[derive(Clone, Default)]
struct CapturedOutput(Arc<Mutex<Vec<u8>>>);

impl CapturedOutput {
    fn text(&self) -> String {
        let bytes = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        String::from_utf8(bytes).unwrap()
    }

    fn append(&self, buffer: &[u8]) {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .extend_from_slice(buffer);
    }
}

struct CapturedWriter(CapturedOutput);

impl Write for CapturedWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.0.append(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for CapturedOutput {
    type Writer = CapturedWriter;

    fn make_writer(&'a self) -> Self::Writer {
        CapturedWriter(self.clone())
    }
}

impl fmt::Debug for CapturedOutput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("CapturedOutput").finish()
    }
}

#[tokio::test(flavor = "current_thread")]
async fn request_tracing_uses_only_the_bounded_allow_list() {
    let captured = CapturedOutput::default();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_writer(captured.clone())
        .with_ansi(false)
        .without_time()
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .finish();

    let client = HttpClientBuilder::new()
        .connect_timeout(Duration::from_millis(100))
        .request_timeout(Duration::from_millis(250))
        .try_build()
        .unwrap();
    let url = format!("http://127.0.0.1:9/private?token={QUERY}");
    let mut builder = RequestBuilder::post(url).unwrap();
    builder
        .header("Authorization", AUTH_HEADER)
        .unwrap()
        .header("Cookie", COOKIE)
        .unwrap()
        .header("X-Custom", CUSTOM_HEADER)
        .unwrap()
        .text(REQUEST_BODY);

    let _ = client
        .execute(builder.build().unwrap())
        .with_subscriber(subscriber)
        .await;

    let output = captured.text();
    assert!(!output.is_empty(), "test subscriber captured no events");
    assert_contains_no_sentinel(&output);
}
