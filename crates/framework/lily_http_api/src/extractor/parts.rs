use std::fmt;
use std::future::{ready, Future};
use std::net::IpAddr;
use std::ops::Deref;
use std::sync::Arc;

use headers::Header;
use http::HeaderValue;
use lily_error::application::http_api::HttpApiError;
use lily_injection::Extensions;
use lily_web_core::{Principal, Request, RequestCookieJar};
use percent_encoding::percent_decode_str;
use serde::de::DeserializeOwned;

use super::{FromRequestParts, OptionalFromRequestParts};

/// Typed route-parameter binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Path<T>(pub T);

impl<T> Path<T> {
    /// Returns the bound route-parameter value.
    pub fn into_inner(self) -> T {
        self.0
    }
}

/// Typed URI-query binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Query<T>(pub T);

impl<T> Query<T> {
    /// Returns the bound query value.
    pub fn into_inner(self) -> T {
        self.0
    }
}

/// A request header decoded by the standard `headers` crate contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TypedHeader<T>(pub T)
where
    T: Header;

impl<T> TypedHeader<T>
where
    T: Header,
{
    /// Returns the decoded header value.
    pub fn into_inner(self) -> T {
        self.0
    }
}

/// Cheaply cloned shared view of the request's authoritative cookie jar.
#[derive(Clone, PartialEq, Eq)]
pub struct RequestCookies {
    jar: Arc<RequestCookieJar>,
}

impl RequestCookies {
    /// Borrows the immutable request cookie jar.
    #[must_use]
    pub fn as_jar(&self) -> &RequestCookieJar {
        self.jar.as_ref()
    }

    /// Returns shared ownership without cloning cookie names or values.
    #[must_use]
    pub fn into_inner(self) -> Arc<RequestCookieJar> {
        self.jar
    }
}

impl Deref for RequestCookies {
    type Target = RequestCookieJar;

    fn deref(&self) -> &Self::Target {
        self.as_jar()
    }
}

impl fmt::Debug for RequestCookies {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("RequestCookies")
            .field(self.as_jar())
            .finish()
    }
}

/// Owned clone of a value published into request-local storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Local<T>(pub T)
where
    T: Clone + Send + Sync + 'static;

impl<T> Local<T>
where
    T: Clone + Send + Sync + 'static,
{
    /// Returns the owned request-local value.
    pub fn into_inner(self) -> T {
        self.0
    }
}

/// Transport-authenticated effective client address.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientIp(pub IpAddr);

/// Stable, value-redacting failures produced by request-parts extractors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RequestPartsRejection {
    /// Route parameters could not be strictly decoded or bound.
    InvalidPath,
    /// Query parameters could not be bound to the requested type.
    InvalidQuery,
    /// The required typed header was absent.
    MissingHeader,
    /// At least one matching field-value was not valid for the typed header.
    InvalidHeader,
    /// The bounded request cookie header was malformed.
    InvalidCookies,
    /// No verified principal was attached to this request.
    MissingPrincipal,
    /// Required request-local state was not attached to this request.
    MissingLocal,
    /// The transport did not publish an effective client address.
    MissingClientIp,
}

impl fmt::Display for RequestPartsRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidPath => "typed route parameters are invalid",
            Self::InvalidQuery => "typed query parameters are invalid",
            Self::MissingHeader => "required request header is missing",
            Self::InvalidHeader => "typed request header is invalid",
            Self::InvalidCookies => "request cookie header is invalid",
            Self::MissingPrincipal => "verified request principal is missing",
            Self::MissingLocal => "required request-local state is missing",
            Self::MissingClientIp => "effective client address is missing",
        })
    }
}

impl std::error::Error for RequestPartsRejection {}

impl From<RequestPartsRejection> for HttpApiError {
    fn from(rejection: RequestPartsRejection) -> Self {
        match rejection {
            RequestPartsRejection::InvalidPath => {
                Self::InvalidPathParameter("typed route parameters are invalid".to_string())
            }
            RequestPartsRejection::InvalidQuery => {
                Self::InvalidQueryString("typed query parameters are invalid".to_string())
            }
            RequestPartsRejection::MissingHeader => {
                Self::InvalidHttpHeader("required typed request header is missing".to_string())
            }
            RequestPartsRejection::InvalidHeader => {
                Self::InvalidHttpHeader("typed request header is invalid".to_string())
            }
            RequestPartsRejection::InvalidCookies => {
                Self::InvalidHttpHeader("request cookie header is invalid".to_string())
            }
            RequestPartsRejection::MissingPrincipal => {
                Self::MissingAuthentication("verified request principal is missing".to_string())
            }
            RequestPartsRejection::MissingLocal => {
                Self::InternalError("required request-local state is unavailable".to_string())
            }
            RequestPartsRejection::MissingClientIp => {
                Self::InternalError("effective client address is unavailable".to_string())
            }
        }
    }
}

impl<T> FromRequestParts for Path<T>
where
    T: DeserializeOwned + Send,
{
    type Rejection = RequestPartsRejection;

    fn from_request_parts(
        request: &mut Request,
        _extensions: &Extensions,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(bind_path(request).map(Self))
    }
}

impl<T> FromRequestParts for Query<T>
where
    T: DeserializeOwned + Send,
{
    type Rejection = RequestPartsRejection;

    fn from_request_parts(
        request: &mut Request,
        _extensions: &Extensions,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(bind_query(request).map(Self))
    }
}

impl<T> FromRequestParts for TypedHeader<T>
where
    T: Header + Send,
{
    type Rejection = RequestPartsRejection;

    fn from_request_parts(
        request: &mut Request,
        _extensions: &Extensions,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(
            decode_header::<T>(request)
                .and_then(|value| value.ok_or(RequestPartsRejection::MissingHeader))
                .map(Self),
        )
    }
}

impl<T> OptionalFromRequestParts for TypedHeader<T>
where
    T: Header + Send,
{
    type Rejection = RequestPartsRejection;

    fn from_request_parts(
        request: &mut Request,
        _extensions: &Extensions,
    ) -> impl Future<Output = Result<Option<Self>, Self::Rejection>> + Send {
        ready(decode_header::<T>(request).map(|value| value.map(Self)))
    }
}

impl FromRequestParts for RequestCookies {
    type Rejection = RequestPartsRejection;

    fn from_request_parts(
        request: &mut Request,
        _extensions: &Extensions,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(
            request
                .shared_cookie_jar()
                .map(|jar| Self { jar })
                .map_err(|_| RequestPartsRejection::InvalidCookies),
        )
    }
}

impl FromRequestParts for Principal {
    type Rejection = RequestPartsRejection;

    fn from_request_parts(
        request: &mut Request,
        _extensions: &Extensions,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(
            request
                .principal()
                .cloned()
                .ok_or(RequestPartsRejection::MissingPrincipal),
        )
    }
}

impl OptionalFromRequestParts for Principal {
    type Rejection = RequestPartsRejection;

    fn from_request_parts(
        request: &mut Request,
        _extensions: &Extensions,
    ) -> impl Future<Output = Result<Option<Self>, Self::Rejection>> + Send {
        ready(Ok(request.principal().cloned()))
    }
}

impl<T> FromRequestParts for Local<T>
where
    T: Clone + Send + Sync + 'static,
{
    type Rejection = RequestPartsRejection;

    fn from_request_parts(
        request: &mut Request,
        _extensions: &Extensions,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(
            request
                .local()
                .get::<T>()
                .cloned()
                .map(Self)
                .ok_or(RequestPartsRejection::MissingLocal),
        )
    }
}

impl<T> OptionalFromRequestParts for Local<T>
where
    T: Clone + Send + Sync + 'static,
{
    type Rejection = RequestPartsRejection;

    fn from_request_parts(
        request: &mut Request,
        _extensions: &Extensions,
    ) -> impl Future<Output = Result<Option<Self>, Self::Rejection>> + Send {
        ready(Ok(request.local().get::<T>().cloned().map(Self)))
    }
}

impl FromRequestParts for ClientIp {
    type Rejection = RequestPartsRejection;

    fn from_request_parts(
        request: &mut Request,
        _extensions: &Extensions,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(
            request
                .client_ip()
                .map(Self)
                .ok_or(RequestPartsRejection::MissingClientIp),
        )
    }
}

impl OptionalFromRequestParts for ClientIp {
    type Rejection = RequestPartsRejection;

    fn from_request_parts(
        request: &mut Request,
        _extensions: &Extensions,
    ) -> impl Future<Output = Result<Option<Self>, Self::Rejection>> + Send {
        ready(Ok(request.client_ip().map(Self)))
    }
}

fn bind_path<T>(request: &Request) -> Result<T, RequestPartsRejection>
where
    T: DeserializeOwned,
{
    let mut parameters = request.params().iter().collect::<Vec<_>>();
    parameters.sort_unstable_by(|(left, _), (right, _)| left.as_str().cmp(right.as_str()));

    let mut encoded = url::form_urlencoded::Serializer::new(String::new());
    for (name, value) in parameters {
        let value = strict_decode_path_component(value.as_str())?;
        encoded.append_pair(name.as_str(), value.as_ref());
    }
    serde_html_form::from_str(&encoded.finish()).map_err(|_| RequestPartsRejection::InvalidPath)
}

fn bind_query<T>(request: &Request) -> Result<T, RequestPartsRejection>
where
    T: DeserializeOwned,
{
    let mut encoded = url::form_urlencoded::Serializer::new(String::new());
    for (name, value) in request.queries() {
        encoded.append_pair(name.as_str(), value.as_str());
    }
    serde_html_form::from_str(&encoded.finish()).map_err(|_| RequestPartsRejection::InvalidQuery)
}

fn strict_decode_path_component(
    value: &str,
) -> Result<std::borrow::Cow<'_, str>, RequestPartsRejection> {
    let bytes = value.as_bytes();
    let mut position = 0;
    while position < bytes.len() {
        if bytes[position] == b'%' {
            if position + 2 >= bytes.len()
                || !bytes[position + 1].is_ascii_hexdigit()
                || !bytes[position + 2].is_ascii_hexdigit()
            {
                return Err(RequestPartsRejection::InvalidPath);
            }
            position += 3;
        } else {
            position += 1;
        }
    }

    percent_decode_str(value)
        .decode_utf8()
        .map_err(|_| RequestPartsRejection::InvalidPath)
}

fn decode_header<T>(request: &Request) -> Result<Option<T>, RequestPartsRejection>
where
    T: Header,
{
    let name = T::name().as_str();
    let values = request
        .headers()
        .iter()
        .filter(|header| header.name.eq_ignore_ascii_case(name))
        .map(|header| {
            HeaderValue::from_bytes(header.value.as_bytes())
                .map_err(|_| RequestPartsRejection::InvalidHeader)
        })
        .collect::<Result<Vec<_>, _>>()?;

    if values.is_empty() {
        return Ok(None);
    }

    T::decode(&mut values.iter())
        .map(Some)
        .map_err(|_| RequestPartsRejection::InvalidHeader)
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::Arc;

    use headers::{Error as HeaderError, Header, HeaderName, HeaderValue};
    use lily_core::RawHeader;
    use lily_web_core::{Principal, Request, RequestConnectionInfo, RequestCookieError};
    use serde::Deserialize;
    use serde_json::Map;
    use uuid::Uuid;

    use super::{ClientIp, Local, Path, Query, RequestCookies, RequestPartsRejection, TypedHeader};
    use crate::extractor::extract_request_parts;
    use crate::{ApplicationContainer, HttpApiError, OptionalFromRequestParts};

    const UUID_VALUE: &str = "550e8400-e29b-41d4-a716-446655440000";
    const SECRET_VALUE: &str = "credential-secret-7f31";

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    struct PathInput {
        id: Uuid,
        slug: String,
    }

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    struct QueryInput {
        enabled: bool,
        count: u32,
        optional: Option<u16>,
        #[serde(default)]
        tag: Vec<String>,
        request_id: Uuid,
    }

    #[derive(Debug, Deserialize)]
    struct ScalarQuery {
        count: u32,
    }

    #[derive(Debug, PartialEq, Eq)]
    struct MultiValueHeader(Vec<String>);

    impl Header for MultiValueHeader {
        fn name() -> &'static HeaderName {
            static NAME: HeaderName = HeaderName::from_static("x-lily-values");
            &NAME
        }

        fn decode<'value, Values>(values: &mut Values) -> Result<Self, HeaderError>
        where
            Values: Iterator<Item = &'value HeaderValue>,
        {
            let decoded = values
                .map(|value| value.to_str().map(str::to_owned))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| HeaderError::invalid())?;
            if decoded.is_empty() {
                return Err(HeaderError::invalid());
            }
            Ok(Self(decoded))
        }

        fn encode<Values>(&self, values: &mut Values)
        where
            Values: Extend<HeaderValue>,
        {
            values.extend(
                self.0
                    .iter()
                    .map(|value| HeaderValue::from_str(value).expect("fixture header is valid")),
            );
        }
    }

    fn raw_header(name: &str, value: &str) -> RawHeader {
        RawHeader {
            name: name.to_string(),
            value: value.to_string(),
            line_number: 1,
            raw_line: String::new(),
        }
    }

    async fn request(path: &str, headers: Vec<RawHeader>) -> Request {
        Request::from_transport_parts("GET".to_string(), path.to_string(), headers, &[])
            .await
            .expect("fixture request is valid")
    }

    #[tokio::test]
    async fn path_and_query_bind_uuid_scalars_optionals_and_collections_fail_closed() {
        let container = ApplicationContainer::build()
            .await
            .expect("test container builds");
        let extensions = container.services();

        let mut valid = request(
            &format!(
                "/resource?enabled=true&count=42&tag=first&optional=7&tag=second&request_id={UUID_VALUE}"
            ),
            Vec::new(),
        )
        .await;
        valid.set_params(vec![
            ("id".to_string(), UUID_VALUE.to_string()),
            ("slug".to_string(), "%252F+literal".to_string()),
        ]);

        let Path(path) = extract_request_parts::<Path<PathInput>>(&mut valid, &extensions)
            .await
            .expect("strict path binding succeeds");
        assert_eq!(path.id, Uuid::parse_str(UUID_VALUE).unwrap());
        assert_eq!(path.slug, "%2F+literal");

        let Query(query) = extract_request_parts::<Query<QueryInput>>(&mut valid, &extensions)
            .await
            .expect("query binding succeeds");
        assert!(query.enabled);
        assert_eq!(query.count, 42);
        assert_eq!(query.optional, Some(7));
        assert_eq!(query.tag, ["first", "second"]);
        assert_eq!(query.request_id, Uuid::parse_str(UUID_VALUE).unwrap());

        for malformed_path in ["%", "%2", "%GG", "%FF"] {
            let mut malformed = request("/resource", Vec::new()).await;
            malformed.set_params(vec![
                ("id".to_string(), UUID_VALUE.to_string()),
                ("slug".to_string(), malformed_path.to_string()),
            ]);
            let error = extract_request_parts::<Path<PathInput>>(&mut malformed, &extensions)
                .await
                .expect_err("malformed path input is rejected");
            assert_eq!(
                error,
                HttpApiError::from(RequestPartsRejection::InvalidPath)
            );
        }

        let mut scalar = request("/resource?count=9", Vec::new()).await;
        let Query(scalar) = extract_request_parts::<Query<ScalarQuery>>(&mut scalar, &extensions)
            .await
            .expect("a scalar query binds");
        assert_eq!(scalar.count, 9);

        let mut duplicate = request("/resource?count=1&count=2", Vec::new()).await;
        let error = extract_request_parts::<Query<ScalarQuery>>(&mut duplicate, &extensions)
            .await
            .expect_err("duplicate scalar query input is rejected");
        assert_eq!(
            error,
            HttpApiError::from(RequestPartsRejection::InvalidQuery)
        );

        let mut secret = request(&format!("/resource?count={SECRET_VALUE}"), Vec::new()).await;
        let error = extract_request_parts::<Query<ScalarQuery>>(&mut secret, &extensions)
            .await
            .expect_err("malformed scalar is rejected");
        for output in [
            format!("{error}"),
            format!("{error:?}"),
            String::from_utf8(error.to_public_json().unwrap()).unwrap(),
        ] {
            assert!(!output.contains(SECRET_VALUE));
        }

        container.close().await.expect("test container closes");
    }

    #[tokio::test]
    async fn typed_headers_preserve_wire_order_and_optional_absence_not_malformed_input() {
        let container = ApplicationContainer::build()
            .await
            .expect("test container builds");
        let extensions = container.services();

        let mut with_values = request(
            "/headers",
            vec![
                raw_header("X-Lily-Values", "first"),
                raw_header("x-lily-values", "second"),
            ],
        )
        .await;
        let TypedHeader(header) =
            extract_request_parts::<TypedHeader<MultiValueHeader>>(&mut with_values, &extensions)
                .await
                .expect("multi-value header decodes");
        assert_eq!(header.0, ["first", "second"]);

        let mut missing = request("/headers", Vec::new()).await;
        let required =
            extract_request_parts::<TypedHeader<MultiValueHeader>>(&mut missing, &extensions)
                .await
                .expect_err("required header absence is rejected");
        assert_eq!(
            required,
            HttpApiError::from(RequestPartsRejection::MissingHeader)
        );
        let optional = extract_request_parts::<Option<TypedHeader<MultiValueHeader>>>(
            &mut missing,
            &extensions,
        )
        .await
        .expect("optional missing header succeeds");
        assert!(optional.is_none());

        let mut malformed = request(
            "/headers",
            vec![raw_header("X-Lily-Values", "header-secret\ninvalid")],
        )
        .await;
        let rejection =
            <TypedHeader<MultiValueHeader> as OptionalFromRequestParts>::from_request_parts(
                &mut malformed,
                &extensions,
            )
            .await
            .expect_err("present malformed optional header is rejected");
        assert_eq!(rejection, RequestPartsRejection::InvalidHeader);

        container.close().await.expect("test container closes");
    }

    #[tokio::test]
    async fn cookie_principal_local_and_client_ip_extract_owned_shared_state() {
        let container = ApplicationContainer::build()
            .await
            .expect("test container builds");
        let extensions = container.services();

        let mut absent = request(
            "/context",
            vec![raw_header("X-Forwarded-For", "198.51.100.77")],
        )
        .await;
        let empty = extract_request_parts::<RequestCookies>(&mut absent, &extensions)
            .await
            .expect("missing Cookie header is an empty jar");
        assert!(empty.is_empty());
        assert!(
            extract_request_parts::<Option<Principal>>(&mut absent, &extensions)
                .await
                .expect("optional principal absence succeeds")
                .is_none()
        );
        assert!(
            extract_request_parts::<Option<Local<Arc<String>>>>(&mut absent, &extensions)
                .await
                .expect("optional local absence succeeds")
                .is_none()
        );
        assert!(
            extract_request_parts::<Option<ClientIp>>(&mut absent, &extensions)
                .await
                .expect("optional client IP absence succeeds")
                .is_none()
        );
        assert_eq!(
            extract_request_parts::<Principal>(&mut absent, &extensions)
                .await
                .expect_err("required principal absence is rejected")
                .http_status()
                .0,
            401
        );
        assert_eq!(
            extract_request_parts::<Local<Arc<String>>>(&mut absent, &extensions)
                .await
                .expect_err("required local absence is rejected")
                .http_status()
                .0,
            500
        );
        assert_eq!(
            extract_request_parts::<ClientIp>(&mut absent, &extensions)
                .await
                .expect_err("required client IP absence is rejected")
                .http_status()
                .0,
            500
        );

        let tenant = Arc::new("tenant-a".to_string());
        absent.set_principal(Principal::new("user-42", [], [], Map::new()));
        absent.local_mut().insert(Arc::clone(&tenant));
        let expected_ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 9));
        absent.set_connection_info(RequestConnectionInfo::from_trusted_transport(
            Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            Some(expected_ip),
            true,
        ));

        let principal = extract_request_parts::<Principal>(&mut absent, &extensions)
            .await
            .expect("verified principal extracts");
        assert_eq!(principal.subject(), "user-42");
        let Local(extracted_tenant) =
            extract_request_parts::<Local<Arc<String>>>(&mut absent, &extensions)
                .await
                .expect("request-local state extracts");
        assert!(Arc::ptr_eq(&tenant, &extracted_tenant));
        let ClientIp(client_ip) = extract_request_parts::<ClientIp>(&mut absent, &extensions)
            .await
            .expect("transport client IP extracts");
        // The extractor trusts only transport-published identity and does not
        // reinterpret the conflicting X-Forwarded-For field above.
        assert_eq!(client_ip, expected_ip);

        let mut cookies = request(
            "/cookies",
            vec![raw_header(
                "Cookie",
                "session=opaque%2Fvalue; session=second",
            )],
        )
        .await;
        let first = extract_request_parts::<RequestCookies>(&mut cookies, &extensions)
            .await
            .expect("cookie jar extracts");
        let second = extract_request_parts::<RequestCookies>(&mut cookies, &extensions)
            .await
            .expect("cached cookie jar extracts");
        let first = first.into_inner();
        let second = second.into_inner();
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(first.get("session"), Err(RequestCookieError::Duplicate));

        let mut malformed = request(
            "/cookies",
            vec![raw_header(
                "Cookie",
                &format!("session={SECRET_VALUE}; broken"),
            )],
        )
        .await;
        let error = extract_request_parts::<RequestCookies>(&mut malformed, &extensions)
            .await
            .expect_err("malformed cookie is rejected");
        assert_eq!(error.http_status().0, 400);
        assert!(!format!("{error:?}").contains(SECRET_VALUE));
        assert!(!String::from_utf8(error.to_public_json().unwrap())
            .unwrap()
            .contains(SECRET_VALUE));

        container.close().await.expect("test container closes");
    }
}
