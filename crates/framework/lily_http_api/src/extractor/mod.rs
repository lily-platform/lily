//! Typed action extraction contracts.
//!
//! These traits separate request metadata extraction from the single
//! request-body consumer. Implementations are statically dispatched by the
//! generated controller adapter; the contract does not require boxed futures
//! or extractor trait objects.

use std::future::Future;

use lily_error::application::http_api::HttpApiError;
use lily_injection::Extensions;
use lily_web_core::Request;

mod body;
mod multipart;
mod parts;
mod service;

pub use body::{BodyStream, Form, RawBody, RequestBodyRejection};
pub use multipart::{
    multipart_file_field, multipart_text_field, reserve_multipart_slot, FormFile,
    FromMultipartForm, MultipartForm, MultipartFormOpenApi, MultipartFormRejection,
};
pub use parts::{ClientIp, Local, Path, Query, RequestCookies, RequestPartsRejection, TypedHeader};
pub use service::Service;
#[doc(hidden)]
pub use service::{extract_service, ServiceRequestExtractor};

/// Extracts one owned value without consuming the request body.
///
/// Implementations may inspect and update request metadata needed to produce
/// their owned result, and receive read-only access to the application's DI
/// provider. They must not retain either reference after the returned future
/// completes.
///
/// Rejections cross the controller boundary only through `Into<HttpApiError>`.
/// Implementations must map untrusted values and secrets to a bounded,
/// disclosure-safe HTTP error rather than embedding them in the result.
pub trait FromRequestParts: Sized {
    /// Typed extraction failure converted at Lily's safe HTTP boundary.
    type Rejection: Into<HttpApiError>;

    /// Extract an owned value from request metadata or request-scoped context.
    fn from_request_parts(
        request: &mut Request,
        extensions: &Extensions,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send;
}

impl FromRequestParts for lily_cancellation::ExecutionCancellation {
    type Rejection = HttpApiError;

    async fn from_request_parts(
        request: &mut Request,
        _extensions: &Extensions,
    ) -> Result<Self, Self::Rejection> {
        Ok(request.execution_cancellation())
    }
}

/// Extracts an optional request-parts value while preserving malformed input.
///
/// `Ok(None)` is reserved for semantic absence. If input is present but
/// malformed, implementations must return `Err`; the blanket `Option<E>`
/// integration deliberately does not turn that rejection into absence.
pub trait OptionalFromRequestParts: Sized {
    /// Typed extraction failure converted at Lily's safe HTTP boundary.
    type Rejection: Into<HttpApiError>;

    /// Extract an optional owned value from request metadata.
    fn from_request_parts(
        request: &mut Request,
        extensions: &Extensions,
    ) -> impl Future<Output = Result<Option<Self>, Self::Rejection>> + Send;
}

/// Makes `Option<E>` a parts extractor only when `E` defines semantic absence.
impl<E> FromRequestParts for Option<E>
where
    E: OptionalFromRequestParts,
{
    type Rejection = E::Rejection;

    fn from_request_parts(
        request: &mut Request,
        extensions: &Extensions,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        <E as OptionalFromRequestParts>::from_request_parts(request, extensions)
    }
}

/// Extracts one owned value using the action's single request-body slot.
///
/// Implementations must preserve the request's bounded, fail-closed body
/// authority. A body extractor may buffer or stream the body, but it must not
/// introduce an independent replay or an unbounded read path.
pub trait FromRequest: Sized {
    /// Typed extraction failure converted at Lily's safe HTTP boundary.
    type Rejection: Into<HttpApiError>;

    /// Extract an owned value using the request body.
    fn from_request(
        request: &mut Request,
        extensions: &Extensions,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send;
}

/// Monomorphized safe-boundary adapter used by generated controller code.
#[doc(hidden)]
pub async fn extract_request_parts<E>(
    request: &mut Request,
    extensions: &Extensions,
) -> Result<E, HttpApiError>
where
    E: FromRequestParts,
{
    E::from_request_parts(request, extensions)
        .await
        .map_err(Into::into)
}

/// Monomorphized body adapter used by generated controller code.
#[doc(hidden)]
pub async fn extract_request<E>(
    request: &mut Request,
    extensions: &Extensions,
) -> Result<E, HttpApiError>
where
    E: FromRequest,
{
    E::from_request(request, extensions)
        .await
        .map_err(Into::into)
}

/// Type-level selector for a terminal request-parts extractor.
#[doc(hidden)]
pub enum TerminalRequestParts {}

/// Type-level selector for a terminal request-body extractor.
#[doc(hidden)]
pub enum TerminalRequestBody {}

/// Resolves an action's last typed argument without dynamic dispatch.
///
/// The marker keeps the `FromRequestParts` and `FromRequest` implementations
/// disjoint while allowing Rust trait resolution to select the contract for a
/// user-defined extractor type.
#[doc(hidden)]
pub trait TerminalRequestExtractor<Mode>: Sized {
    /// Extracts the terminal typed action argument.
    fn extract_terminal(
        request: &mut Request,
        extensions: &Extensions,
    ) -> impl Future<Output = Result<Self, HttpApiError>> + Send;
}

impl<E> TerminalRequestExtractor<TerminalRequestParts> for E
where
    E: FromRequestParts,
{
    fn extract_terminal(
        request: &mut Request,
        extensions: &Extensions,
    ) -> impl Future<Output = Result<Self, HttpApiError>> + Send {
        extract_request_parts::<E>(request, extensions)
    }
}

impl<E> TerminalRequestExtractor<TerminalRequestBody> for E
where
    E: FromRequest,
{
    fn extract_terminal(
        request: &mut Request,
        extensions: &Extensions,
    ) -> impl Future<Output = Result<Self, HttpApiError>> + Send {
        extract_request::<E>(request, extensions)
    }
}

/// Monomorphized selector used for the last typed action argument.
#[doc(hidden)]
pub async fn extract_terminal_request<E, Mode>(
    request: &mut Request,
    extensions: &Extensions,
) -> Result<E, HttpApiError>
where
    E: TerminalRequestExtractor<Mode>,
{
    E::extract_terminal(request, extensions).await
}

#[cfg(test)]
mod tests {
    use std::future::Future;

    use super::{extract_request, extract_request_parts};
    use crate::{
        ApplicationContainer, Extensions, FromRequest, FromRequestParts, HttpApiError,
        OptionalFromRequestParts, Request,
    };

    const SECRET_INPUT: &str = "customer-secret-9f43";

    #[derive(Debug, PartialEq, Eq)]
    struct RequiredProbe {
        value: String,
        provider_address: usize,
    }

    #[derive(Debug, PartialEq, Eq)]
    struct OptionalProbe(String);

    #[derive(Debug, PartialEq, Eq)]
    struct BodyProbe(usize);

    #[derive(Debug, PartialEq, Eq)]
    enum ProbeRejection {
        Missing,
        Malformed(String),
        BodyUnavailable,
    }

    impl From<ProbeRejection> for HttpApiError {
        fn from(rejection: ProbeRejection) -> Self {
            match rejection {
                ProbeRejection::Missing => {
                    Self::BadRequest("required request input is missing".to_string())
                }
                ProbeRejection::Malformed(_untrusted_value) => {
                    Self::BadRequest("request input is malformed".to_string())
                }
                ProbeRejection::BodyUnavailable => {
                    Self::BadRequest("request body is unavailable".to_string())
                }
            }
        }
    }

    impl FromRequestParts for RequiredProbe {
        type Rejection = ProbeRejection;

        fn from_request_parts(
            request: &mut Request,
            extensions: &Extensions,
        ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
            let value = request.query_first("required").map(ToString::to_string);
            let provider_address = extensions as *const Extensions as usize;
            async move {
                let value = value.ok_or(ProbeRejection::Missing)?;
                Ok(Self {
                    value,
                    provider_address,
                })
            }
        }
    }

    impl OptionalFromRequestParts for OptionalProbe {
        type Rejection = ProbeRejection;

        fn from_request_parts(
            request: &mut Request,
            _extensions: &Extensions,
        ) -> impl Future<Output = Result<Option<Self>, Self::Rejection>> + Send {
            let value = request.query_first("optional").map(ToString::to_string);
            async move {
                match value.as_deref() {
                    None => Ok(None),
                    Some("accepted") => Ok(Some(Self("accepted".to_string()))),
                    Some(_) => Err(ProbeRejection::Malformed(
                        value.expect("a present optional input has a value"),
                    )),
                }
            }
        }
    }

    impl FromRequest for BodyProbe {
        type Rejection = ProbeRejection;

        async fn from_request(
            request: &mut Request,
            _extensions: &Extensions,
        ) -> Result<Self, Self::Rejection> {
            let body = request
                .buffer_body()
                .await
                .map_err(|_| ProbeRejection::BodyUnavailable)?;
            Ok(Self(body.map_or(0, lily_web_core::HttpBuffer::len)))
        }
    }

    async fn request(path: &str, body: &[u8]) -> Request {
        Request::from_transport_parts("POST".to_string(), path.to_string(), Vec::new(), body)
            .await
            .expect("the test request is valid")
    }

    #[tokio::test]
    async fn custom_parts_and_body_extractors_use_static_safe_boundary_adapters() {
        let container = ApplicationContainer::build()
            .await
            .expect("the test container builds");
        let extensions = container.services();

        let mut parts_request = request("/fixture?required=present", &[]).await;
        let parts = extract_request_parts::<RequiredProbe>(&mut parts_request, &extensions)
            .await
            .expect("the custom parts extractor succeeds");
        assert_eq!(parts.value, "present");
        assert_eq!(
            parts.provider_address,
            extensions.as_ref() as *const Extensions as usize
        );

        let mut body_request = request("/fixture", b"bounded-body").await;
        let body = extract_request::<BodyProbe>(&mut body_request, &extensions)
            .await
            .expect("the custom body extractor succeeds");
        assert_eq!(body, BodyProbe(b"bounded-body".len()));

        container.close().await.expect("the test container closes");
    }

    #[tokio::test]
    async fn option_blanket_maps_only_semantic_absence_to_none() {
        let container = ApplicationContainer::build()
            .await
            .expect("the test container builds");
        let extensions = container.services();

        let mut absent = request("/fixture", &[]).await;
        let extracted = extract_request_parts::<Option<OptionalProbe>>(&mut absent, &extensions)
            .await
            .expect("absent optional input is valid");
        assert_eq!(extracted, None);

        let mut present = request("/fixture?optional=accepted", &[]).await;
        let extracted = extract_request_parts::<Option<OptionalProbe>>(&mut present, &extensions)
            .await
            .expect("present optional input is valid");
        assert_eq!(extracted, Some(OptionalProbe("accepted".to_string())));

        let mut malformed = request(&format!("/fixture?optional={SECRET_INPUT}"), &[]).await;
        let error = extract_request_parts::<Option<OptionalProbe>>(&mut malformed, &extensions)
            .await
            .expect_err("present malformed input must not become None");
        assert_eq!(error.http_status().0, 400);
        assert!(!format!("{error:?}").contains(SECRET_INPUT));
        assert!(!error.to_string().contains(SECRET_INPUT));
        assert!(!format!("{:?}", error.public_body()).contains(SECRET_INPUT));

        container.close().await.expect("the test container closes");
    }

    #[tokio::test]
    async fn required_absence_remains_a_typed_secret_safe_rejection() {
        let container = ApplicationContainer::build()
            .await
            .expect("the test container builds");
        let extensions = container.services();
        let mut missing = request("/fixture", &[]).await;

        let error = extract_request_parts::<RequiredProbe>(&mut missing, &extensions)
            .await
            .expect_err("required input absence must reject");
        assert_eq!(error.http_status().0, 400);
        assert_eq!(error.error_code(), "BAD_REQUEST");

        container.close().await.expect("the test container closes");
    }
}
