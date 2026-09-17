//! HTTP application composition and managed server lifecycle.

#[allow(clippy::module_inception)]
mod app;
mod build_error;
pub(crate) mod middleware_executor;

#[cfg(test)]
mod app_unit_tests;

pub use app::*;
pub use build_error::{
    AppBuildError, HttpServerConfigError, HttpTlsConfigError, MissingRouteGuardError,
    RouteMiddlewareBuildError, RouteMiddlewareBuildErrorCause,
};
