//! HTTP response classification shared by the application and transport boundary.

/// Final HTTP status classification. Error origin and response-write failures
/// are retained independently; a diagnostic code alone does not imply failure.
pub(crate) const fn http_status_outcome(status: u16) -> &'static str {
    match status {
        400..=499 => "rejected",
        500..=599 => "error",
        _ => "success",
    }
}

pub(crate) const fn http_status_error_code(status: u16) -> Option<&'static str> {
    match status {
        400..=499 => Some("HTTP_CLIENT_ERROR"),
        500..=599 => Some("HTTP_SERVER_ERROR"),
        _ => None,
    }
}
