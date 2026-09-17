use super::{write_error_response, ResponseWriteError};
use super::{IntoResponse, Response, ResponseWriteOutcome};
use crate::{Request, ResponseBodyError, ResponseBodyStream};
use bytes::Bytes;
use cap_fs_ext::{DirExt, FollowSymlinks, OpenOptionsFollowExt};
use cap_std::{
    ambient_authority,
    fs::{Dir, OpenOptions},
};
use futures::{Stream, StreamExt};
use lily_error::application::http_api::HttpApiError;
use percent_encoding::percent_decode_str;
use std::{
    io::{self, Read, Seek, SeekFrom},
    path::{Component, Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

/// Maximum decoded relative path retained by one static-file request.
pub const MAX_STATIC_FILE_PATH_BYTES: usize = 4 * 1024;
/// Maximum configured route-prefix length.
pub const MAX_STATIC_FILE_ROUTE_PREFIX_BYTES: usize = 1024;
/// Fixed pull size used by the file source before CAP-04 transport framing.
pub const STATIC_FILE_CHUNK_BYTES: usize = 64 * 1024;
/// Maximum explicit cache policy retained by one mount.
pub const MAX_STATIC_FILE_CACHE_CONTROL_BYTES: usize = 1024;
/// Maximum conditional/range field bytes inspected by this adapter.
pub const MAX_STATIC_FILE_CONDITION_BYTES: usize = 8 * 1024;

/// Symlink authority for a static-file mount.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum StaticFileSymlinkPolicy {
    /// Reject every symlink component before opening the file.
    #[default]
    Deny,
    /// Permit symlinks only when capability-based resolution remains in root.
    AllowWithinRoot,
}

/// A typed, path-redacted static-file setup or request failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum StaticFileError {
    /// The configured root could not be opened.
    #[error("static-file root could not be opened")]
    RootUnavailable,
    /// The configured root is not a directory.
    #[error("static-file root must be a directory")]
    RootNotDirectory,
    /// The root itself is a symlink while symlinks are denied.
    #[error("static-file root cannot be a symlink under the selected policy")]
    RootSymlinkDenied,
    /// The URL mount prefix is malformed or unbounded.
    #[error("static-file route prefix is invalid")]
    InvalidRoutePrefix,
    /// The configured `Cache-Control` field is invalid.
    #[error("static-file cache-control value is invalid")]
    InvalidCacheControl,
    /// Only `GET` and `HEAD` may serve a file.
    #[error("static-file request method must be GET or HEAD")]
    UnsupportedMethod,
    /// The request path is malformed, escapes the mount, or names no resource.
    #[error("static-file request path is invalid or outside the mount")]
    InvalidRequestPath,
    /// The decoded relative path exceeded its bound.
    #[error("static-file request path exceeds the {limit_bytes}-byte limit")]
    PathTooLong {
        /// Maximum decoded relative-path bytes.
        limit_bytes: usize,
    },
    /// No regular file exists at the resolved path.
    #[error("static-file resource was not found")]
    NotFound,
    /// A denied symlink was encountered.
    #[error("static-file symlink access is denied")]
    SymlinkDenied,
    /// File metadata could not be read safely.
    #[error("static-file metadata is unavailable")]
    MetadataUnavailable,
    /// The resolved resource could not be opened.
    #[error("static-file resource could not be opened")]
    OpenFailed,
    /// A blocking filesystem task could not complete.
    #[error("static-file runtime task failed")]
    RuntimeUnavailable,
    /// A conditional or range request field was malformed.
    #[error("static-file conditional request field is invalid")]
    InvalidConditionalRequest,
}

impl From<StaticFileError> for HttpApiError {
    fn from(error: StaticFileError) -> Self {
        match error {
            StaticFileError::UnsupportedMethod => Self::MethodNotAllowed(error.to_string()),
            StaticFileError::InvalidRequestPath
            | StaticFileError::PathTooLong { .. }
            | StaticFileError::InvalidConditionalRequest => Self::BadRequest(error.to_string()),
            StaticFileError::NotFound | StaticFileError::SymlinkDenied => {
                Self::NotFound("static-file resource was not found".to_string())
            }
            StaticFileError::RootUnavailable
            | StaticFileError::RootNotDirectory
            | StaticFileError::RootSymlinkDenied
            | StaticFileError::InvalidRoutePrefix
            | StaticFileError::InvalidCacheControl => {
                Self::ResponseEncodingError(error.to_string())
            }
            StaticFileError::MetadataUnavailable
            | StaticFileError::OpenFailed
            | StaticFileError::RuntimeUnavailable => {
                Self::IoError("static-file I/O operation failed".to_string())
            }
        }
    }
}

/// An immutable capability root and URL prefix for static-file responses.
///
/// The root is opened once during construction. Per-request paths are resolved
/// relative to that open directory capability rather than through ambient
/// filesystem paths.
///
/// Register the action itself on a final catch-all route such as
/// `GET /assets/*path`, keep the mount in application-owned state, and return
/// its response directly:
///
/// ```no_run
/// use lily_error::application::http_api::HttpApiError;
/// use lily_web_core::{Request, StaticFileMount, StaticFileResponse};
///
/// async fn static_asset(
///     files: &StaticFileMount,
///     request: &Request,
/// ) -> Result<StaticFileResponse, HttpApiError> {
///     Ok(files.serve(request).await?)
/// }
/// ```
#[derive(Clone)]
pub struct StaticFileMount {
    root: Arc<Dir>,
    route_prefix: Arc<str>,
    symlink_policy: StaticFileSymlinkPolicy,
    cache_control: Option<Arc<str>>,
}

impl std::fmt::Debug for StaticFileMount {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StaticFileMount")
            .field("route_prefix", &self.route_prefix)
            .field("symlink_policy", &self.symlink_policy)
            .field("cache_control_configured", &self.cache_control.is_some())
            .finish_non_exhaustive()
    }
}

impl StaticFileMount {
    /// Opens one static root with symlinks denied.
    pub async fn new(
        root: impl AsRef<Path>,
        route_prefix: impl Into<String>,
    ) -> Result<Self, StaticFileError> {
        Self::with_symlink_policy(root, route_prefix, StaticFileSymlinkPolicy::Deny).await
    }

    /// Opens one static root with an explicit symlink policy.
    pub async fn with_symlink_policy(
        root: impl AsRef<Path>,
        route_prefix: impl Into<String>,
        symlink_policy: StaticFileSymlinkPolicy,
    ) -> Result<Self, StaticFileError> {
        let route_prefix = validate_route_prefix(route_prefix.into())?;
        let root = root.as_ref().to_path_buf();
        let opened = crate::http_resources::blocking(move || {
            let metadata =
                std::fs::symlink_metadata(&root).map_err(|_| StaticFileError::RootUnavailable)?;
            if symlink_policy == StaticFileSymlinkPolicy::Deny && metadata.file_type().is_symlink()
            {
                return Err(StaticFileError::RootSymlinkDenied);
            }
            if !metadata.is_dir() {
                return Err(StaticFileError::RootNotDirectory);
            }
            let directory = Dir::open_ambient_dir(&root, ambient_authority())
                .map_err(|_| StaticFileError::RootUnavailable)?;
            if !directory
                .dir_metadata()
                .map_err(|_| StaticFileError::RootUnavailable)?
                .is_dir()
            {
                return Err(StaticFileError::RootNotDirectory);
            }
            Ok(directory)
        })
        .await
        .map_err(|_| StaticFileError::RuntimeUnavailable)??;

        Ok(Self {
            root: Arc::new(opened),
            route_prefix: route_prefix.into(),
            symlink_policy,
            cache_control: None,
        })
    }

    /// Adds an explicit application-owned cache policy.
    pub fn cache_control(mut self, value: impl Into<String>) -> Result<Self, StaticFileError> {
        let value = value.into();
        let value = value.trim();
        if value.is_empty()
            || value.len() > MAX_STATIC_FILE_CACHE_CONTROL_BYTES
            || http::HeaderValue::from_str(value).is_err()
        {
            return Err(StaticFileError::InvalidCacheControl);
        }
        self.cache_control = Some(Arc::from(value));
        Ok(self)
    }

    #[must_use]
    /// Returns the validated URL prefix claimed by this mount.
    pub fn route_prefix(&self) -> &str {
        &self.route_prefix
    }

    #[must_use]
    /// Returns the mount's symlink authority.
    pub const fn symlink_policy(&self) -> StaticFileSymlinkPolicy {
        self.symlink_policy
    }

    /// Resolves request metadata and prepares one lazy file response.
    pub async fn serve(&self, request: &Request) -> Result<StaticFileResponse, StaticFileError> {
        if !matches!(request.method(), "GET" | "HEAD") {
            return Err(StaticFileError::UnsupportedMethod);
        }
        let relative = request_relative_path(request, &self.route_prefix)?;
        let opened = self.open_file(relative.clone()).await?;
        let modified = opened.modified;
        let modified_parts = modified
            .duration_since(UNIX_EPOCH)
            .map_err(|_| StaticFileError::MetadataUnavailable)?;
        let etag = format!(
            "W/\"{:x}-{:x}-{:x}\"",
            opened.length,
            modified_parts.as_secs(),
            modified_parts.subsec_nanos()
        );
        let last_modified = httpdate::fmt_http_date(modified);
        let content_type = mime_guess::from_path(&relative)
            .first_raw()
            .unwrap_or("application/octet-stream");
        let mut headers = representation_headers(
            content_type,
            &etag,
            &last_modified,
            self.cache_control.as_deref(),
        );

        if let Some(matches) = if_none_match(request, &etag)? {
            if matches {
                return Ok(StaticFileResponse::empty(304, "Not Modified", headers));
            }
        } else if if_modified_since(request, modified) {
            return Ok(StaticFileResponse::empty(304, "Not Modified", headers));
        }

        let requested_range = request_range(request, opened.length);
        let range = if requested_range.is_some() && if_range_allows(request, &etag, modified) {
            requested_range
        } else {
            None
        };
        let (status, reason, offset, length) = match range {
            Some(ParsedRange::Satisfiable { start, end }) => {
                headers.push((
                    "Content-Range".to_string(),
                    format!("bytes {start}-{end}/{}", opened.length),
                ));
                (206, "Partial Content", start, end - start + 1)
            }
            Some(ParsedRange::Unsatisfiable) => {
                headers.push((
                    "Content-Range".to_string(),
                    format!("bytes */{}", opened.length),
                ));
                return Ok(StaticFileResponse::empty(
                    416,
                    "Range Not Satisfiable",
                    headers,
                ));
            }
            None => (200, "OK", 0, opened.length),
        };

        let source = file_byte_stream(opened.file, offset, length);
        let mut body = ResponseBodyStream::new(source);
        body.set_exact_length(Some(length));
        body.set_max_chunk_bytes(Some(STATIC_FILE_CHUNK_BYTES));
        body.set_max_total_bytes(Some(length.max(1)));
        Ok(StaticFileResponse::streaming(status, reason, headers, body))
    }

    async fn open_file(&self, relative: PathBuf) -> Result<OpenedStaticFile, StaticFileError> {
        let root = Arc::clone(&self.root);
        let symlink_policy = self.symlink_policy;
        crate::http_resources::blocking(move || {
            let file = match symlink_policy {
                StaticFileSymlinkPolicy::Deny => open_file_without_symlinks(&root, &relative)?,
                StaticFileSymlinkPolicy::AllowWithinRoot => {
                    root.open(&relative).map_err(map_file_lookup_error)?
                }
            };
            let metadata = file
                .metadata()
                .map_err(|_| StaticFileError::MetadataUnavailable)?;
            if !metadata.is_file() {
                return Err(StaticFileError::NotFound);
            }
            let modified = metadata
                .modified()
                .map_err(|_| StaticFileError::MetadataUnavailable)?
                .into_std();
            Ok(OpenedStaticFile {
                file: file.into_std(),
                length: metadata.len(),
                modified,
            })
        })
        .await
        .map_err(|_| StaticFileError::RuntimeUnavailable)?
    }
}

fn open_file_without_symlinks(
    root: &Dir,
    relative: &Path,
) -> Result<cap_std::fs::File, StaticFileError> {
    let mut directory = root
        .open_dir_nofollow(".")
        .map_err(|_| StaticFileError::OpenFailed)?;
    let mut components = relative.components().peekable();

    while let Some(component) = components.next() {
        let Component::Normal(component) = component else {
            return Err(StaticFileError::InvalidRequestPath);
        };
        let metadata = directory
            .symlink_metadata(component)
            .map_err(map_file_lookup_error)?;
        if metadata.file_type().is_symlink() {
            return Err(StaticFileError::SymlinkDenied);
        }

        if components.peek().is_some() {
            directory = directory
                .open_dir_nofollow(component)
                .map_err(map_file_lookup_error)?;
            continue;
        }

        let mut options = OpenOptions::new();
        options.read(true).follow(FollowSymlinks::No);
        return directory
            .open_with(component, &options)
            .map_err(map_file_lookup_error);
    }

    Err(StaticFileError::NotFound)
}

struct OpenedStaticFile {
    file: std::fs::File,
    length: u64,
    modified: SystemTime,
}

fn map_file_lookup_error(error: io::Error) -> StaticFileError {
    match error.kind() {
        io::ErrorKind::NotFound | io::ErrorKind::PermissionDenied => StaticFileError::NotFound,
        _ => StaticFileError::OpenFailed,
    }
}

fn validate_route_prefix(value: String) -> Result<String, StaticFileError> {
    if value.is_empty()
        || value.len() > MAX_STATIC_FILE_ROUTE_PREFIX_BYTES
        || !value.starts_with('/')
        || !value.is_ascii()
        || value.contains(['?', '#', '\0', '\\', '%'])
        || (value != "/" && value.ends_with('/'))
        || (value != "/"
            && value
                .split('/')
                .skip(1)
                .any(|segment| segment.is_empty() || matches!(segment, "." | "..")))
    {
        return Err(StaticFileError::InvalidRoutePrefix);
    }
    Ok(value)
}

fn request_relative_path(
    request: &Request,
    route_prefix: &str,
) -> Result<PathBuf, StaticFileError> {
    let path = request
        .path()
        .split_once('?')
        .map_or(request.path(), |(path, _)| path);
    let raw_relative = if route_prefix == "/" {
        path.strip_prefix('/')
    } else {
        path.strip_prefix(route_prefix)
            .and_then(|value| value.strip_prefix('/'))
    }
    .ok_or(StaticFileError::InvalidRequestPath)?;
    if raw_relative.is_empty() {
        return Err(StaticFileError::NotFound);
    }
    if raw_relative.len() > MAX_STATIC_FILE_PATH_BYTES {
        return Err(StaticFileError::PathTooLong {
            limit_bytes: MAX_STATIC_FILE_PATH_BYTES,
        });
    }

    let mut relative = PathBuf::new();
    let mut decoded_bytes = 0usize;
    for raw_segment in raw_relative.split('/') {
        if raw_segment.is_empty() || !valid_percent_encoding(raw_segment) {
            return Err(StaticFileError::InvalidRequestPath);
        }
        let segment = percent_decode_str(raw_segment)
            .decode_utf8()
            .map_err(|_| StaticFileError::InvalidRequestPath)?;
        decoded_bytes = decoded_bytes
            .checked_add(usize::from(!relative.as_os_str().is_empty()))
            .and_then(|length| length.checked_add(segment.len()))
            .ok_or(StaticFileError::PathTooLong {
                limit_bytes: MAX_STATIC_FILE_PATH_BYTES,
            })?;
        if decoded_bytes > MAX_STATIC_FILE_PATH_BYTES
            || segment.is_empty()
            || matches!(segment.as_ref(), "." | "..")
            || segment
                .as_bytes()
                .iter()
                .any(|byte| matches!(byte, 0 | b'/' | b'\\' | 1..=31 | 127))
        {
            return Err(StaticFileError::InvalidRequestPath);
        }
        relative.push(segment.as_ref());
    }
    if relative
        .components()
        .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(StaticFileError::InvalidRequestPath);
    }
    Ok(relative)
}

fn valid_percent_encoding(value: &str) -> bool {
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len()
                || !bytes[index + 1].is_ascii_hexdigit()
                || !bytes[index + 2].is_ascii_hexdigit()
            {
                return false;
            }
            index += 3;
        } else {
            index += 1;
        }
    }
    true
}

fn representation_headers(
    content_type: &str,
    etag: &str,
    last_modified: &str,
    cache_control: Option<&str>,
) -> Vec<(String, String)> {
    let mut headers = Vec::with_capacity(5);
    headers.push(("Content-Type".to_string(), content_type.to_string()));
    headers.push(("Accept-Ranges".to_string(), "bytes".to_string()));
    headers.push(("ETag".to_string(), etag.to_string()));
    headers.push(("Last-Modified".to_string(), last_modified.to_string()));
    if let Some(cache_control) = cache_control {
        headers.push(("Cache-Control".to_string(), cache_control.to_string()));
    }
    headers
}

fn if_none_match(request: &Request, current: &str) -> Result<Option<bool>, StaticFileError> {
    let current = normalize_entity_tag(current).expect("generated ETag is valid");
    let mut present = false;
    let mut matched = false;
    let mut wildcard = false;
    let mut tag_count = 0usize;
    let mut total_bytes = 0usize;
    for header in request
        .header_cache()
        .iter()
        .filter(|header| header.name.eq_ignore_ascii_case("if-none-match"))
    {
        present = true;
        total_bytes = total_bytes.saturating_add(header.value.len());
        if total_bytes > MAX_STATIC_FILE_CONDITION_BYTES {
            return Err(StaticFileError::InvalidConditionalRequest);
        }
        for value in header.value.split(',') {
            let value = value.trim();
            if value == "*" {
                wildcard = true;
                continue;
            }
            let candidate =
                normalize_entity_tag(value).ok_or(StaticFileError::InvalidConditionalRequest)?;
            tag_count = tag_count.saturating_add(1);
            matched |= candidate == current;
        }
    }
    if wildcard && tag_count != 0 {
        return Err(StaticFileError::InvalidConditionalRequest);
    }
    Ok(present.then_some(wildcard || matched))
}

fn normalize_entity_tag(value: &str) -> Option<&str> {
    let value = value.strip_prefix("W/").unwrap_or(value);
    if value.len() < 2 || !value.starts_with('"') || !value.ends_with('"') {
        return None;
    }
    let opaque = &value.as_bytes()[1..value.len() - 1];
    opaque
        .iter()
        .all(|byte| *byte == 0x21 || (0x23..=0x7e).contains(byte) || *byte >= 0x80)
        .then_some(value)
}

fn if_modified_since(request: &Request, modified: SystemTime) -> bool {
    let mut values = request
        .header_cache()
        .iter()
        .filter(|header| header.name.eq_ignore_ascii_case("if-modified-since"));
    let Some(value) = values.next() else {
        return false;
    };
    if values.next().is_some() || value.value.len() > MAX_STATIC_FILE_CONDITION_BYTES {
        return false;
    }
    let Ok(since) = httpdate::parse_http_date(&value.value) else {
        return false;
    };
    system_time_seconds(modified)
        .zip(system_time_seconds(since))
        .is_some_and(|(modified, since)| modified <= since)
}

fn if_range_allows(request: &Request, current_etag: &str, modified: SystemTime) -> bool {
    let mut values = request
        .header_cache()
        .iter()
        .filter(|header| header.name.eq_ignore_ascii_case("if-range"));
    let Some(value) = values.next() else {
        return true;
    };
    if values.next().is_some() || value.value.len() > MAX_STATIC_FILE_CONDITION_BYTES {
        return false;
    }
    let value = value.value.trim();
    if value.starts_with('"') || value.starts_with("W/") {
        return !value.starts_with("W/")
            && !current_etag.starts_with("W/")
            && value == current_etag;
    }
    let Ok(since) = httpdate::parse_http_date(value) else {
        return false;
    };
    system_time_seconds(modified)
        .zip(system_time_seconds(since))
        .is_some_and(|(modified, since)| modified <= since)
}

fn system_time_seconds(value: SystemTime) -> Option<u64> {
    value
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|value| value.as_secs())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParsedRange {
    Satisfiable { start: u64, end: u64 },
    Unsatisfiable,
}

fn request_range(request: &Request, length: u64) -> Option<ParsedRange> {
    let mut values = request
        .header_cache()
        .iter()
        .filter(|header| header.name.eq_ignore_ascii_case("range"));
    let value = values.next()?;
    if values.next().is_some() || value.value.len() > MAX_STATIC_FILE_CONDITION_BYTES || length == 0
    {
        return Some(ParsedRange::Unsatisfiable);
    }
    let Some((unit, value)) = value.value.split_once('=') else {
        return Some(ParsedRange::Unsatisfiable);
    };
    if !unit.trim().eq_ignore_ascii_case("bytes") || value.contains(',') {
        return Some(ParsedRange::Unsatisfiable);
    }
    let Some((start, end)) = value.trim().split_once('-') else {
        return Some(ParsedRange::Unsatisfiable);
    };
    let start = start.trim();
    let end = end.trim();
    if start.is_empty() {
        let Ok(suffix) = end.parse::<u64>() else {
            return Some(ParsedRange::Unsatisfiable);
        };
        if suffix == 0 {
            return Some(ParsedRange::Unsatisfiable);
        }
        return Some(ParsedRange::Satisfiable {
            start: length.saturating_sub(suffix),
            end: length - 1,
        });
    }
    let Ok(start) = start.parse::<u64>() else {
        return Some(ParsedRange::Unsatisfiable);
    };
    if start >= length {
        return Some(ParsedRange::Unsatisfiable);
    }
    if end.is_empty() {
        return Some(ParsedRange::Satisfiable {
            start,
            end: length - 1,
        });
    }
    let Ok(end) = end.parse::<u64>() else {
        return Some(ParsedRange::Unsatisfiable);
    };
    if start > end {
        return Some(ParsedRange::Unsatisfiable);
    }
    Some(ParsedRange::Satisfiable {
        start,
        end: end.min(length - 1),
    })
}

fn file_byte_stream(
    file: std::fs::File,
    offset: u64,
    remaining: u64,
) -> impl Stream<Item = Result<Bytes, ResponseBodyError>> + Send + 'static {
    futures::stream::try_unfold(
        (file, offset, remaining),
        |(mut file, offset, remaining)| async move {
            if remaining == 0 {
                return Ok(None);
            }
            let chunk_bytes = usize::try_from(remaining.min(STATIC_FILE_CHUNK_BYTES as u64))
                .expect("bounded file chunk length fits usize");
            crate::http_resources::blocking(move || {
                file.seek(SeekFrom::Start(offset))?;
                let mut buffer = vec![0_u8; chunk_bytes];
                let read = file.read(&mut buffer)?;
                if read == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "static file ended before declared metadata length",
                    ));
                }
                buffer.truncate(read);
                Ok(Some((
                    Bytes::from(buffer),
                    (file, offset + read as u64, remaining - read as u64),
                )))
            })
            .await
            .map_err(|()| io::Error::other("static file worker failed"))?
        },
    )
    .map(|item| item.map_err(ResponseBodyError::from))
}

enum StaticFileResponseBody {
    Empty,
    Stream(ResponseBodyStream),
}

/// One typed static-file response returned directly from a controller action.
pub struct StaticFileResponse {
    status: u16,
    reason: &'static str,
    headers: Vec<(String, String)>,
    body: StaticFileResponseBody,
}

impl StaticFileResponse {
    fn empty(status: u16, reason: &'static str, headers: Vec<(String, String)>) -> Self {
        Self {
            status,
            reason,
            headers,
            body: StaticFileResponseBody::Empty,
        }
    }

    fn streaming(
        status: u16,
        reason: &'static str,
        headers: Vec<(String, String)>,
        body: ResponseBodyStream,
    ) -> Self {
        Self {
            status,
            reason,
            headers,
            body: StaticFileResponseBody::Stream(body),
        }
    }
}

impl std::fmt::Debug for StaticFileResponse {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StaticFileResponse")
            .field("status", &self.status)
            .field("header_count", &self.headers.len())
            .field(
                "streaming",
                &matches!(self.body, StaticFileResponseBody::Stream(_)),
            )
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl IntoResponse for StaticFileResponse {
    async fn write_to_response(
        self,
        response: &mut Response,
        _request: &mut Request,
    ) -> Result<ResponseWriteOutcome, ResponseWriteError> {
        let Self {
            status,
            reason,
            headers,
            body,
        } = self;
        let headers = headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()));
        match body {
            StaticFileResponseBody::Empty => {
                response.replace_buffered(status, reason, headers, Vec::new())?;
            }
            StaticFileResponseBody::Stream(body) => {
                response.replace_streaming(status, reason, headers, body)?;
            }
        }
        Ok(ResponseWriteOutcome::authoritative())
    }
}

#[async_trait::async_trait]
impl<E> IntoResponse for Result<StaticFileResponse, E>
where
    E: IntoResponse + Send,
{
    async fn write_to_response(
        self,
        response: &mut Response,
        request: &mut Request,
    ) -> Result<ResponseWriteOutcome, ResponseWriteError> {
        match self {
            Ok(value) => value.write_to_response(response, request).await,
            Err(error) => write_error_response(error, response, request).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BodyBudget, ResponseLimits, ResponseStreamingLimits, TransportResponseParts};
    use futures::StreamExt;
    use tempfile::TempDir;

    fn write_file(root: &TempDir, relative: &str, contents: &[u8]) {
        let path = root.path().join(relative);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(path, contents).unwrap();
    }

    async fn build_mount(root: &TempDir) -> StaticFileMount {
        StaticFileMount::new(root.path(), "/assets").await.unwrap()
    }

    async fn materialize(
        value: StaticFileResponse,
        request: &mut Request,
    ) -> TransportResponseParts {
        let limits = ResponseLimits::new(BodyBudget::new(1024).unwrap(), 16, 8192)
            .unwrap()
            .with_streaming_limits(
                ResponseStreamingLimits::new(STATIC_FILE_CHUNK_BYTES, None).unwrap(),
            );
        let mut response = Response::with_limits(limits).await.unwrap();
        let outcome = value
            .write_to_response(&mut response, request)
            .await
            .unwrap();
        assert_eq!(outcome, ResponseWriteOutcome::authoritative());
        response.into_transport_parts().unwrap()
    }

    fn header<'a>(parts: &'a TransportResponseParts, name: &str) -> Option<&'a str> {
        parts
            .headers
            .iter()
            .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    async fn collect_stream(parts: &mut TransportResponseParts) -> Vec<u8> {
        let mut output = Vec::new();
        let stream = parts.stream.as_mut().expect("streaming static response");
        while let Some(chunk) = stream.next().await {
            output.extend_from_slice(&chunk.unwrap());
        }
        output
    }

    #[tokio::test]
    async fn full_file_is_lazy_bounded_typed_and_cache_control_is_explicit() {
        let root = TempDir::new().unwrap();
        let contents = vec![b'x'; STATIC_FILE_CHUNK_BYTES + 17];
        write_file(&root, "docs/hello world.txt", &contents);
        let mount = build_mount(&root)
            .await
            .cache_control("public, max-age=60")
            .unwrap();
        let mut request = Request::new_test("GET", "/assets/docs/hello%20world.txt?download=1");
        let response = mount.serve(&request).await.unwrap();
        let mut parts = materialize(response, &mut request).await;

        assert_eq!(parts.status, 200);
        assert_eq!(header(&parts, "content-type"), Some("text/plain"));
        assert_eq!(header(&parts, "accept-ranges"), Some("bytes"));
        assert_eq!(header(&parts, "cache-control"), Some("public, max-age=60"));
        assert!(header(&parts, "etag").unwrap().starts_with("W/\""));
        assert!(httpdate::parse_http_date(header(&parts, "last-modified").unwrap()).is_ok());
        assert_eq!(parts.exact_length(), Some(contents.len() as u64));
        assert_eq!(collect_stream(&mut parts).await, contents);

        let uncached = build_mount(&root).await;
        let response = uncached.serve(&request).await.unwrap();
        let parts = materialize(response, &mut request).await;
        assert_eq!(header(&parts, "cache-control"), None);
    }

    #[tokio::test]
    async fn traversal_ambiguous_encoding_and_mount_escape_fail_before_open() {
        let root = TempDir::new().unwrap();
        write_file(&root, "safe.txt", b"safe");
        let mount = build_mount(&root).await;
        for path in [
            "/asset/safe.txt",
            "/assets/../safe.txt",
            "/assets/%2e%2e/safe.txt",
            "/assets/%2Fetc/passwd",
            "/assets/a%5Cb.txt",
            "/assets/a//b.txt",
            "/assets/%GG",
            "/assets/%00.txt",
        ] {
            let request = Request::new_test("GET", path);
            assert!(matches!(
                mount.serve(&request).await,
                Err(StaticFileError::InvalidRequestPath)
            ));
        }

        let root_mount = StaticFileMount::new(root.path(), "/").await.unwrap();
        let request = Request::new_test("GET", "/safe.txt");
        assert_eq!(
            root_mount.serve(&request).await.unwrap().status,
            200,
            "the explicit root prefix remains usable"
        );
    }

    #[tokio::test]
    async fn duplicate_and_standard_conditional_requests_are_deterministic() {
        let root = TempDir::new().unwrap();
        write_file(&root, "version.txt", b"version-one");
        let mount = build_mount(&root).await;
        let mut initial = Request::new_test("GET", "/assets/version.txt");
        let response = mount.serve(&initial).await.unwrap();
        let parts = materialize(response, &mut initial).await;
        let etag = header(&parts, "etag").unwrap().to_string();
        let last_modified = header(&parts, "last-modified").unwrap().to_string();

        let mut etag_request = Request::new_test("GET", "/assets/version.txt");
        etag_request.add_test_header("If-None-Match", &etag);
        let response = mount.serve(&etag_request).await.unwrap();
        let parts = materialize(response, &mut etag_request).await;
        assert_eq!(parts.status, 304);
        assert!(parts.stream.is_none());
        assert!(parts.body.is_empty());

        let mut date_request = Request::new_test("HEAD", "/assets/version.txt");
        date_request.add_test_header("If-Modified-Since", &last_modified);
        let response = mount.serve(&date_request).await.unwrap();
        let parts = materialize(response, &mut date_request).await;
        assert_eq!(parts.status, 304);

        let mut duplicate = Request::new_test("GET", "/assets/version.txt");
        duplicate.add_test_header("If-None-Match", &etag);
        duplicate.add_test_header("If-None-Match", "invalid");
        assert!(matches!(
            mount.serve(&duplicate).await,
            Err(StaticFileError::InvalidConditionalRequest)
        ));
    }

    #[tokio::test]
    async fn single_open_ended_suffix_and_unsatisfied_ranges_are_exact() {
        let root = TempDir::new().unwrap();
        write_file(&root, "digits.txt", b"0123456789");
        let mount = build_mount(&root).await;

        for (range, expected_range, expected) in [
            ("bytes=2-5", "bytes 2-5/10", b"2345".as_slice()),
            ("bytes=7-", "bytes 7-9/10", b"789".as_slice()),
            ("bytes=-3", "bytes 7-9/10", b"789".as_slice()),
        ] {
            let mut request = Request::new_test("GET", "/assets/digits.txt");
            request.add_test_header("Range", range);
            let response = mount.serve(&request).await.unwrap();
            let mut parts = materialize(response, &mut request).await;
            assert_eq!(parts.status, 206);
            assert_eq!(header(&parts, "content-range"), Some(expected_range));
            assert_eq!(parts.exact_length(), Some(expected.len() as u64));
            assert_eq!(collect_stream(&mut parts).await, expected);
        }

        for range in ["bytes=99-100", "bytes=8-3", "bytes=0-1,4-5", "items=0-1"] {
            let mut request = Request::new_test("GET", "/assets/digits.txt");
            request.add_test_header("Range", range);
            let response = mount.serve(&request).await.unwrap();
            let parts = materialize(response, &mut request).await;
            assert_eq!(parts.status, 416);
            assert_eq!(header(&parts, "content-range"), Some("bytes */10"));
            assert!(parts.stream.is_none());
        }
    }

    #[tokio::test]
    async fn if_range_mismatch_ignores_range_and_head_keeps_exact_representation_length() {
        let root = TempDir::new().unwrap();
        write_file(&root, "asset.bin", b"abcdefghij");
        let mount = build_mount(&root).await;
        let mut request = Request::new_test("HEAD", "/assets/asset.bin");
        request.add_test_header("Range", "bytes=2-4");
        request.add_test_header("If-Range", "\"different-strong-tag\"");
        let response = mount.serve(&request).await.unwrap();
        let parts = materialize(response, &mut request).await;
        assert_eq!(parts.status, 200);
        assert_eq!(parts.exact_length(), Some(10));
        assert_eq!(header(&parts, "content-range"), None);
        assert!(parts.stream.is_some());
    }

    #[tokio::test]
    async fn configuration_and_error_debug_are_bounded_and_path_redacted() {
        let root = TempDir::new().unwrap();
        let mount = build_mount(&root).await;
        assert!(!format!("{mount:?}").contains(root.path().to_string_lossy().as_ref()));
        assert!(matches!(
            StaticFileMount::new(root.path(), "assets").await,
            Err(StaticFileError::InvalidRoutePrefix)
        ));
        assert!(matches!(
            mount.clone().cache_control("bad\r\nvalue"),
            Err(StaticFileError::InvalidCacheControl)
        ));
        let request = Request::new_test("POST", "/assets/file.txt");
        assert!(matches!(
            mount.serve(&request).await,
            Err(StaticFileError::UnsupportedMethod)
        ));

        let file_root = root.path().join("not-a-directory");
        std::fs::write(&file_root, b"file").unwrap();
        assert!(matches!(
            StaticFileMount::new(&file_root, "/assets").await,
            Err(StaticFileError::RootNotDirectory)
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn symlinks_are_denied_by_default_and_capability_cannot_escape_root() {
        use std::os::unix::fs::symlink;

        let parent = TempDir::new().unwrap();
        let root_path = parent.path().join("public");
        std::fs::create_dir(&root_path).unwrap();
        std::fs::write(root_path.join("inside.txt"), b"inside").unwrap();
        std::fs::write(parent.path().join("outside.txt"), b"outside-secret").unwrap();
        symlink("inside.txt", root_path.join("inside-link.txt")).unwrap();
        symlink("../outside.txt", root_path.join("outside-link.txt")).unwrap();
        symlink(&root_path, parent.path().join("public-link")).unwrap();

        assert!(matches!(
            StaticFileMount::new(parent.path().join("public-link"), "/assets").await,
            Err(StaticFileError::RootSymlinkDenied)
        ));

        let denied = StaticFileMount::new(&root_path, "/assets").await.unwrap();
        let request = Request::new_test("GET", "/assets/inside-link.txt");
        assert!(matches!(
            denied.serve(&request).await,
            Err(StaticFileError::SymlinkDenied)
        ));

        let allowed = StaticFileMount::with_symlink_policy(
            &root_path,
            "/assets",
            StaticFileSymlinkPolicy::AllowWithinRoot,
        )
        .await
        .unwrap();
        let mut inside = Request::new_test("GET", "/assets/inside-link.txt");
        let response = allowed.serve(&inside).await.unwrap();
        let mut parts = materialize(response, &mut inside).await;
        assert_eq!(collect_stream(&mut parts).await, b"inside");

        let outside = Request::new_test("GET", "/assets/outside-link.txt");
        assert!(matches!(
            allowed.serve(&outside).await,
            Err(StaticFileError::NotFound | StaticFileError::OpenFailed)
        ));
    }
}
