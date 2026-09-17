use std::collections::{BTreeSet, HashMap, HashSet};

use crate::registry::RouteInfo;

pub(crate) const MAX_ROUTE_TEMPLATE_BYTES: usize = 4 * 1024;

/// Two controller actions registered the same normalized method and path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuplicateRouteError {
    /// Normalized HTTP method shared by both registrations.
    pub method: String,
    /// Canonical path shared by both registrations.
    pub path: String,
    /// Fully qualified name of the first action.
    pub first_handler: String,
    /// Fully qualified name of the duplicate action.
    pub duplicate_handler: String,
}

impl std::fmt::Display for DuplicateRouteError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "duplicate route {} {} is registered by '{}' and '{}'",
            self.method, self.path, self.first_handler, self.duplicate_handler
        )
    }
}

impl std::error::Error for DuplicateRouteError {}

/// Structural reason a route template could not be admitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum InvalidRouteReason {
    /// The method token was empty.
    EmptyMethod,
    /// The method was not a valid HTTP token.
    InvalidMethod,
    /// The route template exceeded Lily's bounded metadata limit.
    PathTooLong,
    /// The route template did not begin with `/`.
    PathMustStartWithSlash,
    /// A route template contained a query string or fragment.
    QueryOrFragmentNotAllowed,
    /// A route template contained an empty interior segment.
    EmptySegment,
    /// A named or catch-all parameter did not use a valid ASCII identifier.
    InvalidParameterName,
    /// A parameter name appeared more than once in one template.
    DuplicateParameterName,
    /// A catch-all parameter was not the final segment.
    CatchAllMustBeFinal,
}

impl std::fmt::Display for InvalidRouteReason {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::EmptyMethod => "HTTP method cannot be empty",
            Self::InvalidMethod => "HTTP method is not a valid token",
            Self::PathTooLong => "route template exceeds the configured limit",
            Self::PathMustStartWithSlash => "route template must start with '/'",
            Self::QueryOrFragmentNotAllowed => {
                "route template cannot contain a query string or fragment"
            }
            Self::EmptySegment => "route template cannot contain an empty segment",
            Self::InvalidParameterName => {
                "route parameters must use :name with an ASCII identifier"
            }
            Self::DuplicateParameterName => {
                "route parameter names must be unique within one template"
            }
            Self::CatchAllMustBeFinal => {
                "a catch-all route parameter must be the final path segment"
            }
        })
    }
}

/// One controller action registered an invalid method or route template.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidRouteError {
    /// Method supplied by the registration.
    pub method: String,
    /// Route template supplied by the registration.
    pub path: String,
    /// Fully qualified controller action name.
    pub handler: String,
    /// Structural validation failure.
    pub reason: InvalidRouteReason,
}

impl std::fmt::Display for InvalidRouteError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "invalid route {} {} (handler '{}'): {}",
            self.method, self.path, self.handler, self.reason
        )
    }
}

impl std::error::Error for InvalidRouteError {}

/// Two parameterized route templates had equal precedence for one method.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AmbiguousRouteError {
    /// Normalized HTTP method shared by both registrations.
    pub method: String,
    /// First ambiguous route template.
    pub first_path: String,
    /// Fully qualified action owning the first template.
    pub first_handler: String,
    /// Second ambiguous route template.
    pub second_path: String,
    /// Fully qualified action owning the second template.
    pub second_handler: String,
}

impl std::fmt::Display for AmbiguousRouteError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "ambiguous route templates {} {} (handler '{}') and {} (handler '{}') have equal precedence",
            self.method,
            self.first_path,
            self.first_handler,
            self.second_path,
            self.second_handler
        )
    }
}

impl std::error::Error for AmbiguousRouteError {}

/// Failure while freezing the immutable application route table.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RouteTableBuildError {
    /// Two actions registered one exact route.
    Duplicate(DuplicateRouteError),
    /// One action supplied an invalid route.
    Invalid(InvalidRouteError),
    /// Two templates could match with equal precedence.
    Ambiguous(AmbiguousRouteError),
}

impl std::fmt::Display for RouteTableBuildError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Duplicate(error) => error.fmt(formatter),
            Self::Invalid(error) => error.fmt(formatter),
            Self::Ambiguous(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for RouteTableBuildError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Duplicate(error) => Some(error),
            Self::Invalid(error) => Some(error),
            Self::Ambiguous(error) => Some(error),
        }
    }
}

pub(crate) enum RouteResolution<'a> {
    Found(&'a RouteInfo),
    MethodNotAllowed { allow: Vec<String> },
    NotFound,
}

/// Immutable route table with O(1) exact lookup and deterministic template lookup.
#[derive(Debug, Clone, Default)]
pub struct RouteTable {
    exact_routes: HashMap<String, RouteInfo>,
    param_routes: Vec<RouteInfo>,
    exact_count: usize,
    param_count: usize,
}

impl RouteTable {
    pub(crate) fn from_routes(mut routes: Vec<RouteInfo>) -> Result<Self, RouteTableBuildError> {
        for (route_plan_id, route) in routes.iter_mut().enumerate() {
            validate_and_normalize_route(route)?;
            route.route_plan_id = route_plan_id;
        }

        let mut exact_routes = HashMap::new();
        let mut param_routes = Vec::new();
        let mut registered = HashMap::<String, String>::new();

        for route in routes {
            let normalized_path = normalized_registration_path(&route.path);
            let registration_key = route_key(&route.method, &normalized_path);
            if let Some(first_handler) = registered.get(&registration_key) {
                return Err(RouteTableBuildError::Duplicate(DuplicateRouteError {
                    method: route.method.clone(),
                    path: route.path.clone(),
                    first_handler: first_handler.clone(),
                    duplicate_handler: route.handler_name.clone(),
                }));
            }
            registered.insert(registration_key, route.handler_name.clone());

            if is_parameterized(&route.path) {
                reject_equal_precedence_overlap(&param_routes, &route)?;
                param_routes.push(route);
            } else {
                exact_routes.insert(route_key(&route.method, &route.path), route);
            }
        }

        param_routes.sort_by(|left, right| {
            literal_segment_count(&right.path)
                .cmp(&literal_segment_count(&left.path))
                .then_with(|| {
                    fixed_segment_count(&right.path).cmp(&fixed_segment_count(&left.path))
                })
                .then_with(|| has_catch_all(&left.path).cmp(&has_catch_all(&right.path)))
                .then_with(|| left.method.cmp(&right.method))
                .then_with(|| left.path.cmp(&right.path))
                .then_with(|| left.handler_name.cmp(&right.handler_name))
        });

        Ok(Self {
            exact_count: exact_routes.len(),
            param_count: param_routes.len(),
            exact_routes,
            param_routes,
        })
    }

    pub(crate) fn routes(&self) -> impl Iterator<Item = &RouteInfo> {
        self.exact_routes.values().chain(self.param_routes.iter())
    }

    pub(crate) fn route_count(&self) -> usize {
        self.exact_count + self.param_count
    }

    pub(crate) fn resolve(&self, method: &str, path: &str) -> RouteResolution<'_> {
        if let Some(route) = self.find_route_for_method(method, path) {
            return RouteResolution::Found(route);
        }
        if method == "HEAD" {
            if let Some(route) = self.find_route_for_method("GET", path) {
                return RouteResolution::Found(route);
            }
        }

        let allow = self.allowed_methods(path);
        if allow.is_empty() {
            RouteResolution::NotFound
        } else {
            RouteResolution::MethodNotAllowed { allow }
        }
    }

    #[inline(always)]
    #[cfg(test)]
    pub(crate) fn find_route(&self, method: &str, path: &str) -> Option<&RouteInfo> {
        match self.resolve(method, path) {
            RouteResolution::Found(route) => Some(route),
            RouteResolution::MethodNotAllowed { .. } | RouteResolution::NotFound => None,
        }
    }

    #[inline(always)]
    fn find_route_for_method(&self, method: &str, path: &str) -> Option<&RouteInfo> {
        let key_len = method.len().saturating_add(path.len()).saturating_add(1);
        if key_len <= 256 {
            let mut key_buf = [0_u8; 256];
            let method_end = method.len();
            key_buf[..method_end].copy_from_slice(method.as_bytes());
            key_buf[method_end] = b':';
            key_buf[method_end + 1..key_len].copy_from_slice(path.as_bytes());
            let key = std::str::from_utf8(&key_buf[..key_len]).expect("route key is UTF-8");
            if let Some(route) = self.exact_routes.get(key) {
                return Some(route);
            }
        } else if let Some(route) = self.exact_routes.get(&route_key(method, path)) {
            return Some(route);
        }

        self.param_routes
            .iter()
            .find(|route| route.method == method && path_matches(&route.path, path))
    }

    fn allowed_methods(&self, path: &str) -> Vec<String> {
        let mut methods = BTreeSet::new();
        for route in self.routes().filter(|route| {
            if is_parameterized(&route.path) {
                path_matches(&route.path, path)
            } else {
                route.path == path
            }
        }) {
            methods.insert(route.method.clone());
            if route.method == "GET" {
                methods.insert("HEAD".to_string());
            }
        }
        methods.into_iter().collect()
    }

    #[inline(always)]
    pub(crate) fn extract_params(pattern: &str, path: &str) -> Vec<(String, String)> {
        let mut params = Vec::new();
        let mut pattern_segments = pattern.split('/');
        let mut path_segments = path.split('/');
        while let (Some(pattern_segment), Some(path_segment)) =
            (pattern_segments.next(), path_segments.next())
        {
            if let Some(name) = pattern_segment.strip_prefix(':') {
                params.push((name.to_string(), path_segment.to_string()));
            } else if let Some(name) = pattern_segment.strip_prefix('*') {
                let mut value = path_segment.to_string();
                for remaining in path_segments {
                    value.push('/');
                    value.push_str(remaining);
                }
                params.push((name.to_string(), value));
                break;
            }
        }
        params
    }

    pub fn stats(&self) -> RouteTableStats {
        RouteTableStats {
            exact_routes: self.exact_count,
            param_routes: self.param_count,
            total_routes: self.exact_count + self.param_count,
        }
    }
}

fn validate_and_normalize_route(route: &mut RouteInfo) -> Result<(), RouteTableBuildError> {
    if route.method.is_empty() {
        return invalid_route(route, InvalidRouteReason::EmptyMethod);
    }
    route.method.make_ascii_uppercase();
    if http::Method::from_bytes(route.method.as_bytes()).is_err() {
        return invalid_route(route, InvalidRouteReason::InvalidMethod);
    }
    if route.path.len() > MAX_ROUTE_TEMPLATE_BYTES {
        return invalid_route(route, InvalidRouteReason::PathTooLong);
    }
    if !route.path.starts_with('/') {
        return invalid_route(route, InvalidRouteReason::PathMustStartWithSlash);
    }
    if route.path.contains(['?', '#']) {
        return invalid_route(route, InvalidRouteReason::QueryOrFragmentNotAllowed);
    }
    if route.path != "/" && route.path.split('/').skip(1).any(str::is_empty) {
        return invalid_route(route, InvalidRouteReason::EmptySegment);
    }

    let mut parameter_names = HashSet::new();
    let segments = route.path.split('/').skip(1).collect::<Vec<_>>();
    for (index, segment) in segments.iter().enumerate() {
        let name = if let Some(name) = segment.strip_prefix(':') {
            Some(name)
        } else if let Some(name) = segment.strip_prefix('*') {
            if index + 1 != segments.len() {
                return invalid_route(route, InvalidRouteReason::CatchAllMustBeFinal);
            }
            Some(name)
        } else {
            None
        };
        let Some(name) = name else {
            continue;
        };
        if !is_parameter_name(name) {
            return invalid_route(route, InvalidRouteReason::InvalidParameterName);
        }
        if !parameter_names.insert(name) {
            return invalid_route(route, InvalidRouteReason::DuplicateParameterName);
        }
    }
    Ok(())
}

fn invalid_route<T>(
    route: &RouteInfo,
    reason: InvalidRouteReason,
) -> Result<T, RouteTableBuildError> {
    Err(RouteTableBuildError::Invalid(InvalidRouteError {
        method: route.method.clone(),
        path: route.path.clone(),
        handler: route.handler_name.clone(),
        reason,
    }))
}

fn is_parameter_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    matches!(bytes.next(), Some(b'a'..=b'z' | b'A'..=b'Z' | b'_'))
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn reject_equal_precedence_overlap(
    existing_routes: &[RouteInfo],
    candidate: &RouteInfo,
) -> Result<(), RouteTableBuildError> {
    for existing in existing_routes {
        if existing.method == candidate.method
            && literal_segment_count(&existing.path) == literal_segment_count(&candidate.path)
            && fixed_segment_count(&existing.path) == fixed_segment_count(&candidate.path)
            && has_catch_all(&existing.path) == has_catch_all(&candidate.path)
            && templates_overlap(&existing.path, &candidate.path)
        {
            return Err(RouteTableBuildError::Ambiguous(AmbiguousRouteError {
                method: candidate.method.clone(),
                first_path: existing.path.clone(),
                first_handler: existing.handler_name.clone(),
                second_path: candidate.path.clone(),
                second_handler: candidate.handler_name.clone(),
            }));
        }
    }
    Ok(())
}

fn templates_overlap(left: &str, right: &str) -> bool {
    let left = left.split('/').skip(1).collect::<Vec<_>>();
    let right = right.split('/').skip(1).collect::<Vec<_>>();
    let left_fixed = left
        .iter()
        .position(|segment| segment.starts_with('*'))
        .unwrap_or(left.len());
    let right_fixed = right
        .iter()
        .position(|segment| segment.starts_with('*'))
        .unwrap_or(right.len());
    for index in 0..left_fixed.min(right_fixed) {
        let left_segment = left[index];
        let right_segment = right[index];
        if !left_segment.starts_with(':')
            && !right_segment.starts_with(':')
            && left_segment != right_segment
        {
            return false;
        }
    }
    match (left_fixed < left.len(), right_fixed < right.len()) {
        (false, false) => left_fixed == right_fixed,
        (true, false) => right_fixed > left_fixed,
        (false, true) => left_fixed > right_fixed,
        (true, true) => true,
    }
}

fn path_matches(pattern: &str, path: &str) -> bool {
    let mut pattern = pattern.split('/');
    let mut path_segments = path.split('/');
    loop {
        match (pattern.next(), path_segments.next()) {
            (Some(pattern), Some(path_segment)) => {
                if pattern.starts_with('*') {
                    return !path_segment.is_empty()
                        && path_segments.all(|segment| !segment.is_empty());
                }
                if pattern.starts_with(':') {
                    if path_segment.is_empty() {
                        return false;
                    }
                } else if pattern != path_segment {
                    return false;
                }
            }
            (None, None) => return true,
            _ => return false,
        }
    }
}

fn normalized_registration_path(path: &str) -> String {
    path.split('/')
        .map(|segment| {
            if segment.starts_with(':') {
                ":"
            } else if segment.starts_with('*') {
                "*"
            } else {
                segment
            }
        })
        .collect::<Vec<_>>()
        .join("/")
}

fn literal_segment_count(path: &str) -> usize {
    path.split('/')
        .skip(1)
        .filter(|segment| !segment.starts_with(':') && !segment.starts_with('*'))
        .count()
}

fn is_parameterized(path: &str) -> bool {
    path.split('/')
        .any(|segment| segment.starts_with(':') || segment.starts_with('*'))
}

fn fixed_segment_count(path: &str) -> usize {
    path.split('/')
        .skip(1)
        .take_while(|segment| !segment.starts_with('*'))
        .count()
}

fn has_catch_all(path: &str) -> bool {
    path.split('/').any(|segment| segment.starts_with('*'))
}

fn route_key(method: &str, path: &str) -> String {
    format!("{method}:{path}")
}

/// Counts of exact and parameterized routes frozen into an application.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteTableStats {
    /// Routes resolved through exact-key lookup.
    pub exact_routes: usize,
    /// Routes resolved through deterministic template matching.
    pub param_routes: usize,
    /// Total number of admitted routes.
    pub total_routes: usize,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::handler::Handler;

    fn route(method: &str, path: &str, handler_name: &str) -> RouteInfo {
        RouteInfo {
            method: method.to_string(),
            path: path.to_string(),
            handler: Handler::new(Arc::new(|_, _, _| Box::pin(async { Ok(()) })), false),
            handler_name: handler_name.to_string(),
            guard_type_ids: Vec::new(),
            middleware_registrations: Vec::new(),
            cors_policy_registration: crate::private::CorsRoutePolicyRegistration::inherit(),
            route_plan_id: 0,
        }
    }

    #[test]
    fn exact_and_more_specific_templates_have_stable_precedence() {
        for routes in [
            vec![
                route("GET", "/:group/:id", "generic"),
                route("GET", "/items/:id", "items"),
                route("GET", "/items/new", "exact"),
            ],
            vec![
                route("GET", "/items/new", "exact"),
                route("GET", "/items/:id", "items"),
                route("GET", "/:group/:id", "generic"),
            ],
        ] {
            let table = RouteTable::from_routes(routes).unwrap();
            assert_eq!(
                table.find_route("GET", "/items/new").unwrap().handler_name,
                "exact"
            );
            assert_eq!(
                table.find_route("GET", "/items/42").unwrap().handler_name,
                "items"
            );
            assert_eq!(
                table.find_route("GET", "/users/42").unwrap().handler_name,
                "generic"
            );
        }
    }

    #[test]
    fn crossing_equal_specificity_templates_are_rejected_in_both_orders() {
        for routes in [
            vec![
                route("GET", "/a/:x", "left"),
                route("GET", "/:y/b", "right"),
            ],
            vec![
                route("GET", "/:y/b", "right"),
                route("GET", "/a/:x", "left"),
            ],
        ] {
            assert!(matches!(
                RouteTable::from_routes(routes),
                Err(RouteTableBuildError::Ambiguous(_))
            ));
        }
    }

    #[test]
    fn grammar_and_duplicate_parameter_shapes_fail_closed() {
        for (path, reason) in [
            ("items/:id", InvalidRouteReason::PathMustStartWithSlash),
            ("/items/", InvalidRouteReason::EmptySegment),
            ("/items//:id", InvalidRouteReason::EmptySegment),
            ("/items/:", InvalidRouteReason::InvalidParameterName),
            ("/items/:9id", InvalidRouteReason::InvalidParameterName),
            ("/items/*", InvalidRouteReason::InvalidParameterName),
            ("/items/*path/tail", InvalidRouteReason::CatchAllMustBeFinal),
            ("/items/:id/:id", InvalidRouteReason::DuplicateParameterName),
            (
                "/items/:path/*path",
                InvalidRouteReason::DuplicateParameterName,
            ),
            (
                "/items/:id?x=1",
                InvalidRouteReason::QueryOrFragmentNotAllowed,
            ),
        ] {
            assert!(matches!(
                RouteTable::from_routes(vec![route("GET", path, "invalid")]),
                Err(RouteTableBuildError::Invalid(InvalidRouteError { reason: actual, .. }))
                    if actual == reason
            ));
        }

        assert!(matches!(
            RouteTable::from_routes(vec![
                route("GET", "/items/:id", "first"),
                route("GET", "/items/:item_id", "second"),
            ]),
            Err(RouteTableBuildError::Duplicate(_))
        ));
    }

    #[test]
    fn head_falls_back_to_get_and_method_miss_reports_sorted_allow() {
        let table = RouteTable::from_routes(vec![
            route("POST", "/items/:id", "post"),
            route("GET", "/items/:id", "get"),
        ])
        .unwrap();

        assert_eq!(
            table.find_route("HEAD", "/items/42").unwrap().handler_name,
            "get"
        );
        assert!(matches!(
            table.resolve("DELETE", "/items/42"),
            RouteResolution::MethodNotAllowed { allow }
                if allow == vec!["GET", "HEAD", "POST"]
        ));
        assert!(matches!(
            table.resolve("GET", "/missing"),
            RouteResolution::NotFound
        ));
    }

    #[test]
    fn empty_parameter_segments_do_not_match() {
        let table = RouteTable::from_routes(vec![route("GET", "/items/:id", "item")]).unwrap();
        assert!(table.find_route("GET", "/items/").is_none());
    }

    #[test]
    fn final_catch_all_matches_nested_paths_and_extracts_the_exact_remainder() {
        let table = RouteTable::from_routes(vec![route("GET", "/assets/*path", "assets")])
            .expect("valid catch-all route");
        assert_eq!(
            table
                .find_route("GET", "/assets/css/site.css")
                .unwrap()
                .handler_name,
            "assets"
        );
        assert_eq!(
            table
                .find_route("HEAD", "/assets/images/logo.svg")
                .unwrap()
                .handler_name,
            "assets"
        );
        assert!(table.find_route("GET", "/assets/").is_none());
        assert!(table.find_route("GET", "/assets//logo.svg").is_none());
        assert_eq!(
            RouteTable::extract_params("/assets/*path", "/assets/a%20b/logo.svg"),
            vec![("path".to_string(), "a%20b/logo.svg".to_string())]
        );
    }

    #[test]
    fn fixed_and_single_segment_routes_precede_catch_all_routes() {
        for routes in [
            vec![
                route("GET", "/assets/*path", "catch_all"),
                route("GET", "/assets/:name", "single"),
                route("GET", "/assets/status", "exact"),
            ],
            vec![
                route("GET", "/assets/status", "exact"),
                route("GET", "/assets/:name", "single"),
                route("GET", "/assets/*path", "catch_all"),
            ],
        ] {
            let table = RouteTable::from_routes(routes).unwrap();
            assert_eq!(
                table
                    .find_route("GET", "/assets/status")
                    .unwrap()
                    .handler_name,
                "exact"
            );
            assert_eq!(
                table
                    .find_route("GET", "/assets/logo.svg")
                    .unwrap()
                    .handler_name,
                "single"
            );
            assert_eq!(
                table
                    .find_route("GET", "/assets/css/site.css")
                    .unwrap()
                    .handler_name,
                "catch_all"
            );
        }
    }
}
