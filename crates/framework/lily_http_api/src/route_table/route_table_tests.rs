// //! Comprehensive unit tests for framework/route_table.rs
// //!
// //! Tests RouteTable, RouteTableStats, exact/parameter route matching,
// //! zero-allocation optimizations, and performance characteristics.

// use crate::route_table::{RouteTable, RouteTableStats};
// use crate::registry::RouteInfo;
// use crate::handler::Handler;
// use crate::extension::Extensions;
// use crate::request::Request;
// use crate::response::Response;
// use akinci_test_core::init_test_env;
// use std::sync::Arc;

// /// Initialize test environment for all route table tests
// fn setup_route_table_test() {
//     init_test_env();
// }

// // Helper function to create test handler
// fn create_test_handler(name: &str) -> Handler {
//     let handler_name = name.to_string();
//     let handler_fn = Arc::new(move |_ext: &Extensions, _req: Request, _rsp: &mut Response| -> std::io::Result<()> {
//         Ok(())
//     });
//     Handler::new(handler_fn, false)
// }

// #[cfg(test)]
// mod route_table_creation_tests {
//     use super::*;

//     #[test]
//     fn test_route_table_new() {
//         setup_route_table_test();

//         let route_table = RouteTable::new();
//         let stats = route_table.stats();

//         assert_eq!(stats.exact_routes, 0);
//         assert_eq!(stats.param_routes, 0);
//         assert_eq!(stats.total_routes, 0);
//     }

//     #[test]
//     fn test_route_table_add_exact_routes() {
//         setup_route_table_test();

//         let mut route_table = RouteTable::new();
//         let handler = create_test_handler("test");

//         route_table.add_route("GET", "/api/health", handler.clone());
//         route_table.add_route("POST", "/api/users", handler.clone());
//         route_table.add_route("DELETE", "/api/sessions", handler);

//         let stats = route_table.stats();
//         assert_eq!(stats.exact_routes, 3);
//         assert_eq!(stats.param_routes, 0);
//         assert_eq!(stats.total_routes, 3);
//     }

//     #[test]
//     fn test_route_table_add_param_routes() {
//         setup_route_table_test();

//         let mut route_table = RouteTable::new();
//         let handler = create_test_handler("param_test");

//         route_table.add_route("GET", "/api/users/:id", handler.clone());
//         route_table.add_route("PUT", "/api/posts/:id/comments/:comment_id", handler.clone());
//         route_table.add_route("DELETE", "/api/files/:path", handler);

//         let stats = route_table.stats();
//         assert_eq!(stats.exact_routes, 0);
//         assert_eq!(stats.param_routes, 3);
//         assert_eq!(stats.total_routes, 3);
//     }

//     #[test]
//     fn test_route_table_mixed_routes() {
//         setup_route_table_test();

//         let mut route_table = RouteTable::new();
//         let handler = create_test_handler("mixed_test");

//         // Add exact routes
//         route_table.add_route("GET", "/api/health", handler.clone());
//         route_table.add_route("POST", "/api/login", handler.clone());

//         // Add param routes
//         route_table.add_route("GET", "/api/users/:id", handler.clone());
//         route_table.add_route("PUT", "/api/posts/:id", handler);

//         let stats = route_table.stats();
//         assert_eq!(stats.exact_routes, 2);
//         assert_eq!(stats.param_routes, 2);
//         assert_eq!(stats.total_routes, 4);
//     }
// }

// #[cfg(test)]
// mod route_matching_tests {
//     use super::*;

//     #[test]
//     fn test_exact_route_matching() {
//         setup_route_table_test();

//         let mut route_table = RouteTable::new();
//         let handler = create_test_handler("exact_match");

//         route_table.add_route("GET", "/api/health", handler.clone());
//         route_table.add_route("POST", "/api/users", handler.clone());
//         route_table.add_route("DELETE", "/api/sessions", handler);

//         // Test successful matches
//         assert!(route_table.find_route("GET", "/api/health").is_some());
//         assert!(route_table.find_route("POST", "/api/users").is_some());
//         assert!(route_table.find_route("DELETE", "/api/sessions").is_some());

//         // Test method mismatch
//         assert!(route_table.find_route("POST", "/api/health").is_none());
//         assert!(route_table.find_route("GET", "/api/users").is_none());

//         // Test path mismatch
//         assert!(route_table.find_route("GET", "/api/status").is_none());
//         assert!(route_table.find_route("POST", "/api/login").is_none());
//     }

//     #[test]
//     fn test_parameter_route_matching() {
//         setup_route_table_test();

//         let mut route_table = RouteTable::new();
//         let handler = create_test_handler("param_match");

//         route_table.add_route("GET", "/api/users/:id", handler.clone());
//         route_table.add_route("PUT", "/api/posts/:id/comments/:comment_id", handler);

//         // Test successful parameter matches
//         assert!(route_table.find_route("GET", "/api/users/123").is_some());
//         assert!(route_table.find_route("GET", "/api/users/abc").is_some());
//         assert!(route_table.find_route("PUT", "/api/posts/456/comments/789").is_some());

//         // Test parameter count mismatch
//         assert!(route_table.find_route("GET", "/api/users").is_none());
//         assert!(route_table.find_route("GET", "/api/users/123/extra").is_none());
//         assert!(route_table.find_route("PUT", "/api/posts/456/comments").is_none());

//         // Test method mismatch
//         assert!(route_table.find_route("POST", "/api/users/123").is_none());
//         assert!(route_table.find_route("DELETE", "/api/posts/456/comments/789").is_none());
//     }

//     #[test]
//     fn test_mixed_route_matching() {
//         setup_route_table_test();

//         let mut route_table = RouteTable::new();
//         let handler = create_test_handler("mixed_match");

//         // Add both exact and parameter routes
//         route_table.add_route("GET", "/api/health", handler.clone());
//         route_table.add_route("GET", "/api/users/:id", handler.clone());
//         route_table.add_route("POST", "/api/users", handler);

//         // Test exact route takes precedence over parameter route
//         assert!(route_table.find_route("GET", "/api/health").is_some());
//         assert!(route_table.find_route("POST", "/api/users").is_some());

//         // Test parameter route matching
//         assert!(route_table.find_route("GET", "/api/users/123").is_some());

//         // Test non-matching routes
//         assert!(route_table.find_route("DELETE", "/api/health").is_none());
//         assert!(route_table.find_route("GET", "/api/posts/123").is_none());
//     }
// }

// #[cfg(test)]
// mod parameter_extraction_tests {
//     use super::*;

//     #[test]
//     fn test_extract_params_single() {
//         setup_route_table_test();

//         let params = RouteTable::extract_params("/api/users/:id", "/api/users/123");

//         assert_eq!(params.len(), 1);
//         assert_eq!(params[0].0, "id");
//         assert_eq!(params[0].1, "123");
//     }

//     #[test]
//     fn test_extract_params_multiple() {
//         setup_route_table_test();

//         let params = RouteTable::extract_params(
//             "/api/posts/:id/comments/:comment_id",
//             "/api/posts/456/comments/789"
//         );

//         assert_eq!(params.len(), 2);
//         assert_eq!(params[0].0, "id");
//         assert_eq!(params[0].1, "456");
//         assert_eq!(params[1].0, "comment_id");
//         assert_eq!(params[1].1, "789");
//     }

//     #[test]
//     fn test_extract_params_no_params() {
//         setup_route_table_test();

//         let params = RouteTable::extract_params("/api/health", "/api/health");

//         assert_eq!(params.len(), 0);
//     }

//     #[test]
//     fn test_extract_params_to_buffer() {
//         setup_route_table_test();

//         let mut params_buf = Vec::new();

//         RouteTable::extract_params_to_buffer(
//             "/api/users/:id/posts/:post_id",
//             "/api/users/alice/posts/hello-world",
//             &mut params_buf
//         );

//         assert_eq!(params_buf.len(), 2);
//         assert_eq!(params_buf[0].0, "id");
//         assert_eq!(params_buf[0].1, "alice");
//         assert_eq!(params_buf[1].0, "post_id");
//         assert_eq!(params_buf[1].1, "hello-world");
//     }

//     #[test]
//     fn test_extract_params_to_buffer_reuse() {
//         setup_route_table_test();

//         let mut params_buf = Vec::new();

//         // First extraction
//         RouteTable::extract_params_to_buffer(
//             "/api/users/:id",
//             "/api/users/123",
//             &mut params_buf
//         );
//         assert_eq!(params_buf.len(), 1);

//         // Second extraction should clear and reuse buffer
//         RouteTable::extract_params_to_buffer(
//             "/api/posts/:id/comments/:comment_id",
//             "/api/posts/456/comments/789",
//             &mut params_buf
//         );
//         assert_eq!(params_buf.len(), 2);
//         assert_eq!(params_buf[0].0, "id");
//         assert_eq!(params_buf[0].1, "456");
//     }
// }

// #[cfg(test)]
// mod route_table_stats_tests {
//     use super::*;

//     #[test]
//     fn test_route_table_stats_empty() {
//         setup_route_table_test();

//         let route_table = RouteTable::new();
//         let stats = route_table.stats();

//         assert_eq!(stats.exact_routes, 0);
//         assert_eq!(stats.param_routes, 0);
//         assert_eq!(stats.total_routes, 0);
//         assert_eq!(stats.performance_ratio(), 1.0);
//     }

//     #[test]
//     fn test_route_table_stats_exact_only() {
//         setup_route_table_test();

//         let mut route_table = RouteTable::new();
//         let handler = create_test_handler("stats_exact");

//         for i in 0..10 {
//             route_table.add_route("GET", &format!("/api/endpoint{}", i), handler.clone());
//         }

//         let stats = route_table.stats();
//         assert_eq!(stats.exact_routes, 10);
//         assert_eq!(stats.param_routes, 0);
//         assert_eq!(stats.total_routes, 10);
//         assert!(stats.performance_ratio() > 1.0);
//     }

//     #[test]
//     fn test_route_table_stats_param_only() {
//         setup_route_table_test();

//         let mut route_table = RouteTable::new();
//         let handler = create_test_handler("stats_param");

//         for i in 0..5 {
//             route_table.add_route("GET", &format!("/api/resource{}/:id", i), handler.clone());
//         }

//         let stats = route_table.stats();
//         assert_eq!(stats.exact_routes, 0);
//         assert_eq!(stats.param_routes, 5);
//         assert_eq!(stats.total_routes, 5);
//         assert!(stats.performance_ratio() >= 1.0);
//     }

//     #[test]
//     fn test_route_table_stats_mixed() {
//         setup_route_table_test();

//         let mut route_table = RouteTable::new();
//         let handler = create_test_handler("stats_mixed");

//         // Add exact routes
//         for i in 0..7 {
//             route_table.add_route("GET", &format!("/api/exact{}", i), handler.clone());
//         }

//         // Add param routes
//         for i in 0..3 {
//             route_table.add_route("POST", &format!("/api/param{}/:id", i), handler.clone());
//         }

//         let stats = route_table.stats();
//         assert_eq!(stats.exact_routes, 7);
//         assert_eq!(stats.param_routes, 3);
//         assert_eq!(stats.total_routes, 10);
//         assert!(stats.performance_ratio() > 1.0);
//     }
// }

// #[cfg(test)]
// mod performance_tests {
//     use super::*;

//     #[test]
//     fn test_o1_exact_lookup_performance() {
//         setup_route_table_test();

//         let mut route_table = RouteTable::new();
//         let handler = create_test_handler("perf_exact");

//         // Add many exact routes
//         for i in 0..1000 {
//             route_table.add_route("GET", &format!("/api/endpoint{}", i), handler.clone());
//         }

//         // Test that lookup is still fast (O(1))
//         for i in [0, 500, 999] {
//             let route = route_table.find_route("GET", &format!("/api/endpoint{}", i));
//             assert!(route.is_some());
//         }

//         // Test non-existent route
//         assert!(route_table.find_route("GET", "/api/endpoint1000").is_none());
//     }

//     #[test]
//     fn test_parameter_matching_performance() {
//         setup_route_table_test();

//         let mut route_table = RouteTable::new();
//         let handler = create_test_handler("perf_param");

//         // Add many parameter routes
//         for i in 0..100 {
//             route_table.add_route("GET", &format!("/api/resource{}/:id", i), handler.clone());
//         }

//         // Test parameter matching (should be O(m) where m is number of param routes)
//         let route = route_table.find_route("GET", "/api/resource50/123");
//         assert!(route.is_some());

//         // Test non-matching parameter route
//         assert!(route_table.find_route("GET", "/api/resource100/123").is_none());
//     }

//     #[test]
//     fn test_parameter_extraction_basic() {
//         setup_route_table_test();

//         let mut params_buf = Vec::with_capacity(10);
//         let path = "/api/users/123/posts/post456";

//         RouteTable::extract_params_to_buffer(
//             "/api/users/:id/posts/:post_id",
//             path,
//             &mut params_buf
//         );

//         assert_eq!(params_buf.len(), 2);
//         assert_eq!(params_buf[0].0, "id");
//         assert_eq!(params_buf[0].1, "123");
//         assert_eq!(params_buf[1].0, "post_id");
//         assert_eq!(params_buf[1].1, "post456");
//     }
// }

// #[cfg(test)]
// mod edge_case_tests {
//     use super::*;

//     #[test]
//     fn test_empty_path_segments() {
//         setup_route_table_test();

//         let mut route_table = RouteTable::new();
//         let handler = create_test_handler("edge_empty");

//         route_table.add_route("GET", "/", handler.clone());
//         route_table.add_route("POST", "/api", handler);

//         assert!(route_table.find_route("GET", "/").is_some());
//         assert!(route_table.find_route("POST", "/api").is_some());
//         assert!(route_table.find_route("GET", "/api").is_none());
//     }

//     #[test]
//     fn test_special_characters_in_paths() {
//         setup_route_table_test();

//         let mut route_table = RouteTable::new();
//         let handler = create_test_handler("edge_special");

//         route_table.add_route("GET", "/api/files/:filename", handler);

//         // Test special characters in parameter values
//         assert!(route_table.find_route("GET", "/api/files/test.txt").is_some());
//         assert!(route_table.find_route("GET", "/api/files/file-name_123").is_some());
//         assert!(route_table.find_route("GET", "/api/files/path%2Fto%2Ffile").is_some());
//     }

//     #[test]
//     fn test_case_sensitivity() {
//         setup_route_table_test();

//         let mut route_table = RouteTable::new();
//         let handler = create_test_handler("edge_case");

//         route_table.add_route("GET", "/api/Health", handler.clone());
//         route_table.add_route("get", "/api/status", handler);

//         // Test case sensitivity in paths
//         assert!(route_table.find_route("GET", "/api/Health").is_some());
//         assert!(route_table.find_route("GET", "/api/health").is_none());

//         // Test case sensitivity in methods
//         assert!(route_table.find_route("get", "/api/status").is_some());
//         assert!(route_table.find_route("GET", "/api/status").is_none());
//     }

//     #[test]
//     fn test_very_long_paths() {
//         setup_route_table_test();

//         let mut route_table = RouteTable::new();
//         let handler = create_test_handler("edge_long");

//         // Test very long path (should trigger fallback allocation)
//         let long_path = format!("/api/{}", "very_long_segment_name".repeat(20));
//         route_table.add_route("GET", &long_path, handler);

//         assert!(route_table.find_route("GET", &long_path).is_some());
//     }
// }
