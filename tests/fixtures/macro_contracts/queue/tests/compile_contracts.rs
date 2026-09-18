#![cfg(not(feature = "asyncapi"))]

#[test]
fn queue_macro_compile_contracts() {
    let tests = trybuild::TestCases::new();
    tests.pass("tests/ui/pass_queue_markers.rs");
    tests.pass("tests/ui/pass_versioned_queue_markers.rs");
    tests.pass("tests/ui/pass_duplicate_dispatch_metadata.rs");
    tests.pass("tests/ui/pass_pipeline_markers.rs");
    tests.pass("tests/ui/pass_typed_extractors.rs");
    tests.pass("tests/ui/pass_custom_parts_named_json.rs");
    tests.pass("tests/ui/pass_delivery_guarantees.rs");
    tests.compile_fail("tests/ui/fail_sync_handler.rs");
    tests.compile_fail("tests/ui/fail_standalone_queue.rs");
    tests.compile_fail("tests/ui/fail_unknown_parameter.rs");
    tests.compile_fail("tests/ui/fail_legacy_queue_args.rs");
    tests.compile_fail("tests/ui/fail_zero_version.rs");
    tests.compile_fail("tests/ui/fail_version_overflow.rs");
    tests.compile_fail("tests/ui/fail_reference_extractor.rs");
    tests.compile_fail("tests/ui/fail_too_many_extractors.rs");
    tests.compile_fail("tests/ui/fail_payload_not_terminal.rs");
    tests.compile_fail("tests/ui/fail_two_custom_payloads.rs");
    tests.compile_fail("tests/ui/fail_handler_error_not_convertible.rs");
    tests.compile_fail("tests/ui/fail_non_unit_success.rs");
    tests.compile_fail("tests/ui/fail_pipeline_marker_list.rs");
    tests.compile_fail("tests/ui/fail_pipeline_marker_without_queue.rs");
    tests.compile_fail("tests/ui/fail_pipeline_marker_before_queue_service.rs");
    tests.compile_fail("tests/ui/fail_pipeline_trait_bound.rs");
    tests.compile_fail("tests/ui/fail_pipeline_guard_trait_bound.rs");
    tests.compile_fail("tests/ui/fail_service_pipeline_without_handler.rs");
    tests.compile_fail("tests/ui/fail_delivery_guarantee_invalid.rs");
    tests.compile_fail("tests/ui/fail_delivery_guarantee_duplicate.rs");
    tests.compile_fail("tests/ui/fail_asyncapi_feature_disabled.rs");
}
