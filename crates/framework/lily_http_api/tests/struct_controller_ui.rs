#[test]
fn struct_controller_compile_contracts() {
    let tests = trybuild::TestCases::new();
    tests.pass("tests/ui/struct_controller/pass_minimal.rs");
    tests.pass("tests/ui/struct_controller/pass_openapi_disabled_no_schema.rs");
    tests.pass("tests/ui/struct_controller/pass_openapi_metadata.rs");
    tests.pass("tests/ui/struct_controller/pass_typed_action.rs");
    tests.pass("tests/ui/struct_controller/pass_typed_body.rs");
    tests.pass("tests/ui/struct_controller/pass_typed_multipart.rs");
    tests.pass("tests/ui/struct_controller/pass_response_modes.rs");
    tests.compile_fail("tests/ui/struct_controller/fail_*.rs");
}
