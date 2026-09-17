#[test]
fn configuration_background_and_queue_do_not_expose_di() {
    let tests = trybuild::TestCases::new();
    // Exact diagnostics also detect partially restored export groups.
    tests.compile_fail("tests/ui/config_exports.rs");
    tests.compile_fail("tests/ui/config_hidden_runtime.rs");
    tests.compile_fail("tests/ui/background_exports.rs");
    tests.compile_fail("tests/ui/queue_exports.rs");
    tests.compile_fail("tests/ui/macro_without_di.rs");
}
