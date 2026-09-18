#[test]
fn interface_contracts_are_checked_at_compile_time() {
    let tests = trybuild::TestCases::new();
    tests.pass("tests/ui/interface_valid.rs");
    tests.pass("tests/ui/interface_injected_consumer.rs");
    tests.pass("tests/ui/lifetime_variants.rs");
    tests.pass("tests/ui/public_service.rs");
    tests.compile_fail("tests/ui/service_trait_missing.rs");
    tests.compile_fail("tests/ui/interface_missing_impl.rs");
    tests.compile_fail("tests/ui/interface_requires_dyn.rs");
    tests.compile_fail("tests/ui/interface_not_dyn_compatible.rs");
    tests.compile_fail("tests/ui/interface_not_send_sync.rs");
    tests.compile_fail("tests/ui/inject_requires_arc.rs");
    tests.compile_fail("tests/ui/injected_field_accessor_not_generated.rs");
    tests.compile_fail("tests/ui/disabled_invalid_syntax.rs");
    tests.compile_fail("tests/ui/config_parameter_removed.rs");
    tests.compile_fail("tests/ui/add_service_removed.rs");
    tests.compile_fail("tests/ui/ignored_container_close.rs");
    tests.compile_fail("tests/ui/invalid_lifetime.rs");
    tests.compile_fail("tests/ui/tuple_struct.rs");
    tests.compile_fail("tests/ui/runtime_state_requires_default.rs");
    tests.compile_fail("tests/ui/generic_service.rs");
}
