#[test]
fn websocket_macro_compile_fail_contracts() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/ui/fail_gateway_invalid_url.rs");
    tests.compile_fail("tests/ui/fail_gateway_missing_client.rs");
    tests.compile_fail("tests/ui/fail_controller_missing_namespace.rs");
    tests.compile_fail("tests/ui/fail_controller_message_signature.rs");
    tests.compile_fail("tests/ui/fail_controller_reference_input.rs");
    tests.compile_fail("tests/ui/fail_controller_raw_input.rs");
    tests.compile_fail("tests/ui/fail_controller_too_many_arguments.rs");
    tests.compile_fail("tests/ui/fail_controller_action_frame_codec.rs");
    tests.compile_fail("tests/ui/fail_controller_lifecycle_payload_codec.rs");
    tests.compile_fail("tests/ui/fail_controller_two_payload_consumers.rs");
    tests.compile_fail("tests/ui/fail_controller_connected_payload.rs");
    tests.compile_fail("tests/ui/fail_controller_disconnected_payload.rs");
    tests.compile_fail("tests/ui/fail_controller_handshake_middleware_trait.rs");
    tests.compile_fail("tests/ui/fail_controller_connection_middleware_trait.rs");
    tests.compile_fail("tests/ui/fail_controller_message_middleware_trait.rs");
    tests.compile_fail("tests/ui/fail_action_message_middleware_trait.rs");
    tests.compile_fail("tests/ui/fail_cancellation_authority.rs");
    tests.compile_fail("tests/ui/fail_controller_cancellation_phase.rs");
}

#[test]
fn websocket_macro_compile_pass_contracts() {
    let tests = trybuild::TestCases::new();
    tests.pass("tests/ui/pass_controller_handshake_connection_middleware.rs");
    tests.pass("tests/ui/pass_controller_message_middleware_guard.rs");
    tests.pass("tests/ui/pass_controller_custom_lifecycle_payload_name.rs");
    tests.pass("tests/ui/pass_controller_cancellation.rs");
}
