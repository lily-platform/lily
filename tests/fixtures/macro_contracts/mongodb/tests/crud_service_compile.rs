#[test]
fn crud_service_contracts_are_checked_at_compile_time() {
    let tests = trybuild::TestCases::new();
    tests.pass("tests/ui/crud_service_valid.rs");
    tests.compile_fail("tests/ui/crud_service_missing_dto_to_entity.rs");
    tests.compile_fail("tests/ui/crud_service_missing_entity_to_dto.rs");
    tests.compile_fail("tests/ui/crud_service_wrong_repository_field.rs");
    tests.compile_fail("tests/ui/crud_service_repository_trait_missing.rs");
}
