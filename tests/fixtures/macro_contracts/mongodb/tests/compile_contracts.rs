#[test]
fn mongodb_derive_compile_fail_contracts() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/ui/fail_collection_missing_type.rs");
    tests.compile_fail("tests/ui/fail_collection_invalid_index.rs");
    tests.compile_fail("tests/ui/fail_collection_invalid_combination.rs");
    tests.compile_fail("tests/ui/fail_collection_duplicate_attribute.rs");
    tests.compile_fail("tests/ui/fail_repository_missing_entity.rs");
}
