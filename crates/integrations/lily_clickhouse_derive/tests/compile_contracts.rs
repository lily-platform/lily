#[test]
fn clickhouse_derive_compile_contracts() {
    let tests = trybuild::TestCases::new();
    tests.pass("tests/ui/pass_schema.rs");
    tests.compile_fail("tests/ui/fail_schema_unknown_attribute.rs");
    tests.compile_fail("tests/ui/fail_schema_invalid_identifier.rs");
    tests.compile_fail("tests/ui/fail_schema_invalid_engine.rs");
    tests.compile_fail("tests/ui/fail_schema_unknown_order_column.rs");
    tests.compile_fail("tests/ui/fail_schema_unsafe_type.rs");
    tests.compile_fail("tests/ui/fail_table_missing_entity.rs");
    tests.compile_fail("tests/ui/fail_repository_missing_table.rs");
}
