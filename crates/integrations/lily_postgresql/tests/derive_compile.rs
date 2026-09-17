#[test]
fn downstream_derive_contract_compile_passes() {
    let tests = trybuild::TestCases::new();
    tests.pass("tests/ui/pass_derive_contract.rs");
}
