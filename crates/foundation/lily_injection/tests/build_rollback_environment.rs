use lily_error::injection::InjectionError;
use lily_injection::{
    ApplicationContainer, BUILD_ROLLBACK_TIMEOUT_ENV, DEFAULT_SHUTDOWN_TIMEOUT,
    MAX_BUILD_ROLLBACK_TIMEOUT_SECS,
};
use std::ffi::OsString;
use std::time::Duration;

struct EnvironmentRestore(Option<OsString>);

impl EnvironmentRestore {
    fn capture() -> Self {
        Self(std::env::var_os(BUILD_ROLLBACK_TIMEOUT_ENV))
    }
}

impl Drop for EnvironmentRestore {
    fn drop(&mut self) {
        // SAFETY: this integration-test binary contains one test, performs no
        // concurrent environment reads, and restores the prior process value.
        unsafe {
            match self.0.take() {
                Some(value) => std::env::set_var(BUILD_ROLLBACK_TIMEOUT_ENV, value),
                None => std::env::remove_var(BUILD_ROLLBACK_TIMEOUT_ENV),
            }
        }
    }
}

#[test]
fn build_rollback_timeout_environment_is_optional_bounded_and_fail_closed() {
    let _restore = EnvironmentRestore::capture();

    // SAFETY: this is the only test in its process and no worker threads have
    // been started while these bootstrap values are changed.
    unsafe { std::env::remove_var(BUILD_ROLLBACK_TIMEOUT_ENV) };
    let build = lily_injection::__private::begin_application_container_build(
        ApplicationContainer::builder(),
    );
    assert_eq!(build.rollback_timeout(), DEFAULT_SHUTDOWN_TIMEOUT);
    drop(build);

    // SAFETY: see the single-test-process invariant above.
    unsafe { std::env::set_var(BUILD_ROLLBACK_TIMEOUT_ENV, "7") };
    let build = lily_injection::__private::begin_application_container_build(
        ApplicationContainer::builder(),
    );
    assert_eq!(build.rollback_timeout(), Duration::from_secs(7));
    drop(build);

    // Invalid input is rejected before the container graph is prepared.
    for invalid in ["", "0", " 7", "+7", "7s", "18446744073709551616"] {
        // SAFETY: see the single-test-process invariant above.
        unsafe { std::env::set_var(BUILD_ROLLBACK_TIMEOUT_ENV, invalid) };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime");
        let error = runtime
            .block_on(ApplicationContainer::builder().build())
            .expect_err("invalid rollback bootstrap value must fail closed");
        let InjectionError::InitError(detail) = error else {
            panic!("expected a typed initialization error: {error:?}")
        };
        assert!(detail.contains(BUILD_ROLLBACK_TIMEOUT_ENV));
        assert!(detail.contains("between 1 and"));
    }

    // SAFETY: see the single-test-process invariant above.
    unsafe {
        std::env::set_var(
            BUILD_ROLLBACK_TIMEOUT_ENV,
            MAX_BUILD_ROLLBACK_TIMEOUT_SECS.to_string(),
        )
    };
    let build = lily_injection::__private::begin_application_container_build(
        ApplicationContainer::builder(),
    );
    assert_eq!(
        build.rollback_timeout(),
        Duration::from_secs(MAX_BUILD_ROLLBACK_TIMEOUT_SECS)
    );
}
