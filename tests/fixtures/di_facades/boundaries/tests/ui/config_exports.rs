use lily_config::{
    async_trait, ApplicationContainer, ApplicationContainerBuilder, ApplicationScope,
    ApplicationScopeFactory, ContainerShutdownReport, Extensions, Injectable, InjectionError,
    ProcessContext, ServiceLifetime, ServiceScope, ServiceTrait, ShutdownOutcome,
    ShutdownOutcomeStatus, ShutdownRemainingWork, BUILD_ROLLBACK_TIMEOUT_ENV,
    DEFAULT_SHUTDOWN_TIMEOUT, MAX_BUILD_ROLLBACK_TIMEOUT_SECS,
};

fn main() {}
