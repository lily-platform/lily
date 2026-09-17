//! Internal lifetime validation shared by registry planning and unit tests.

use crate::ServiceLifetime;

/// Internal compatibility helpers used while validating the service graph.
pub(crate) mod lifetime_utils {
    use super::*;

    /// Validate that a service lifetime is compatible with its dependencies
    pub(crate) fn validate_lifetime_compatibility(
        service_lifetime: ServiceLifetime,
        dependency_lifetime: ServiceLifetime,
    ) -> Result<(), String> {
        match (service_lifetime, dependency_lifetime) {
            // ✅ ALLOWED: Same or longer lifetime dependencies
            (ServiceLifetime::Singleton, ServiceLifetime::Singleton) => Ok(()),
            // A transient captured during singleton construction becomes a
            // root-owned instance. It remains a fresh value per resolution,
            // is recorded before its owning singleton in the common root
            // ledger, and is therefore disposed after that singleton.
            (ServiceLifetime::Singleton, ServiceLifetime::Transient) => Ok(()),
            (ServiceLifetime::Scoped, ServiceLifetime::Singleton) => Ok(()),
            (ServiceLifetime::Scoped, ServiceLifetime::Scoped) => Ok(()),
            // A scoped service may own transient dependencies because every
            // transient instance is recorded in that scope's lifecycle ledger
            // and disposed dependants-first when the scope closes.
            (ServiceLifetime::Scoped, ServiceLifetime::Transient) => Ok(()),
            (ServiceLifetime::Transient, ServiceLifetime::Singleton) => Ok(()),
            (ServiceLifetime::Transient, ServiceLifetime::Scoped) => Ok(()),
            (ServiceLifetime::Transient, ServiceLifetime::Transient) => Ok(()),

            // ❌ FORBIDDEN: Shorter lifetime dependencies
            (ServiceLifetime::Singleton, ServiceLifetime::Scoped) => {
                Err("Singleton service cannot depend on Scoped service. Use factory pattern or make dependency Singleton.".to_string())
            }
        }
    }

    /// Check if a service lifetime requires context for resolution
    #[cfg(test)]
    pub(crate) fn requires_context(lifetime: ServiceLifetime) -> bool {
        matches!(lifetime, ServiceLifetime::Scoped)
    }

    /// Get human-readable lifetime description
    #[cfg(test)]
    pub(crate) fn lifetime_description(lifetime: ServiceLifetime) -> &'static str {
        match lifetime {
            ServiceLifetime::Singleton => "Singleton (one instance per application)",
            ServiceLifetime::Scoped => "Scoped (one instance per request/process)",
            ServiceLifetime::Transient => "Transient (new instance every time)",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::lifetime_utils::*;
    use super::*;

    #[test]
    fn test_lifetime_compatibility_valid() {
        // Valid combinations
        assert!(validate_lifetime_compatibility(
            ServiceLifetime::Singleton,
            ServiceLifetime::Singleton
        )
        .is_ok());

        assert!(validate_lifetime_compatibility(
            ServiceLifetime::Scoped,
            ServiceLifetime::Singleton
        )
        .is_ok());

        assert!(validate_lifetime_compatibility(
            ServiceLifetime::Transient,
            ServiceLifetime::Scoped
        )
        .is_ok());

        assert!(validate_lifetime_compatibility(
            ServiceLifetime::Scoped,
            ServiceLifetime::Transient
        )
        .is_ok());
    }

    #[test]
    fn test_lifetime_compatibility_invalid() {
        // Invalid combinations
        assert!(validate_lifetime_compatibility(
            ServiceLifetime::Singleton,
            ServiceLifetime::Scoped
        )
        .is_err());

        assert!(validate_lifetime_compatibility(
            ServiceLifetime::Singleton,
            ServiceLifetime::Transient
        )
        .is_ok());
    }

    #[test]
    fn test_requires_context() {
        assert!(!requires_context(ServiceLifetime::Singleton));
        assert!(requires_context(ServiceLifetime::Scoped));
        assert!(!requires_context(ServiceLifetime::Transient));
    }

    #[test]
    fn test_lifetime_descriptions() {
        assert!(lifetime_description(ServiceLifetime::Singleton)
            .contains("one instance per application"));
        assert!(lifetime_description(ServiceLifetime::Scoped).contains("per request"));
        assert!(lifetime_description(ServiceLifetime::Transient).contains("new instance"));
    }
}
