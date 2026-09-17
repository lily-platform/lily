use std::sync::Arc;

use async_trait::async_trait;
use lily_injection::Extensions;

/// Application-scoped controller construction contract.
///
/// A controller is initialized once for each application that materializes its
/// registered actions. Dependencies resolved here therefore live for at least
/// as long as the controller instance that stores them.
#[async_trait]
pub trait ControllerTrait: Send + Sync + 'static {
    /// Constructs the application-scoped controller from Lily's DI services.
    ///
    /// Lily calls this once while materializing the controller's registered
    /// actions. Return a typed error instead of falling back to a partially
    /// initialized controller.
    async fn new(extensions: Arc<Extensions>) -> Result<Self, ControllerInitError>
    where
        Self: Sized;
}

/// Secret-safe controller initialization failure categories.
///
/// The variants deliberately carry no raw dependency/configuration error. A
/// controller may map an internal error with [`Self::dependency`],
/// [`Self::invalid_configuration`] or [`Self::internal`] without publishing
/// connection strings, filesystem paths or other sensitive payloads through
/// the application build error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ControllerInitError {
    /// A required application setting was not supplied.
    MissingConfiguration,
    /// A supplied application setting failed validation.
    InvalidConfiguration,
    /// A controller dependency could not be resolved or initialized.
    Dependency,
    /// Controller initialization failed for another secret-safe reason.
    Internal,
}

impl ControllerInitError {
    /// Redacts a dependency error into the stable dependency category.
    pub fn dependency<E>(_source: E) -> Self {
        Self::Dependency
    }

    /// Redacts a configuration error into the stable invalid-config category.
    pub fn invalid_configuration<E>(_source: E) -> Self {
        Self::InvalidConfiguration
    }

    /// Redacts an implementation error into the stable internal category.
    pub fn internal<E>(_source: E) -> Self {
        Self::Internal
    }
}

impl std::fmt::Display for ControllerInitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::MissingConfiguration => "required controller configuration is missing",
            Self::InvalidConfiguration => "controller configuration is invalid",
            Self::Dependency => "controller dependency initialization failed",
            Self::Internal => "controller initialization failed internally",
        })
    }
}

impl std::error::Error for ControllerInitError {}

/// Build-time mismatch between a registered action and its controller value.
///
/// Applications do not construct this error. It is public because
/// [`ControllerMaterializationError::Binding`] exposes it as a typed source
/// when generated controller metadata cannot be reconciled safely.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControllerBindingError {
    expected_controller: &'static str,
}

impl ControllerBindingError {
    pub(crate) const fn type_mismatch(expected_controller: &'static str) -> Self {
        Self {
            expected_controller,
        }
    }

    /// Returns the fully qualified controller type expected by the action.
    pub const fn expected_controller(&self) -> &'static str {
        self.expected_controller
    }
}

impl std::fmt::Display for ControllerBindingError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "controller action expected instance type '{}'",
            self.expected_controller
        )
    }
}

impl std::error::Error for ControllerBindingError {}

/// Failure while turning static controller metadata into App-owned routes.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ControllerMaterializationError {
    /// The same controller type was registered more than once.
    DuplicateRegistration {
        /// Fully qualified Rust type name of the duplicated controller.
        controller: &'static str,
    },
    /// An action referenced a controller with no matching registration.
    MissingRegistration {
        /// Fully qualified Rust type name of the missing controller.
        controller: &'static str,
        /// Rust action name that required the controller.
        action: &'static str,
    },
    /// Static action metadata named a different controller than its type ID.
    MetadataMismatch {
        /// Controller type associated with the registered type ID.
        registered_controller: &'static str,
        /// Controller type name embedded in the action metadata.
        action_controller: &'static str,
        /// Rust action name whose metadata was inconsistent.
        action: &'static str,
    },
    /// The application-scoped controller constructor failed.
    Initialization {
        /// Fully qualified Rust type name of the controller.
        controller: &'static str,
        /// Secret-safe initialization category returned by the controller.
        source: ControllerInitError,
    },
    /// A generated action could not bind to its controller instance.
    Binding {
        /// Fully qualified Rust type name of the controller.
        controller: &'static str,
        /// Rust action name that could not be bound.
        action: &'static str,
        /// Generated binding failure.
        source: ControllerBindingError,
    },
    /// Generated controller/action binding panicked during application build.
    BindingPanicked {
        /// Fully qualified Rust type name of the controller.
        controller: &'static str,
        /// Rust action name whose binding panicked.
        action: &'static str,
    },
}

impl std::fmt::Display for ControllerMaterializationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DuplicateRegistration { controller } => {
                write!(formatter, "controller '{controller}' is registered more than once")
            }
            Self::MissingRegistration { controller, action } => write!(
                formatter,
                "controller '{controller}' required by action '{action}' is not registered"
            ),
            Self::MetadataMismatch {
                registered_controller,
                action_controller,
                action,
            } => write!(
                formatter,
                "action '{action}' names controller '{action_controller}' but its TypeId belongs to '{registered_controller}'"
            ),
            Self::Initialization { controller, source } => {
                write!(formatter, "controller '{controller}' initialization failed: {source}")
            }
            Self::Binding {
                controller,
                action,
                source,
            } => write!(
                formatter,
                "controller '{controller}' action '{action}' binding failed: {source}"
            ),
            Self::BindingPanicked { controller, action } => write!(
                formatter,
                "controller '{controller}' action '{action}' binding panicked"
            ),
        }
    }
}

impl std::error::Error for ControllerMaterializationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Initialization { source, .. } => Some(source),
            Self::Binding { source, .. } => Some(source),
            _ => None,
        }
    }
}
