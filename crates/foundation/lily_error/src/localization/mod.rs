//! Startup-loaded, in-memory localization support for error keys.
//!
//! Locale files are loaded once by [`LocalizationManager::load`]. Translation
//! lookups never access the filesystem and require no synchronization lock.
//!
//! # Example
//!
//! ```no_run
//! use lily_error::LocalizationManager;
//!
//! # async fn example() -> Result<(), lily_error::LocalizationError> {
//! let localization = LocalizationManager::load("locales").await?;
//!
//! assert_eq!(
//!     localization.translate_or_key("tr", "Auth.EmailOrPasswordWrong"),
//!     "E-posta veya şifre hatalı."
//! );
//! # Ok(())
//! # }
//! ```

mod error;
mod manager;

pub use error::LocalizationError;
pub use manager::LocalizationManager;

/// Canonical name for an immutable, application-owned localization snapshot.
///
/// Configure this snapshot through the owning HTTP application. Catalogs are
/// immutable after construction and are never installed process-globally.
pub type LocalizationCatalog = LocalizationManager;
