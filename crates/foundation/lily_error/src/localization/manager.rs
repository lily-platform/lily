use std::collections::HashMap;
use std::fmt;
use std::path::Path;
use std::sync::Arc;

use super::LocalizationError;

type TranslationMap = HashMap<String, String>;
type LanguageMap = HashMap<String, TranslationMap>;

/// Default language used when the requested language is unavailable or does
/// not define the requested key.
const DEFAULT_LANGUAGE: &str = "en";

/// Immutable, thread-safe in-memory localization cache.
///
/// Cloning a manager is cheap because all translations are stored behind an
/// [`Arc`]. Once [`load`](Self::load) completes, lookups do not perform I/O,
/// acquire locks, or mutate shared state.
///
/// # Locale format
///
/// Each `*.json` filename is treated as a language identifier:
///
/// ```text
/// locales/en.json
/// locales/tr.json
/// ```
///
/// Each file must contain a flat JSON object whose keys and values are strings:
///
/// ```json
/// {
///   "Auth.EmailOrPasswordWrong": "E-mail or password is incorrect."
/// }
/// ```
#[derive(Clone)]
pub struct LocalizationManager {
    translations: Arc<LanguageMap>,
}

impl fmt::Debug for LocalizationManager {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalizationCatalog")
            .field("language_count", &self.translations.len())
            .field("translations", &"<redacted>")
            .finish()
    }
}

impl LocalizationManager {
    /// Loads every JSON locale file in `path` into memory.
    ///
    /// Non-JSON files and subdirectories are ignored. Loading fails when the
    /// directory does not exist, contains no JSON locale files, or any locale
    /// file violates the expected format.
    pub async fn load(path: impl AsRef<Path>) -> Result<Self, LocalizationError> {
        Self::load_path(path.as_ref()).await
    }

    /// Builds an immutable catalog without filesystem I/O.
    ///
    /// Language identifiers are normalized to ASCII lowercase and must use
    /// only letters, numbers, `-`, or `_`. The input must contain at least one
    /// language and translation keys cannot be blank. The resulting catalog
    /// has no mutation or reload API; replace the owning application to use a
    /// different snapshot.
    pub fn from_translations(
        translations: HashMap<String, HashMap<String, String>>,
    ) -> Result<Self, LocalizationError> {
        if translations.is_empty() {
            return Err(LocalizationError::InvalidCatalog {
                reason: "catalog must contain at least one language".to_string(),
            });
        }

        let mut normalized = HashMap::with_capacity(translations.len());
        for (language, messages) in translations {
            let language = normalize_language_identifier(&language).ok_or_else(|| {
                LocalizationError::InvalidCatalog {
                    reason:
                        "language identifiers may contain only ASCII letters, numbers, '-' and '_'"
                            .to_string(),
                }
            })?;
            if messages.keys().any(|key| key.trim().is_empty()) {
                return Err(LocalizationError::InvalidCatalog {
                    reason: "translation keys cannot be empty".to_string(),
                });
            }
            if normalized.insert(language, messages).is_some() {
                return Err(LocalizationError::InvalidCatalog {
                    reason: "catalog contains duplicate normalized language identifiers"
                        .to_string(),
                });
            }
        }

        Ok(Self {
            translations: Arc::new(normalized),
        })
    }

    async fn load_path(path: &Path) -> Result<Self, LocalizationError> {
        let metadata = tokio::fs::metadata(path)
            .await
            .map_err(|source| match source.kind() {
                std::io::ErrorKind::NotFound => LocalizationError::DirectoryNotFound {
                    path: path.to_path_buf(),
                },
                _ => LocalizationError::DirectoryReadError {
                    path: path.to_path_buf(),
                    source,
                },
            })?;

        if !metadata.is_dir() {
            return Err(LocalizationError::DirectoryNotFound {
                path: path.to_path_buf(),
            });
        }

        let mut directory = tokio::fs::read_dir(path).await.map_err(|source| {
            LocalizationError::DirectoryReadError {
                path: path.to_path_buf(),
                source,
            }
        })?;
        let mut translations = HashMap::new();

        while let Some(entry) = directory.next_entry().await.map_err(|source| {
            LocalizationError::DirectoryReadError {
                path: path.to_path_buf(),
                source,
            }
        })? {
            let file_path = entry.path();
            let file_type =
                entry
                    .file_type()
                    .await
                    .map_err(|source| LocalizationError::FileReadError {
                        path: file_path.clone(),
                        source,
                    })?;

            if !file_type.is_file() || !has_json_extension(&file_path) {
                continue;
            }

            let language = language_from_path(&file_path)?;
            if translations.contains_key(&language) {
                return Err(LocalizationError::InvalidLanguageFile {
                    path: file_path,
                    reason: format!("duplicate language identifier '{language}'"),
                });
            }

            let contents = tokio::fs::read_to_string(&file_path)
                .await
                .map_err(|source| LocalizationError::FileReadError {
                    path: file_path.clone(),
                    source,
                })?;
            let messages: TranslationMap = serde_json::from_str(&contents).map_err(|source| {
                LocalizationError::JsonParseError {
                    path: file_path.clone(),
                    source,
                }
            })?;

            if messages.keys().any(|key| key.trim().is_empty()) {
                return Err(LocalizationError::InvalidLanguageFile {
                    path: file_path,
                    reason: "translation keys cannot be empty".to_string(),
                });
            }

            translations.insert(language, messages);
        }

        if translations.is_empty() {
            return Err(LocalizationError::InvalidLanguageFile {
                path: path.to_path_buf(),
                reason: "directory does not contain any JSON locale files".to_string(),
            });
        }

        Ok(Self {
            translations: Arc::new(translations),
        })
    }

    /// Returns the localized value for `key`.
    ///
    /// The requested language is checked first. When either the language or
    /// key is unavailable, English (`en`) is checked as the fallback language.
    /// No allocation occurs during this lookup.
    #[inline]
    pub fn translate<'a>(&'a self, language: &str, key: &str) -> Option<&'a str> {
        let mut candidate = language.trim();
        while !candidate.is_empty() {
            if let Some(message) = self
                .messages_for_language(candidate)
                .and_then(|messages| messages.get(key))
            {
                return Some(message.as_str());
            }
            let Some(separator) = candidate.rfind(['-', '_']) else {
                break;
            };
            candidate = &candidate[..separator];
        }

        self.messages_for_language(DEFAULT_LANGUAGE)
            .and_then(|messages| messages.get(key))
            .map(String::as_str)
    }

    /// Returns the localized value or the key itself when no translation is
    /// available in either the requested language or English.
    #[inline]
    pub fn translate_or_key(&self, language: &str, key: &str) -> String {
        self.translate(language, key).unwrap_or(key).to_owned()
    }

    /// Returns the number of languages loaded into the cache.
    #[inline]
    pub fn language_count(&self) -> usize {
        self.translations.len()
    }

    /// Returns whether a language was loaded from disk.
    #[inline]
    pub fn contains_language(&self, language: &str) -> bool {
        self.messages_for_language(language).is_some()
    }

    fn messages_for_language(&self, language: &str) -> Option<&TranslationMap> {
        self.translations.get(language).or_else(|| {
            self.translations
                .iter()
                .find(|(candidate, _)| candidate.eq_ignore_ascii_case(language))
                .map(|(_, messages)| messages)
        })
    }
}

fn has_json_extension(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| extension.eq_ignore_ascii_case("json"))
}

fn language_from_path(path: &Path) -> Result<String, LocalizationError> {
    let language = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .filter(|stem| !stem.is_empty())
        .ok_or_else(|| LocalizationError::InvalidLanguageFile {
            path: path.to_path_buf(),
            reason: "filename must contain a UTF-8 language identifier".to_string(),
        })?;

    let Some(language) = normalize_language_identifier(language) else {
        return Err(LocalizationError::InvalidLanguageFile {
            path: path.to_path_buf(),
            reason: format!(
                "language identifier '{language}' may contain only letters, numbers, '-' and '_'"
            ),
        });
    };

    Ok(language)
}

fn normalize_language_identifier(language: &str) -> Option<String> {
    (!language.is_empty()
        && language
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_'))
    .then(|| language.to_ascii_lowercase())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::*;

    async fn write_locale(directory: &TempDir, language: &str, json: &str) {
        tokio::fs::write(directory.path().join(format!("{language}.json")), json)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn loads_languages_and_translates_keys() {
        let directory = TempDir::new().unwrap();
        write_locale(
            &directory,
            "en",
            r#"{"Auth.Invalid":"Invalid credentials","Only.English":"English value"}"#,
        )
        .await;
        write_locale(
            &directory,
            "tr",
            r#"{"Auth.Invalid":"E-posta veya şifre hatalı."}"#,
        )
        .await;

        let manager = LocalizationManager::load(directory.path().to_str().unwrap())
            .await
            .unwrap();

        assert_eq!(manager.language_count(), 2);
        assert!(manager.contains_language("tr"));
        assert_eq!(
            manager.translate("tr", "Auth.Invalid"),
            Some("E-posta veya şifre hatalı.")
        );
        assert_eq!(
            manager.translate("tr", "Only.English"),
            Some("English value")
        );
        assert_eq!(
            manager.translate("de", "Auth.Invalid"),
            Some("Invalid credentials")
        );
        assert_eq!(manager.translate("tr", "Unknown.Key"), None);
        assert_eq!(manager.translate_or_key("tr", "Unknown.Key"), "Unknown.Key");
    }

    #[tokio::test]
    async fn reports_missing_directory() {
        let directory = TempDir::new().unwrap();
        let missing = directory.path().join("missing");

        let error = LocalizationManager::load(missing.to_str().unwrap())
            .await
            .unwrap_err();

        assert!(matches!(error, LocalizationError::DirectoryNotFound { .. }));
    }

    #[tokio::test]
    async fn rejects_invalid_json_shape() {
        let directory = TempDir::new().unwrap();
        write_locale(&directory, "en", r#"{"Auth.Invalid":42}"#).await;

        let error = LocalizationManager::load(directory.path().to_str().unwrap())
            .await
            .unwrap_err();

        assert!(matches!(error, LocalizationError::JsonParseError { .. }));
    }

    #[tokio::test]
    async fn rejects_invalid_language_filename() {
        let directory = TempDir::new().unwrap();
        tokio::fs::write(directory.path().join("bad language.json"), "{}")
            .await
            .unwrap();

        let error = LocalizationManager::load(directory.path().to_str().unwrap())
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            LocalizationError::InvalidLanguageFile { .. }
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn supports_concurrent_lock_free_reads() {
        let directory = TempDir::new().unwrap();
        write_locale(&directory, "en", r#"{"Shared.Key":"Shared value"}"#).await;
        let manager = Arc::new(
            LocalizationManager::load(directory.path().to_str().unwrap())
                .await
                .unwrap(),
        );

        let mut tasks = Vec::new();
        for _ in 0..64 {
            let manager = manager.clone();
            tasks.push(tokio::spawn(async move {
                for _ in 0..1_000 {
                    assert_eq!(manager.translate("en", "Shared.Key"), Some("Shared value"));
                }
            }));
        }

        for task in tasks {
            task.await.unwrap();
        }
    }

    #[test]
    fn in_memory_catalog_supports_unicode_and_language_subtag_fallback() {
        let manager = LocalizationManager::from_translations(HashMap::from([
            (
                "EN".to_string(),
                HashMap::from([
                    ("Greeting".to_string(), "Hello 🌍".to_string()),
                    ("EnglishOnly".to_string(), "English fallback".to_string()),
                ]),
            ),
            (
                "tr".to_string(),
                HashMap::from([("Greeting".to_string(), "Merhaba dünya".to_string())]),
            ),
        ]))
        .unwrap();

        assert_eq!(
            manager.translate("tr-TR", "Greeting"),
            Some("Merhaba dünya")
        );
        assert_eq!(
            manager.translate("TR-tr", "EnglishOnly"),
            Some("English fallback")
        );
        assert_eq!(manager.translate("ja-JP", "Greeting"), Some("Hello 🌍"));
        assert!(manager.contains_language("EN"));
    }

    #[test]
    fn catalog_debug_never_exposes_translation_keys_or_values() {
        let manager = LocalizationManager::from_translations(HashMap::from([(
            "en".to_string(),
            HashMap::from([(
                "Secret.DatabasePassword".to_string(),
                "password=hunter2".to_string(),
            )]),
        )]))
        .unwrap();

        let debug = format!("{manager:?}");
        assert!(debug.contains("language_count"));
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("DatabasePassword"));
        assert!(!debug.contains("hunter2"));
    }
}
