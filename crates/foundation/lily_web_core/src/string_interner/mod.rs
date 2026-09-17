//! Opaque strings retained by parsed request data.

/// A request string that is either borrowed from a framework-owned static or
/// owned by the request.
///
/// This value type never transforms its input. Call [`Self::as_str`] to inspect
/// the retained text. The parser that creates a value can still apply the
/// governing protocol grammar first: for example, [`crate::QueryParams`]
/// form-url-decodes query names and values exactly once before storing them,
/// while route parameters and header values are retained without that decoding.
#[derive(Debug, Clone)]
pub enum InternedString {
    /// A framework-owned static value that needs no per-request allocation.
    Static(&'static str),
    /// A value owned by the request.
    Owned(String),
}

impl InternedString {
    /// Returns the retained string value.
    #[inline(always)]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Static(value) => value,
            Self::Owned(value) => value.as_str(),
        }
    }
}

impl PartialEq for InternedString {
    #[inline(always)]
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl Eq for InternedString {}

impl std::hash::Hash for InternedString {
    #[inline(always)]
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(self.as_str(), state);
    }
}

impl AsRef<str> for InternedString {
    #[inline(always)]
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl std::borrow::Borrow<str> for InternedString {
    #[inline(always)]
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl std::ops::Deref for InternedString {
    type Target = str;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl std::fmt::Display for InternedString {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl PartialEq<str> for InternedString {
    #[inline(always)]
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<&str> for InternedString {
    #[inline(always)]
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl PartialEq<String> for InternedString {
    #[inline(always)]
    fn eq(&self, other: &String) -> bool {
        self.as_str() == other.as_str()
    }
}

/// Preserves an opaque user-controlled value byte-for-byte.
#[inline(always)]
pub(crate) fn preserve_opaque(value: &str) -> InternedString {
    InternedString::Owned(value.to_owned())
}

/// Preserves an already-owned opaque value without copying it again.
#[inline(always)]
pub(crate) fn preserve_opaque_owned(value: String) -> InternedString {
    InternedString::Owned(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opaque_values_preserve_case_and_support_standard_string_methods() {
        let value = preserve_opaque("Content-Type-AuThZ");

        assert_eq!(value.as_str(), "Content-Type-AuThZ");
        assert!(value.starts_with("Content"));
        assert_eq!(value.to_lowercase(), "content-type-authz");
    }

    #[test]
    fn owned_and_static_representations_compare_and_hash_by_value() {
        use std::collections::HashSet;

        let owned = InternedString::Owned("GET".to_owned());
        let static_value = InternedString::Static("GET");
        let mut values = HashSet::new();
        values.insert(owned);

        assert!(values.contains(&static_value));
        assert_eq!(static_value, "GET");
    }
}
