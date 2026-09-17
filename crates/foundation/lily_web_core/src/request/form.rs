//! Ordered `application/x-www-form-urlencoded` data and shared wire codec.

use std::{collections::HashMap, fmt};

use lily_error::application::http_api::request::{FormComponent, FormError};
use percent_encoding::percent_decode;

/// A decoded, insertion-ordered URL-encoded form multimap.
///
/// Repeated names and their original wire order are preserved. [`Self::get`]
/// uses an explicit first-value-wins convenience policy; callers that need
/// every occurrence should use [`Self::get_all`] or [`Self::iter`].
#[derive(Clone, Default, PartialEq, Eq)]
pub struct FormData {
    pairs: Vec<(String, String)>,
    indices: HashMap<String, Vec<usize>>,
}

impl fmt::Debug for FormData {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FormData")
            .field("field_count", &self.pairs.len())
            .field("name_count", &self.indices.len())
            .finish()
    }
}

impl FormData {
    #[must_use]
    /// Creates an empty ordered form.
    pub fn new() -> Self {
        Self::default()
    }

    /// Builds ordered form data from key-value pairs without discarding
    /// repeated names.
    pub fn from_pairs<I, K, V>(pairs: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let mut result = Self::new();
        for (name, value) in pairs {
            result.push(name, value);
        }
        result
    }

    /// Strictly validates and decodes a URL-encoded form body.
    ///
    /// Empty `&` segments are ignored, a missing `=` means an empty value,
    /// `+` decodes to space, malformed percent escapes are rejected, and the
    /// decoded name and value must both be valid UTF-8. The actual wire codec
    /// is supplied by `url::form_urlencoded`.
    pub fn parse(encoded: &[u8]) -> Result<Self, FormError> {
        validate_encoded_form(encoded)?;

        let mut result = Self::new();
        for (name, value) in url::form_urlencoded::parse(encoded) {
            result.push(name.into_owned(), value.into_owned());
        }
        Ok(result)
    }

    /// Appends one decoded field while preserving insertion order.
    pub fn push(&mut self, name: impl Into<String>, value: impl Into<String>) {
        let name = name.into();
        let index = self.pairs.len();
        self.indices.entry(name.clone()).or_default().push(index);
        self.pairs.push((name, value.into()));
    }

    /// Returns the first value for `name` in insertion order.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&str> {
        let index = self.indices.get(name)?.first()?;
        self.pairs.get(*index).map(|(_, value)| value.as_str())
    }

    /// Returns every value for `name` in insertion order.
    #[must_use]
    pub fn get_all<'a>(&'a self, name: &str) -> FormValues<'a> {
        FormValues {
            form: self,
            indices: self
                .indices
                .get(name)
                .map(Vec::as_slice)
                .unwrap_or_default(),
            position: 0,
        }
    }

    /// Iterates over all fields in insertion order, including duplicates.
    pub fn iter(&self) -> FormIter<'_> {
        FormIter(self.pairs.iter())
    }

    #[must_use]
    /// Returns the number of fields, including repeated names.
    pub fn len(&self) -> usize {
        self.pairs.len()
    }

    #[must_use]
    /// Returns `true` when no fields are present.
    pub fn is_empty(&self) -> bool {
        self.pairs.is_empty()
    }

    /// Number of distinct, case-sensitive field names.
    #[must_use]
    pub fn name_count(&self) -> usize {
        self.indices.len()
    }

    /// Removes every occurrence of `name` and returns the number removed.
    pub fn remove_all(&mut self, name: &str) -> usize {
        let before = self.pairs.len();
        self.pairs.retain(|(candidate, _)| candidate != name);
        let removed = before - self.pairs.len();
        if removed != 0 {
            self.rebuild_indices();
        }
        removed
    }

    /// Removes every field.
    pub fn clear(&mut self) {
        self.pairs.clear();
        self.indices.clear();
    }

    /// Encodes all fields through the standards-based URL form serializer.
    #[must_use]
    pub fn to_url_encoded(&self) -> String {
        let mut serializer = url::form_urlencoded::Serializer::new(String::new());
        serializer.extend_pairs(
            self.pairs
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_str())),
        );
        serializer.finish()
    }

    fn rebuild_indices(&mut self) {
        self.indices.clear();
        for (index, (name, _)) in self.pairs.iter().enumerate() {
            self.indices.entry(name.clone()).or_default().push(index);
        }
    }
}

impl<'a> IntoIterator for &'a FormData {
    type Item = (&'a str, &'a str);
    type IntoIter = FormIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Iterator over all form fields in insertion order.
pub struct FormIter<'a>(std::slice::Iter<'a, (String, String)>);

impl<'a> Iterator for FormIter<'a> {
    type Item = (&'a str, &'a str);

    fn next(&mut self) -> Option<Self::Item> {
        self.0
            .next()
            .map(|(name, value)| (name.as_str(), value.as_str()))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl DoubleEndedIterator for FormIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0
            .next_back()
            .map(|(name, value)| (name.as_str(), value.as_str()))
    }
}

impl ExactSizeIterator for FormIter<'_> {}
impl std::iter::FusedIterator for FormIter<'_> {}

/// Iterator over repeated values for one form field name.
pub struct FormValues<'a> {
    form: &'a FormData,
    indices: &'a [usize],
    position: usize,
}

impl<'a> Iterator for FormValues<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        let index = *self.indices.get(self.position)?;
        self.position += 1;
        self.form.pairs.get(index).map(|(_, value)| value.as_str())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.indices.len().saturating_sub(self.position);
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for FormValues<'_> {}
impl std::iter::FusedIterator for FormValues<'_> {}

pub(crate) fn is_form_content_type(value: &str) -> bool {
    let Ok(media_type) = value.parse::<mime::Mime>() else {
        return false;
    };
    if !media_type
        .essence_str()
        .eq_ignore_ascii_case("application/x-www-form-urlencoded")
    {
        return false;
    }

    let mut charset_seen = false;
    for (name, value) in media_type.params() {
        if name.as_str().eq_ignore_ascii_case("charset") {
            if charset_seen || !value.as_str().eq_ignore_ascii_case("utf-8") {
                return false;
            }
            charset_seen = true;
        }
    }
    true
}

fn validate_encoded_form(encoded: &[u8]) -> Result<(), FormError> {
    for (pair_index, pair) in encoded.split(|byte| *byte == b'&').enumerate() {
        if pair.is_empty() {
            continue;
        }
        let separator = pair.iter().position(|byte| *byte == b'=');
        let (name, value) = match separator {
            Some(index) => (&pair[..index], &pair[index + 1..]),
            None => (pair, &[][..]),
        };
        validate_component(name, pair_index, FormComponent::Name)?;
        validate_component(value, pair_index, FormComponent::Value)?;
    }
    Ok(())
}

fn validate_component(
    encoded: &[u8],
    pair_index: usize,
    component: FormComponent,
) -> Result<(), FormError> {
    let mut cursor = 0_usize;
    while cursor < encoded.len() {
        if encoded[cursor] != b'%' {
            cursor += 1;
            continue;
        }

        let valid_triplet = encoded
            .get(cursor + 1..=cursor + 2)
            .is_some_and(|digits| digits.iter().all(u8::is_ascii_hexdigit));
        if !valid_triplet {
            return Err(FormError::InvalidPercentEncoding {
                pair_index,
                component,
                byte_offset: cursor,
            });
        }
        cursor += 3;
    }

    percent_decode(encoded)
        .decode_utf8()
        .map_err(|error| FormError::InvalidUtf8 {
            pair_index,
            component,
            byte_offset: error.valid_up_to(),
        })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{is_form_content_type, FormData};
    use lily_error::application::http_api::request::{FormComponent, FormError};

    #[test]
    fn codec_roundtrips_unicode_plus_space_duplicates_and_order() {
        let original = FormData::from_pairs([
            ("city", "İzmir"),
            ("tag", "first value"),
            ("tag", "+second"),
            ("empty", ""),
        ]);
        let encoded = original.to_url_encoded();
        assert_eq!(
            encoded,
            "city=%C4%B0zmir&tag=first+value&tag=%2Bsecond&empty="
        );
        assert_eq!(FormData::parse(encoded.as_bytes()).unwrap(), original);
    }

    #[test]
    fn parser_uses_url_form_empty_segment_and_missing_equals_semantics() {
        assert!(FormData::parse(b"").unwrap().is_empty());
        let parsed = FormData::parse(b"flag&&name=value&").unwrap();
        assert_eq!(
            parsed.iter().collect::<Vec<_>>(),
            [("flag", ""), ("name", "value")]
        );
    }

    #[test]
    fn malformed_percent_and_utf8_are_typed_without_retaining_values() {
        for input in [
            b"safe=%".as_slice(),
            b"safe=%A".as_slice(),
            b"safe=%GG".as_slice(),
            b"safe=%0G".as_slice(),
        ] {
            assert_eq!(
                FormData::parse(input).unwrap_err(),
                FormError::InvalidPercentEncoding {
                    pair_index: 0,
                    component: FormComponent::Value,
                    byte_offset: 0,
                }
            );
        }
        assert_eq!(
            FormData::parse(b"safe=%FF").unwrap_err(),
            FormError::InvalidUtf8 {
                pair_index: 0,
                component: FormComponent::Value,
                byte_offset: 0,
            }
        );
    }

    #[test]
    fn ordered_multimap_access_and_removal_are_explicit() {
        let mut data = FormData::from_pairs([("tag", "one"), ("x", "1"), ("tag", "two")]);
        assert_eq!(data.get("tag"), Some("one"));
        assert_eq!(data.get_all("tag").collect::<Vec<_>>(), ["one", "two"]);
        assert_eq!(data.remove_all("tag"), 2);
        assert_eq!(data.iter().collect::<Vec<_>>(), [("x", "1")]);
    }

    #[test]
    fn debug_output_is_structural_and_does_not_expose_form_values() {
        let data = FormData::from_pairs([("LILY_SECRET_NAME", "LILY_SECRET_VALUE")]);
        let debug = format!("{data:?}");
        assert!(!debug.contains("LILY_SECRET"));
        assert!(debug.contains("field_count"));
    }

    #[test]
    fn content_type_requires_the_form_media_type_and_utf8_charset() {
        assert!(is_form_content_type("application/x-www-form-urlencoded"));
        assert!(is_form_content_type(
            "Application/X-Www-Form-Urlencoded; charset=UTF-8"
        ));
        assert!(!is_form_content_type("text/plain"));
        assert!(!is_form_content_type(
            "application/x-www-form-urlencoded; charset=iso-8859-1"
        ));
    }
}
