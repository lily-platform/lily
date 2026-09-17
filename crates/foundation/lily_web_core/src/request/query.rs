use std::collections::HashMap;
use std::fmt;

use crate::string_interner::InternedString;

/// Identifies which side of a query pair failed to decode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryComponent {
    /// The field name failed to decode.
    Key,
    /// The field value failed to decode.
    Value,
}

impl fmt::Display for QueryComponent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Key => "key",
            Self::Value => "value",
        })
    }
}

/// Stable categories returned by the query decoder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryParseErrorKind {
    /// A `%` escape was not followed by exactly two hexadecimal digits.
    InvalidPercentEncoding,
    /// The decoded octets were not valid UTF-8.
    InvalidUtf8,
}

/// A query decoding failure that is safe to expose in a client error.
///
/// The original key or value is deliberately not retained because query
/// strings commonly contain credentials and other sensitive values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryParseError {
    pair_index: usize,
    component: QueryComponent,
    byte_offset: usize,
    kind: QueryParseErrorKind,
}

impl QueryParseError {
    /// Returns the zero-based pair index containing the failure.
    pub fn pair_index(&self) -> usize {
        self.pair_index
    }

    /// Returns the side of the pair that failed.
    pub fn component(&self) -> QueryComponent {
        self.component
    }

    /// Returns the encoded byte offset for percent errors and the decoded byte
    /// offset for UTF-8 errors.
    pub fn byte_offset(&self) -> usize {
        self.byte_offset
    }

    /// Returns the stable failure category.
    pub fn kind(&self) -> QueryParseErrorKind {
        self.kind
    }
}

impl fmt::Display for QueryParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let reason = match self.kind {
            QueryParseErrorKind::InvalidPercentEncoding => "invalid percent escape",
            QueryParseErrorKind::InvalidUtf8 => "decoded value is not valid UTF-8",
        };
        write!(
            formatter,
            "query pair {} {} at byte {}: {}",
            self.pair_index, self.component, self.byte_offset, reason
        )
    }
}

impl std::error::Error for QueryParseError {}

/// A percent-decoded, insertion-ordered query multimap.
///
/// Pairs and repeated values remain in wire order. [`QueryParams::get`] uses
/// an explicit first-value-wins compatibility policy; callers that need every
/// occurrence should use [`QueryParams::get_all`] or iterate over the map.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueryParams {
    pairs: Vec<(InternedString, InternedString)>,
    indices: HashMap<String, Vec<usize>>,
}

impl QueryParams {
    /// Parses an `application/x-www-form-urlencoded` style URI query.
    ///
    /// `+` decodes to a space, percent escapes are validated before decoding,
    /// a missing `=` means an empty value, and empty `&` segments are ignored.
    pub fn parse(encoded: &str) -> Result<Self, QueryParseError> {
        let mut result = Self::default();

        if encoded.is_empty() {
            return Ok(result);
        }

        for (pair_index, pair) in encoded.split('&').enumerate() {
            if pair.is_empty() {
                continue;
            }

            let (encoded_key, encoded_value) = pair.split_once('=').unwrap_or((pair, ""));
            let key = decode_component(encoded_key, pair_index, QueryComponent::Key)?;
            let value = decode_component(encoded_value, pair_index, QueryComponent::Value)?;
            result.push(key, value);
        }

        Ok(result)
    }

    fn push(&mut self, key: String, value: String) {
        let index = self.pairs.len();
        self.indices.entry(key.clone()).or_default().push(index);
        self.pairs
            .push((InternedString::Owned(key), InternedString::Owned(value)));
    }

    /// Returns the first value for `name` in wire order.
    pub fn get<Q>(&self, name: &Q) -> Option<&InternedString>
    where
        Q: AsRef<str> + ?Sized,
    {
        let index = self.indices.get(name.as_ref())?.first()?;
        self.pairs.get(*index).map(|(_, value)| value)
    }

    /// Returns every value for `name` in wire order.
    pub fn get_all<Q>(&self, name: &Q) -> QueryValues<'_>
    where
        Q: AsRef<str> + ?Sized,
    {
        QueryValues {
            params: self,
            indices: self
                .indices
                .get(name.as_ref())
                .map(Vec::as_slice)
                .unwrap_or_default(),
            position: 0,
        }
    }

    /// Iterates over every decoded pair in wire order, including duplicates.
    pub fn iter(&self) -> QueryIter<'_> {
        QueryIter(self.pairs.iter())
    }

    /// Number of decoded pairs, including repeated keys.
    pub fn len(&self) -> usize {
        self.pairs.len()
    }

    /// Returns `true` when the query contains no pairs.
    pub fn is_empty(&self) -> bool {
        self.pairs.is_empty()
    }

    /// Number of distinct, case-sensitive decoded keys.
    pub fn key_count(&self) -> usize {
        self.indices.len()
    }
}

/// Iterator over all query pairs in wire order.
pub struct QueryIter<'a>(std::slice::Iter<'a, (InternedString, InternedString)>);

impl<'a> Iterator for QueryIter<'a> {
    type Item = (&'a InternedString, &'a InternedString);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(key, value)| (key, value))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl DoubleEndedIterator for QueryIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0.next_back().map(|(key, value)| (key, value))
    }
}

impl ExactSizeIterator for QueryIter<'_> {}
impl std::iter::FusedIterator for QueryIter<'_> {}

impl<'a> IntoIterator for &'a QueryParams {
    type Item = (&'a InternedString, &'a InternedString);
    type IntoIter = QueryIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Iterator over the repeated values for one query key.
pub struct QueryValues<'a> {
    params: &'a QueryParams,
    indices: &'a [usize],
    position: usize,
}

impl<'a> Iterator for QueryValues<'a> {
    type Item = &'a InternedString;

    fn next(&mut self) -> Option<Self::Item> {
        let index = *self.indices.get(self.position)?;
        self.position += 1;
        self.params.pairs.get(index).map(|(_, value)| value)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.indices.len().saturating_sub(self.position);
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for QueryValues<'_> {}
impl std::iter::FusedIterator for QueryValues<'_> {}

fn decode_component(
    encoded: &str,
    pair_index: usize,
    component: QueryComponent,
) -> Result<String, QueryParseError> {
    let input = encoded.as_bytes();
    let mut decoded = Vec::with_capacity(input.len());
    let mut position = 0;

    while position < input.len() {
        match input[position] {
            b'+' => {
                decoded.push(b' ');
                position += 1;
            }
            b'%' => {
                let Some(high) = input.get(position + 1).copied().and_then(hex_value) else {
                    return Err(QueryParseError {
                        pair_index,
                        component,
                        byte_offset: position,
                        kind: QueryParseErrorKind::InvalidPercentEncoding,
                    });
                };
                let Some(low) = input.get(position + 2).copied().and_then(hex_value) else {
                    return Err(QueryParseError {
                        pair_index,
                        component,
                        byte_offset: position,
                        kind: QueryParseErrorKind::InvalidPercentEncoding,
                    });
                };
                decoded.push((high << 4) | low);
                position += 3;
            }
            byte => {
                decoded.push(byte);
                position += 1;
            }
        }
    }

    String::from_utf8(decoded).map_err(|error| QueryParseError {
        pair_index,
        component,
        byte_offset: error.utf8_error().valid_up_to(),
        kind: QueryParseErrorKind::InvalidUtf8,
    })
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{QueryComponent, QueryParams, QueryParseErrorKind};

    #[test]
    fn decodes_percent_escapes_plus_and_unicode() {
        let query = QueryParams::parse("q=hello+world&path=%2Fdocs%2Fv1&city=%C4%B0zmir")
            .expect("valid query");

        assert_eq!(
            query.get("q").map(|value| value.as_str()),
            Some("hello world")
        );
        assert_eq!(
            query.get("path").map(|value| value.as_str()),
            Some("/docs/v1")
        );
        assert_eq!(query.get("city").map(|value| value.as_str()), Some("İzmir"));
    }

    #[test]
    fn preserves_pair_and_repeated_value_order() {
        let query = QueryParams::parse("tag=first&other=1&tag=second&tag=").expect("valid query");

        assert_eq!(query.len(), 4);
        assert_eq!(query.key_count(), 2);
        assert_eq!(query.get("tag").map(|value| value.as_str()), Some("first"));
        assert_eq!(
            query
                .get_all("tag")
                .map(|value| value.as_str())
                .collect::<Vec<_>>(),
            vec!["first", "second", ""]
        );
        assert_eq!(
            query
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str()))
                .collect::<Vec<_>>(),
            vec![
                ("tag", "first"),
                ("other", "1"),
                ("tag", "second"),
                ("tag", ""),
            ]
        );
    }

    #[test]
    fn keys_are_decoded_and_case_sensitive() {
        let query =
            QueryParams::parse("user%20name=Ada&Name=upper&name=lower").expect("valid query");

        assert_eq!(
            query.get("user name").map(|value| value.as_str()),
            Some("Ada")
        );
        assert_eq!(query.get("Name").map(|value| value.as_str()), Some("upper"));
        assert_eq!(query.get("name").map(|value| value.as_str()), Some("lower"));
    }

    #[test]
    fn opaque_query_values_preserve_case_and_unicode() {
        let query =
            QueryParams::parse("token=Content-Type-AuThZ&upper=AUTHORIZATION&city=%C4%B0zMir")
                .expect("valid query");

        assert_eq!(
            query.get("token").map(|value| value.as_str()),
            Some("Content-Type-AuThZ")
        );
        assert_eq!(
            query.get("upper").map(|value| value.as_str()),
            Some("AUTHORIZATION")
        );
        assert_eq!(query.get("city").map(|value| value.as_str()), Some("İzMir"));
    }

    #[test]
    fn missing_equals_is_empty_and_empty_segments_are_ignored() {
        let query = QueryParams::parse("flag&&empty=&").expect("valid query");

        assert_eq!(query.len(), 2);
        assert_eq!(query.get("flag").map(|value| value.as_str()), Some(""));
        assert_eq!(query.get("empty").map(|value| value.as_str()), Some(""));
    }

    #[test]
    fn rejects_malformed_percent_encoding_without_echoing_input() {
        for encoded in ["key=%", "key=%2", "key=%2G"] {
            let error = QueryParams::parse(encoded).expect_err("invalid percent escape");
            assert_eq!(error.pair_index(), 0);
            assert_eq!(error.component(), QueryComponent::Value);
            assert_eq!(error.kind(), QueryParseErrorKind::InvalidPercentEncoding);
            assert_eq!(error.byte_offset(), 0);
            assert!(!error.to_string().contains(encoded));
        }
    }

    #[test]
    fn rejects_decoded_non_utf8() {
        let error = QueryParams::parse("safe=1&name=%FF").expect_err("invalid UTF-8");

        assert_eq!(error.pair_index(), 1);
        assert_eq!(error.component(), QueryComponent::Value);
        assert_eq!(error.kind(), QueryParseErrorKind::InvalidUtf8);
        assert_eq!(error.byte_offset(), 0);
    }
}
