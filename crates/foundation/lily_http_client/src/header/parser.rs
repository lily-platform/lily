use crate::error::{HttpClientError, Result};
use crate::header::HeaderMap;

/// HTTP header parser for parsing raw HTTP headers
#[derive(Debug, Clone)]
pub struct HeaderParser;

impl HeaderParser {
    /// Parse raw HTTP headers from a string
    ///
    /// Expected format:
    /// ```text
    /// Header-Name: Header-Value\r\n
    /// Another-Header: Another-Value\r\n
    /// \r\n
    /// ```
    pub fn parse(raw_headers: &str) -> Result<HeaderMap> {
        let mut headers = HeaderMap::new();

        for line in raw_headers.lines() {
            let line = line.trim();

            // Skip empty lines
            if line.is_empty() {
                continue;
            }

            // Find the colon separator
            if let Some(colon_pos) = line.find(':') {
                let name = line[..colon_pos].trim();
                let value = line[colon_pos + 1..].trim();

                if name.is_empty() {
                    return Err(HttpClientError::http_parsing(
                        "Header name cannot be empty".to_string(),
                    ));
                }

                headers.append(name, value)?;
            } else {
                return Err(HttpClientError::http_parsing(format!(
                    "Invalid header line: '{line}'"
                )));
            }
        }

        Ok(headers)
    }

    /// Parse headers from raw HTTP response bytes
    ///
    /// This method extracts the header section from a complete HTTP response
    pub fn parse_from_response(response_bytes: &[u8]) -> Result<(HeaderMap, usize)> {
        let response_str = String::from_utf8_lossy(response_bytes);

        // Find the end of headers (double CRLF)
        let header_end = response_str
            .find("\r\n\r\n")
            .or_else(|| response_str.find("\n\n"))
            .ok_or_else(|| {
                HttpClientError::http_parsing(
                    "Could not find end of headers in response".to_string(),
                )
            })?;

        let header_section = &response_str[..header_end];

        // Skip the status line (first line)
        let lines: Vec<&str> = header_section.lines().collect();
        if lines.is_empty() {
            return Err(HttpClientError::http_parsing(
                "No status line found in response".to_string(),
            ));
        }

        // Parse headers starting from the second line
        let header_lines = if lines.len() > 1 {
            lines[1..].join("\n")
        } else {
            String::new()
        };

        let headers = Self::parse(&header_lines)?;

        // Calculate the actual byte position after headers
        let header_end_bytes = if response_str.contains("\r\n\r\n") {
            header_end + 4 // \r\n\r\n
        } else {
            header_end + 2 // \n\n
        };

        Ok((headers, header_end_bytes))
    }

    /// Parse a single header line
    pub fn parse_header_line(line: &str) -> Result<(String, String)> {
        let line = line.trim();

        if let Some(colon_pos) = line.find(':') {
            let name = line[..colon_pos].trim().to_string();
            let value = line[colon_pos + 1..].trim().to_string();

            if name.is_empty() {
                return Err(HttpClientError::http_parsing(
                    "Header name cannot be empty".to_string(),
                ));
            }

            Ok((name, value))
        } else {
            Err(HttpClientError::http_parsing(format!(
                "Invalid header line: '{line}'"
            )))
        }
    }

    /// Format headers for HTTP transmission
    pub fn format_headers(headers: &HeaderMap) -> String {
        let mut result = String::new();

        for (name, values) in headers.iter() {
            for value in values {
                result.push_str(&format!("{name}: {value}\r\n"));
            }
        }

        result
    }

    /// Parse Content-Length header value
    pub fn parse_content_length(headers: &HeaderMap) -> Result<Option<usize>> {
        if let Some(content_length_str) = headers.get("content-length") {
            content_length_str.parse::<usize>().map(Some).map_err(|_| {
                HttpClientError::http_parsing(format!(
                    "Invalid Content-Length value: '{content_length_str}'"
                ))
            })
        } else {
            Ok(None)
        }
    }

    /// Check if Transfer-Encoding is chunked
    pub fn is_chunked_encoding(headers: &HeaderMap) -> bool {
        headers
            .get("transfer-encoding")
            .map(|value| value.to_lowercase().contains("chunked"))
            .unwrap_or(false)
    }

    /// Parse Connection header to check if keep-alive
    pub fn is_keep_alive(headers: &HeaderMap) -> bool {
        headers
            .get("connection")
            .map(|value| value.to_lowercase() == "keep-alive")
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_headers() {
        let raw = "Content-Type: application/json\nContent-Length: 123\nHost: example.com";
        let headers = HeaderParser::parse(raw).unwrap();

        assert_eq!(headers.get("content-type"), Some("application/json"));
        assert_eq!(headers.get("content-length"), Some("123"));
        assert_eq!(headers.get("host"), Some("example.com"));
    }

    #[test]
    fn test_parse_headers_with_spaces() {
        let raw = "Content-Type:   application/json   \n  Host  :  example.com  ";
        let headers = HeaderParser::parse(raw).unwrap();

        assert_eq!(headers.get("content-type"), Some("application/json"));
        assert_eq!(headers.get("host"), Some("example.com"));
    }

    #[test]
    fn test_parse_duplicate_headers() {
        let raw = "Accept: text/html\nAccept: application/json";
        let headers = HeaderParser::parse(raw).unwrap();

        let accept_values = headers.get_all("accept").unwrap();
        assert_eq!(accept_values.len(), 2);
        assert!(accept_values.contains(&"text/html".to_string()));
        assert!(accept_values.contains(&"application/json".to_string()));
    }

    #[test]
    fn test_parse_invalid_headers() {
        let raw = "Invalid header without colon";
        assert!(HeaderParser::parse(raw).is_err());

        let raw = ": value without name";
        assert!(HeaderParser::parse(raw).is_err());
    }

    #[test]
    fn test_parse_from_response() {
        let response = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 123\r\n\r\n{\"test\": true}";
        let (headers, body_start) = HeaderParser::parse_from_response(response.as_bytes()).unwrap();

        assert_eq!(headers.get("content-type"), Some("application/json"));
        assert_eq!(headers.get("content-length"), Some("123"));
        assert_eq!(body_start, response.len() - 14); // Length of body: {"test": true} = 14 chars
    }

    #[test]
    fn test_format_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("Content-Type", "application/json").unwrap();
        headers.insert("Host", "example.com").unwrap();

        let formatted = HeaderParser::format_headers(&headers);
        assert!(formatted.contains("content-type: application/json\r\n"));
        assert!(formatted.contains("host: example.com\r\n"));
    }

    #[test]
    fn test_parse_content_length() {
        let mut headers = HeaderMap::new();
        headers.insert("Content-Length", "1234").unwrap();

        let length = HeaderParser::parse_content_length(&headers).unwrap();
        assert_eq!(length, Some(1234));

        // Test missing header
        let empty_headers = HeaderMap::new();
        let length = HeaderParser::parse_content_length(&empty_headers).unwrap();
        assert_eq!(length, None);

        // Test invalid value
        let mut invalid_headers = HeaderMap::new();
        invalid_headers.insert("Content-Length", "invalid").unwrap();
        assert!(HeaderParser::parse_content_length(&invalid_headers).is_err());
    }

    #[test]
    fn test_is_chunked_encoding() {
        let mut headers = HeaderMap::new();
        headers.insert("Transfer-Encoding", "chunked").unwrap();
        assert!(HeaderParser::is_chunked_encoding(&headers));

        headers
            .insert("Transfer-Encoding", "gzip, chunked")
            .unwrap();
        assert!(HeaderParser::is_chunked_encoding(&headers));

        headers.insert("Transfer-Encoding", "gzip").unwrap();
        assert!(!HeaderParser::is_chunked_encoding(&headers));
    }

    #[test]
    fn test_is_keep_alive() {
        let mut headers = HeaderMap::new();
        headers.insert("Connection", "keep-alive").unwrap();
        assert!(HeaderParser::is_keep_alive(&headers));

        headers.insert("Connection", "close").unwrap();
        assert!(!HeaderParser::is_keep_alive(&headers));

        let empty_headers = HeaderMap::new();
        assert!(!HeaderParser::is_keep_alive(&empty_headers));
    }
}
