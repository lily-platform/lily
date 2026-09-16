/// HTTP Protocol version selection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpProtocol {
    /// Accept only HTTP/1.1 connections.
    Http1_1,
    /// Accept HTTP/2 prior knowledge on plaintext listeners and require `h2`
    /// ALPN on Lily-managed TLS listeners.
    Http2,
    /// Detect HTTP/1.1 or the HTTP/2 connection preface on plaintext listeners.
    /// Lily-managed TLS listeners advertise `h2` and `http/1.1` through ALPN.
    Auto,
}
