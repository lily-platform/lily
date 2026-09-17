use std::net::IpAddr;

/// Transport-authenticated network identity for one HTTP or WebSocket request.
///
/// These values are supplied by a Lily transport adapter, never copied from
/// application-visible forwarding headers. A trusted proxy policy may replace
/// `client_ip` only after validating the socket peer and the complete forwarded
/// chain.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RequestConnectionInfo {
    peer_ip: Option<IpAddr>,
    client_ip: Option<IpAddr>,
    trusted_proxy: bool,
}

impl RequestConnectionInfo {
    /// Creates metadata for a request received directly from `peer_ip`.
    pub fn direct(peer_ip: IpAddr) -> Self {
        Self {
            peer_ip: Some(peer_ip),
            client_ip: Some(peer_ip),
            trusted_proxy: false,
        }
    }

    /// Creates connection metadata after a transport adapter has validated a
    /// trusted proxy chain.
    #[doc(hidden)]
    pub fn from_trusted_transport(
        peer_ip: Option<IpAddr>,
        client_ip: Option<IpAddr>,
        trusted_proxy: bool,
    ) -> Self {
        Self {
            peer_ip,
            client_ip,
            trusted_proxy,
        }
    }

    /// Returns the socket peer authenticated by the transport.
    pub fn peer_ip(self) -> Option<IpAddr> {
        self.peer_ip
    }

    /// Returns the effective client address after trusted-proxy processing.
    pub fn client_ip(self) -> Option<IpAddr> {
        self.client_ip
    }

    /// Reports whether a trusted proxy supplied the effective client address.
    pub fn via_trusted_proxy(self) -> bool {
        self.trusted_proxy
    }
}
