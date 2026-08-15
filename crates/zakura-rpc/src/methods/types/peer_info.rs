//! An array of [`PeerInfo`] is the output of the `getpeerinfo` RPC method.

use derive_getters::Getters;
use zakura_network::{types::MetaAddr, PeerSocketAddr};

/// Item of the `getpeerinfo` response
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, Getters)]
pub struct PeerInfo {
    /// The IP address and port of the peer
    #[getter(copy)]
    pub(crate) addr: PeerSocketAddr,

    /// The peer's user agent string.
    #[serde(default)]
    pub(crate) subver: String,

    /// The negotiated protocol version.
    #[serde(default)]
    pub(crate) version: u32,

    /// Inbound (true) or Outbound (false)
    pub(crate) inbound: bool,

    /// The round-trip ping time in seconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) pingtime: Option<f64>,

    /// The wait time on a ping response in seconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) pingwait: Option<f64>,
}

/// Response type for the `getpeerinfo` RPC method.
pub type GetPeerInfoResponse = Vec<PeerInfo>;

impl PeerInfo {
    /// Creates peer information without optional handshake metadata.
    pub fn new(
        addr: PeerSocketAddr,
        inbound: bool,
        pingtime: Option<f64>,
        pingwait: Option<f64>,
    ) -> Self {
        Self {
            addr,
            subver: String::new(),
            version: 0,
            inbound,
            pingtime,
            pingwait,
        }
    }
}

impl From<MetaAddr> for PeerInfo {
    fn from(meta_addr: MetaAddr) -> Self {
        let subver = meta_addr.user_agent().unwrap_or_default().to_string();
        let version = meta_addr
            .negotiated_version()
            .map_or(0, |version| version.0);

        Self {
            addr: meta_addr.addr(),
            subver,
            version,
            inbound: meta_addr.is_inbound(),
            pingtime: meta_addr.rtt().map(|d| d.as_secs_f64()),
            pingwait: meta_addr.ping_sent_at().map(|t| t.elapsed().as_secs_f64()),
        }
    }
}

impl Default for PeerInfo {
    fn default() -> Self {
        Self {
            addr: PeerSocketAddr::unspecified(),
            subver: String::new(),
            version: 0,
            inbound: false,
            pingtime: None,
            pingwait: None,
        }
    }
}
