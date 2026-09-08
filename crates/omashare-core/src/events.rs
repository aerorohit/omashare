use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use crate::protocol::{DeviceKind, FileOffer, MeteredPolicy, NetworkHint};

/// Next event, skipping lagged bursts so a slow UI cannot kill the listener.
pub async fn recv_event(rx: &mut broadcast::Receiver<NodeEvent>) -> Option<NodeEvent> {
    loop {
        match rx.recv().await {
            Ok(ev) => return Some(ev),
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                tracing::warn!("event listener lagged, skipped {skipped}");
            }
            Err(broadcast::error::RecvError::Closed) => return None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NearbyPeer {
    pub alias: String,
    pub device_kind: DeviceKind,
    pub endpoint_id: String,
    pub pin_required: bool,
    pub source: DiscoverySource,
    /// Direct IP hints from LAN discovery (help magicsock skip relays).
    pub addrs: Vec<String>,
}

impl NearbyPeer {
    pub fn from_presence(p: crate::protocol::Presence, source: DiscoverySource) -> Self {
        Self {
            alias: p.alias,
            device_kind: p.device_kind,
            endpoint_id: p.endpoint_id,
            pin_required: p.pin_required,
            source,
            addrs: p.addrs,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscoverySource {
    Multicast,
    Mdns,
    SubnetScan,
    Manual,
    NativeDnsSd,
    Bluetooth,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum NodeEvent {
    NearbyUpdated {
        peers: Vec<NearbyPeer>,
    },
    IncomingOffer {
        session_id: String,
        from_alias: String,
        from_endpoint: String,
        files: Vec<FileOffer>,
        pin_required: bool,
    },
    OfferResolved {
        session_id: String,
        accepted: bool,
    },
    /// Sender: receiver accepted; blobs are being served (not file-complete).
    OfferAccepted {
        session_id: String,
    },
    TransferProgress {
        session_id: String,
        bytes_done: u64,
        bytes_total: u64,
        path: TransferPath,
    },
    TransferComplete {
        session_id: String,
        saved_to: Option<String>,
    },
    TransferFailed {
        session_id: String,
        error: String,
    },
    MeteredConsentNeeded {
        session_id: String,
        bytes_done: u64,
        bytes_total: u64,
        hint: NetworkHint,
    },
    PathChanged {
        session_id: String,
        path: TransferPath,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransferPath {
    Lan,
    DirectWan,
    Relay,
    Unknown,
}

#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub alias: String,
    pub device_kind: DeviceKind,
    /// Optional 4-digit PIN; incoming offers must match when set.
    pub pin: Option<String>,
    pub receive_dir: std::path::PathBuf,
    pub data_dir: std::path::PathBuf,
    pub discovery_port: u16,
    pub enable_mdns: bool,
    pub enable_subnet_scan: bool,
    /// UDP multicast + HTTP /omashare/info. Disable in unit tests.
    pub enable_lan_beacon: bool,
    pub metered_policy: MeteredPolicy,
    /// Use n0 relays + DNS discovery. Disable in tests / fully offline LAN.
    pub use_relays: bool,
}

impl NodeConfig {
    pub fn default_alias() -> String {
        hostname::get()
            .ok()
            .and_then(|h| h.into_string().ok())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "omashare".into())
    }
}
