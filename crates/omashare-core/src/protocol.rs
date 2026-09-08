//! Omashare protocol constants and wire messages.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Application ALPN for offer/consent (not blob bytes).
pub const OFFER_ALPN: &[u8] = b"omashare/offer/0";

/// UDP multicast group for LAN presence (distinct from LocalSend).
pub const MULTICAST_ADDR: [u8; 4] = [224, 0, 0, 169];

/// Shared TCP (HTTP info) + UDP (multicast) discovery port.
pub const DISCOVERY_PORT: u16 = 53318;

pub const DNS_SD_SERVICE: &str = "_omashare._udp.local.";

/// BLE GATT service. Advertisements carry this UUID only (31-byte ads cannot
/// hold an EndpointId). Full [`Presence`] is a GATT read of
/// [`BLE_PRESENCE_CHAR_UUID`].
pub const BLE_SERVICE_UUID: &str = "6f6d6173-6861-7265-0000-000000005318";

/// BLE GATT characteristic (read): UTF-8 JSON [`Presence`], including home
/// relay URLs so iroh can dial WAN/relay after a Bluetooth handshake.
pub const BLE_PRESENCE_CHAR_UUID: &str = "6f6d6173-6861-7265-0000-000000005319";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeviceKind {
    Mobile,
    Desktop,
    Headless,
    Unknown,
}

impl Default for DeviceKind {
    fn default() -> Self {
        Self::Unknown
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Presence {
    pub alias: String,
    pub device_kind: DeviceKind,
    /// Iroh endpoint id, base32.
    pub endpoint_id: String,
    pub pin_required: bool,
    /// Direct Iroh UDP addresses (`ip:port`) and home relay URLs so peers can
    /// connect on LAN or via WAN/relay after BLE/HTTP discovery.
    #[serde(default)]
    pub addrs: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileOffer {
    pub name: String,
    pub size: u64,
    /// BLAKE3 hex from iroh-blobs.
    pub hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WireMsg {
    Offer {
        session_id: Uuid,
        alias: String,
        files: Vec<FileOffer>,
        pin: Option<String>,
        /// Sender asks the receiver to confirm before using metered networks.
        metered_policy: MeteredPolicy,
    },
    Accept {
        session_id: Uuid,
    },
    Decline {
        session_id: Uuid,
        reason: String,
    },
    Cancel {
        session_id: Uuid,
    },
    /// Receiver finished saving files; sender can treat the session as done.
    Complete {
        session_id: Uuid,
    },
    /// Receiver download progress so the sender can update its UI.
    Progress {
        session_id: Uuid,
        bytes_done: u64,
        bytes_total: u64,
    },
    /// Receiver tells sender the path is now metered and is paused.
    MeteredPause {
        session_id: Uuid,
        bytes_done: u64,
        bytes_total: u64,
    },
    MeteredDecision {
        session_id: Uuid,
        allow: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum MeteredPolicy {
    Ask,
    /// Continue on any path (LAN, WAN, cellular, relay). Default for walk-away.
    #[default]
    Allow,
    Deny,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum NetworkHint {
    #[default]
    Wifi,
    Metered,
    Unknown,
}
