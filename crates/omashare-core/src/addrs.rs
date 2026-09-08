use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use iroh::endpoint::Connection;
use iroh::{Endpoint, EndpointAddr, EndpointId, RelayUrl, TransportAddr};

use crate::events::TransferPath;

pub fn is_cgnat(ip: Ipv4Addr) -> bool {
    let o = ip.octets();
    o[0] == 100 && (64..128).contains(&o[1])
}

pub fn is_lan_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v) => (v.is_private() && !is_cgnat(v)) || v.is_loopback() || v.is_link_local(),
        IpAddr::V6(v) => v.is_loopback() || v.is_unique_local(),
    }
}

pub fn classify_transport(addr: &TransportAddr) -> TransferPath {
    match addr {
        TransportAddr::Relay(_) => TransferPath::Relay,
        TransportAddr::Ip(sa) if is_lan_ip(sa.ip()) => TransferPath::Lan,
        TransportAddr::Ip(_) => TransferPath::DirectWan,
        _ => TransferPath::Unknown,
    }
}

fn skip_virtual_iface(name: &str) -> bool {
    let n = name.to_ascii_lowercase();
    n == "lo"
        || n.starts_with("docker")
        || n.starts_with("br-")
        || n.starts_with("veth")
        || n.starts_with("virbr")
        || n.starts_with("cni")
        || n.starts_with("flannel")
        || n.contains("tailscale")
        || n.starts_with("tun")
        || n.starts_with("wg")
}

pub fn physical_v4_nets() -> Vec<(Ipv4Addr, u32)> {
    if_addrs::get_if_addrs()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|iface| {
            if iface.is_loopback() || skip_virtual_iface(&iface.name) {
                return None;
            }
            match iface.addr {
                if_addrs::IfAddr::V4(v) if v.ip.is_private() && !is_cgnat(v.ip) => {
                    Some((v.ip, u32::from(v.netmask)))
                }
                _ => None,
            }
        })
        .collect()
}

fn on_physical_lan(ip: Ipv4Addr) -> bool {
    physical_v4_nets()
        .into_iter()
        .any(|(iface, mask)| (u32::from(iface) & mask) == (u32::from(ip) & mask))
}

pub fn next_download_backoff(prev: Duration) -> Duration {
    prev.saturating_mul(2).min(Duration::from_secs(15))
}

pub fn is_relay_hint(h: &str) -> bool {
    h.starts_with("http://") || h.starts_with("https://")
}

/// LAN sockets first, then other private IPv4, then every relay URL.
/// Relays are never dropped so magicsock can walk away from Wi-Fi.
pub fn prefer_lan_hints(hints: &[String]) -> Vec<String> {
    let mut lan = Vec::new();
    let mut relays = Vec::new();
    let mut other_priv = Vec::new();
    for h in hints {
        if is_relay_hint(h) {
            if !relays.contains(h) {
                relays.push(h.clone());
            }
            continue;
        }
        let Ok(sa) = h.parse::<SocketAddr>() else {
            continue;
        };
        match sa.ip() {
            IpAddr::V4(v) if on_physical_lan(v) => lan.push(h.clone()),
            IpAddr::V4(v) if v.is_private() && !is_cgnat(v) => other_priv.push(h.clone()),
            _ => {}
        }
    }
    let mut out = if lan.is_empty() { other_priv } else { lan };
    out.extend(relays);
    out
}

pub fn merge_relay_hints(hints: &[String], extra_relays: &[String]) -> Vec<String> {
    let mut out = hints.to_vec();
    for r in extra_relays {
        if is_relay_hint(r) && !out.contains(r) {
            out.push(r.clone());
        }
    }
    out
}

pub fn relays_from_endpoint(endpoint: &Endpoint) -> Vec<String> {
    endpoint
        .addr()
        .addrs
        .iter()
        .filter_map(|a| match a {
            TransportAddr::Relay(url) => Some(url.to_string()),
            _ => None,
        })
        .collect()
}

pub fn endpoint_addr_from_hints(id: EndpointId, hints: &[String]) -> EndpointAddr {
    let mut addr = EndpointAddr::new(id);
    for h in prefer_lan_hints(hints) {
        if let Ok(url) = h.parse::<RelayUrl>() {
            addr = addr.with_relay_url(url);
        } else if let Ok(sa) = h.parse::<SocketAddr>() {
            addr = addr.with_ip_addr(sa);
        }
    }
    addr
}

pub fn addr_has_relay(addr: &EndpointAddr) -> bool {
    addr.addrs
        .iter()
        .any(|a| matches!(a, TransportAddr::Relay(_)))
}

/// Selected IP path plus relays only — never the full historical path list.
pub fn connection_dial_hints(conn: &Connection) -> Vec<String> {
    let mut relays = Vec::new();
    let mut selected_ip = None;
    for p in conn.paths().iter() {
        match p.remote_addr() {
            TransportAddr::Relay(url) => {
                let s = url.to_string();
                if !relays.contains(&s) {
                    relays.push(s);
                }
            }
            TransportAddr::Ip(sa) if p.is_selected() => {
                selected_ip = Some(sa.to_string());
            }
            _ => {}
        }
    }
    let mut out = Vec::new();
    if let Some(ip) = selected_ip {
        out.push(ip);
    }
    out.extend(relays);
    out
}

/// Physical LAN IPv4 plus every relay URL from the local endpoint.
/// Used in HTTP `/omashare/info` and BLE GATT Presence so a handshake without
/// LAN addrs can still dial n0 relay / WAN.
pub fn ip_hint_list(endpoint: &Endpoint) -> Vec<String> {
    let nets = physical_v4_nets();
    let mut port = None;
    let mut relays = Vec::new();
    for a in &endpoint.addr().addrs {
        match a {
            TransportAddr::Ip(sa) if sa.port() != 0 => port = Some(sa.port()),
            TransportAddr::Relay(url) => {
                let s = url.to_string();
                if !relays.contains(&s) {
                    relays.push(s);
                }
            }
            _ => {}
        }
    }
    let mut out = Vec::new();
    if let Some(port) = port {
        for (ip, _) in nets {
            out.push(SocketAddr::from((ip, port)).to_string());
        }
    }
    out.extend(relays);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh::SecretKey;

    fn eid() -> EndpointId {
        SecretKey::from_bytes(&[0x11; 32]).public()
    }

    #[test]
    fn prefer_lan_keeps_relays_with_lan_ip() {
        let hints = vec![
            "10.50.0.3:56266".into(),
            "192.168.1.3:56266".into(),
            "100.68.124.36:56266".into(),
            "https://euw1-1.relay.iroh.network./".into(),
        ];
        let out = prefer_lan_hints(&hints);
        assert!(
            out.iter().any(|h| h.starts_with("https://")),
            "relays must survive LAN filtering: {out:?}"
        );
        assert!(
            !out.iter().any(|h| h.starts_with("10.50.")),
            "docker should be dropped when a LAN IP exists: {out:?}"
        );
        assert!(
            !out.iter().any(|h| h.starts_with("100.68.")),
            "CGNAT should be dropped: {out:?}"
        );
    }

    #[test]
    fn merge_relay_hints_appends_local_n0() {
        let peer = vec!["192.168.1.8:1234".into()];
        let extra = vec!["https://relay.example/".into()];
        let merged = merge_relay_hints(&peer, &extra);
        let addr = endpoint_addr_from_hints(eid(), &merged);
        assert!(addr_has_relay(&addr), "{:?}", addr.addrs);
        assert!(addr
            .addrs
            .iter()
            .any(|a| matches!(a, TransportAddr::Ip(sa) if sa.ip().is_ipv4())));
    }

    #[test]
    fn cgnat_helper() {
        assert!(is_cgnat(Ipv4Addr::new(100, 64, 0, 1)));
        assert!(!is_cgnat(Ipv4Addr::new(192, 168, 1, 1)));
        assert!(!is_cgnat(Ipv4Addr::new(10, 0, 0, 1)));
    }

    #[test]
    fn backoff_caps_at_15s() {
        let mut d = Duration::from_millis(400);
        d = next_download_backoff(d);
        assert_eq!(d, Duration::from_millis(800));
        for _ in 0..20 {
            d = next_download_backoff(d);
        }
        assert_eq!(d, Duration::from_secs(15));
    }
}
