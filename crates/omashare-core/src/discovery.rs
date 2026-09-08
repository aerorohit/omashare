//! LAN discovery: UDP multicast + HTTP info + subnet unicast scan.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4, UdpSocket as StdUdp};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use axum::{extract::State, routing::get, Json, Router};
use iroh::EndpointId;
use socket2::{Domain, Protocol, Socket, Type};
use tokio::sync::{Notify, RwLock};
use tracing::{debug, warn};

use crate::events::{DiscoverySource, NearbyPeer};
use crate::protocol::{Presence, MULTICAST_ADDR};

const ANNOUNCE_INTERVAL: Duration = Duration::from_secs(2);
/// Must be longer than subnet-scan interval so peers do not flap in and out.
const PEER_TTL: Duration = Duration::from_secs(45);

#[derive(Clone)]
pub struct Discovery {
    inner: Arc<Inner>,
}

struct Inner {
    presence: RwLock<Presence>,
    port: u16,
    peers: RwLock<HashMap<String, (NearbyPeer, Instant)>>,
    rebind: Notify,
}

fn should_update_source(existing: DiscoverySource, new: DiscoverySource) -> bool {
    match new {
        DiscoverySource::Mdns => false,
        DiscoverySource::Bluetooth => matches!(
            existing,
            DiscoverySource::Bluetooth | DiscoverySource::Manual
        ),
        _ => true,
    }
}

impl Discovery {
    pub fn new(presence: Presence, port: u16) -> Self {
        Self {
            inner: Arc::new(Inner {
                presence: RwLock::new(presence),
                port,
                peers: RwLock::new(HashMap::new()),
                rebind: Notify::new(),
            }),
        }
    }

    pub async fn peers(&self) -> Vec<NearbyPeer> {
        let mut map = self.inner.peers.write().await;
        let now = Instant::now();
        map.retain(|_, (_, seen)| now.duration_since(*seen) < PEER_TTL);
        map.values().map(|(p, _)| p.clone()).collect()
    }

    pub async fn upsert(&self, mut peer: NearbyPeer) {
        let self_id = self.inner.presence.read().await.endpoint_id.clone();
        if peer.endpoint_id == self_id {
            return;
        }
        let mut map = self.inner.peers.write().await;
        let alias_lc = peer.alias.to_ascii_lowercase();
        map.retain(|id, (p, _)| {
            p.alias.to_ascii_lowercase() != alias_lc || *id == peer.endpoint_id
        });
        if let Some((existing, seen)) = map.get_mut(&peer.endpoint_id) {
            for a in peer.addrs.drain(..) {
                if !existing.addrs.contains(&a) {
                    existing.addrs.push(a);
                }
            }
            existing.alias = peer.alias;
            existing.device_kind = peer.device_kind;
            existing.pin_required = peer.pin_required;
            if should_update_source(existing.source, peer.source) {
                existing.source = peer.source;
            }
            *seen = Instant::now();
        } else {
            map.insert(peer.endpoint_id.clone(), (peer, Instant::now()));
        }
    }

    pub async fn presence(&self) -> Presence {
        self.inner.presence.read().await.clone()
    }

    pub async fn set_direct_addrs(&self, addrs: Vec<String>) {
        self.inner.presence.write().await.addrs = addrs;
    }

    /// Rebind multicast and scan the subnet now (Wi-Fi came back).
    pub async fn refresh(&self) {
        self.inner.rebind.notify_waiters();
        let scan = self.clone();
        tokio::spawn(async move {
            if let Err(e) = scan.subnet_scan().await {
                debug!("refresh subnet scan: {e:#}");
            }
        });
    }

    pub fn spawn(&self, enable_scan: bool) {
        let http = self.clone();
        tokio::spawn(async move {
            if let Err(e) = http.serve_http().await {
                warn!("discovery http: {e:#}");
            }
        });
        let mc = self.clone();
        tokio::spawn(async move {
            if let Err(e) = mc.multicast_loop().await {
                warn!("discovery multicast: {e:#}");
            }
        });
        if enable_scan {
            let scan = self.clone();
            let every = if cfg!(target_os = "android") {
                Duration::from_secs(20)
            } else {
                Duration::from_secs(8)
            };
            tokio::spawn(async move {
                loop {
                    if let Err(e) = scan.subnet_scan().await {
                        debug!("subnet scan: {e:#}");
                    }
                    tokio::time::sleep(every).await;
                }
            });
        }
    }

    async fn serve_http(&self) -> Result<()> {
        let state = self.clone();
        let app = Router::new()
            .route("/omashare/info", get(info_handler))
            .with_state(state);
        let addr = SocketAddr::from(([0, 0, 0, 0], self.inner.port));
        let listener = tokio::net::TcpListener::bind(addr).await?;
        axum::serve(listener, app).await?;
        Ok(())
    }

    async fn multicast_loop(&self) -> Result<()> {
        let maddr = Ipv4Addr::from(MULTICAST_ADDR);
        let dest = SocketAddr::V4(SocketAddrV4::new(maddr, self.inner.port));
        let mut buf = vec![0u8; 2048];
        let mut tick = tokio::time::interval(ANNOUNCE_INTERVAL);
        loop {
            let sock = match bind_multicast(self.inner.port) {
                Ok(s) => {
                    if let Err(e) = s.set_nonblocking(true) {
                        warn!("multicast nonblocking: {e}");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                    match tokio::net::UdpSocket::from_std(s) {
                        Ok(s) => s,
                        Err(e) => {
                            warn!("multicast tokio socket: {e}");
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            continue;
                        }
                    }
                }
                Err(e) => {
                    warn!("bind multicast: {e:#}");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };
            loop {
                tokio::select! {
                    _ = self.inner.rebind.notified() => {
                        debug!("rebind multicast");
                        break;
                    }
                    _ = tick.tick() => {
                        let payload = serde_json::to_vec(&self.presence().await)?;
                        if sock.send_to(&payload, dest).await.is_err() {
                            break;
                        }
                    }
                    ok = sock.recv_from(&mut buf) => {
                        match ok {
                            Ok((n, from)) => self.handle_datagram(&buf[..n], from).await,
                            Err(_) => break,
                        }
                    }
                }
            }
        }
    }

    async fn handle_datagram(&self, bytes: &[u8], from: SocketAddr) {
        let Ok(p) = serde_json::from_slice::<Presence>(bytes) else {
            return;
        };
        if p.endpoint_id.parse::<EndpointId>().is_err() {
            return;
        }
        let addrs = reachable_addrs(&p.addrs, Some(from.ip()));
        self.upsert(NearbyPeer {
            alias: p.alias,
            device_kind: p.device_kind,
            endpoint_id: p.endpoint_id,
            pin_required: p.pin_required,
            source: DiscoverySource::Multicast,
            addrs,
        })
        .await;
    }

    async fn subnet_scan(&self) -> Result<()> {
        let client = reqwest_lite();
        let port = self.inner.port;
        let mut urls = Vec::new();
        for (ip, _) in crate::addrs::physical_v4_nets() {
            let octets = ip.octets();
            for host in 1..=254u8 {
                if host == octets[3] {
                    continue;
                }
                let target = Ipv4Addr::new(octets[0], octets[1], octets[2], host);
                urls.push(format!("http://{target}:{port}/omashare/info"));
            }
        }
        use futures_util::StreamExt;
        let results: Vec<Option<(String, Presence)>> = futures_util::stream::iter(urls)
            .map(|url| {
                let c = client.clone();
                async move {
                    let resp = c.get(&url).send().await.ok()?;
                    let p = resp.json::<Presence>().await.ok()?;
                    Some((url, p))
                }
            })
            .buffer_unordered(32)
            .collect()
            .await;
        for (url, p) in results.into_iter().flatten() {
            if p.endpoint_id.parse::<EndpointId>().is_err() {
                continue;
            }
            let seen = url
                .strip_prefix("http://")
                .and_then(|rest| rest.split([':', '/']).next())
                .and_then(|h| h.parse::<IpAddr>().ok());
            let addrs = reachable_addrs(&p.addrs, seen);
            self.upsert(NearbyPeer {
                alias: p.alias,
                device_kind: p.device_kind,
                endpoint_id: p.endpoint_id,
                pin_required: p.pin_required,
                source: DiscoverySource::SubnetScan,
                addrs,
            })
            .await;
        }
        Ok(())
    }
}

/// Prefer advertised Iroh sockets; if they bind on 0.0.0.0, use the IP we actually saw.
fn reachable_addrs(advertised: &[String], seen_from: Option<IpAddr>) -> Vec<String> {
    let mut out = Vec::new();
    for a in advertised {
        if a.starts_with("http://") || a.starts_with("https://") {
            if !out.contains(a) {
                out.push(a.clone());
            }
            continue;
        }
        let Ok(mut sa) = a.parse::<SocketAddr>() else {
            continue;
        };
        if sa.ip().is_unspecified() {
            if let Some(ip) = seen_from {
                sa.set_ip(ip);
            } else {
                continue;
            }
        }
        if sa.ip().is_loopback() {
            continue;
        }
        let s = sa.to_string();
        if !out.contains(&s) {
            out.push(s);
        }
    }
    out
}

fn reqwest_lite() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_millis(180))
        .connect_timeout(Duration::from_millis(120))
        .no_proxy()
        .build()
        .expect("http client")
}

async fn info_handler(State(disc): State<Discovery>) -> Json<Presence> {
    Json(disc.presence().await)
}

fn ipv4_ifaces() -> Vec<Ipv4Addr> {
    let mut ifaces: Vec<Ipv4Addr> = crate::addrs::physical_v4_nets()
        .into_iter()
        .map(|(ip, _)| ip)
        .collect();
    ifaces.sort();
    ifaces.dedup();
    ifaces
}

fn bind_multicast(port: u16) -> Result<StdUdp> {
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_reuse_address(true)?;
    #[cfg(unix)]
    socket.set_reuse_port(true)?;
    socket.set_multicast_loop_v4(true)?;
    socket.set_multicast_ttl_v4(1)?;
    let bind_addr = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port);
    socket.bind(&bind_addr.into())?;
    let group = Ipv4Addr::from(MULTICAST_ADDR);
    let mut ifaces = ipv4_ifaces();
    if ifaces.is_empty() {
        ifaces.push(Ipv4Addr::UNSPECIFIED);
    }
    for iface in ifaces {
        if let Err(e) = socket.join_multicast_v4(&group, &iface) {
            debug!("join multicast on {iface}: {e}");
        }
    }
    Ok(socket.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{DeviceKind, Presence};

    fn peer(alias: &str, eid: &str) -> NearbyPeer {
        NearbyPeer {
            alias: alias.into(),
            device_kind: DeviceKind::Desktop,
            endpoint_id: eid.into(),
            pin_required: false,
            source: DiscoverySource::Manual,
            addrs: vec!["192.168.1.3:9".into()],
        }
    }

    #[tokio::test]
    async fn one_row_per_alias() {
        let disc = Discovery::new(
            Presence {
                alias: "me".into(),
                device_kind: DeviceKind::Desktop,
                endpoint_id: "self".into(),
                pin_required: false,
                addrs: vec![],
            },
            0,
        );
        disc.upsert(peer("blackbird", "id-old")).await;
        disc.upsert(peer("Blackbird", "id-new")).await;
        let peers = disc.peers().await;
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].endpoint_id, "id-new");
        assert_eq!(peers[0].alias, "Blackbird");
    }
}
