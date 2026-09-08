//! BLE discovery handshake (BlueZ). Advertise Presence over GATT; scan and
//! inject peers. File bytes stay on iroh LAN / WAN / relay.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use bluer::adv::Advertisement;
use bluer::gatt::local::{Application, Characteristic, CharacteristicRead, Service};
use bluer::{Adapter, AdapterEvent, Address, DiscoveryFilter, DiscoveryTransport, Uuid};
use futures::{pin_mut, FutureExt, StreamExt};
use omashare_core::events::{DiscoverySource, NearbyPeer};
use omashare_core::{Node, BLE_PRESENCE_CHAR_UUID, BLE_SERVICE_UUID};
use tokio::sync::Mutex;
use tracing::{info, warn};

const REFRESH: Duration = Duration::from_secs(2);
const REREAD: Duration = Duration::from_secs(20);

pub struct BleSession {
    _adv: bluer::adv::AdvertisementHandle,
    _app: bluer::gatt::local::ApplicationHandle,
    scan: tokio::task::JoinHandle<()>,
}

impl Drop for BleSession {
    fn drop(&mut self) {
        self.scan.abort();
    }
}

pub async fn start(node: Arc<Node>) -> Option<BleSession> {
    match run(node).await {
        Ok(s) => {
            info!("ble discovery listening");
            Some(s)
        }
        Err(e) => {
            warn!("ble discovery disabled: {e:#}");
            None
        }
    }
}

async fn run(node: Arc<Node>) -> Result<BleSession> {
    let service_uuid = Uuid::parse_str(BLE_SERVICE_UUID)?;
    let char_uuid = Uuid::parse_str(BLE_PRESENCE_CHAR_UUID)?;

    let session = bluer::Session::new().await?;
    let adapter = session.default_adapter().await?;
    adapter.set_powered(true).await?;
    let self_addr = adapter.address().await?;
    info!(adapter = %adapter.name(), %self_addr, "ble adapter");

    let payload = Arc::new(Mutex::new(presence_bytes(&node).await));
    let payload_write = payload.clone();
    let node_refresh = node.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(REFRESH).await;
            *payload_write.lock().await = presence_bytes(&node_refresh).await;
        }
    });

    let payload_read = payload.clone();
    let alias = node.alias().to_string();
    let le_advertisement = Advertisement {
        service_uuids: [service_uuid].into_iter().collect(),
        discoverable: Some(true),
        local_name: Some(alias.chars().take(11).collect()),
        ..Default::default()
    };
    let adv = adapter.advertise(le_advertisement).await?;

    let app = Application {
        services: vec![Service {
            uuid: service_uuid,
            primary: true,
            characteristics: vec![Characteristic {
                uuid: char_uuid,
                read: Some(CharacteristicRead {
                    read: true,
                    fun: Box::new(move |_req| {
                        let payload = payload_read.clone();
                        async move { Ok(payload.lock().await.clone()) }.boxed()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let gatt = adapter.serve_gatt_application(app).await?;

    // Do not filter by 128-bit UUID — BlueZ often drops Android ads that way.
    adapter
        .set_discovery_filter(DiscoveryFilter {
            transport: DiscoveryTransport::Le,
            duplicate_data: true,
            ..Default::default()
        })
        .await?;

    let scan_adapter = adapter.clone();
    let scan_node = node.clone();
    let scan = tokio::spawn(async move {
        if let Err(e) = scan_loop(scan_adapter, scan_node, self_addr, service_uuid, char_uuid).await
        {
            warn!("ble scan: {e:#}");
        }
    });

    Ok(BleSession {
        _adv: adv,
        _app: gatt,
        scan,
    })
}

async fn presence_bytes(node: &Node) -> Vec<u8> {
    serde_json::to_vec(&node.presence().await).unwrap_or_default()
}

async fn scan_loop(
    adapter: Adapter,
    node: Arc<Node>,
    self_addr: Address,
    service_uuid: Uuid,
    char_uuid: Uuid,
) -> Result<()> {
    let mut last: HashMap<Address, Instant> = HashMap::new();
    loop {
        let Some(addr) = wait_candidate(&adapter, self_addr, service_uuid, &last).await? else {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        };
        last.insert(addr, Instant::now());
        info!(%addr, "ble candidate, pausing scan for GATT");
        match read_peer(&adapter, addr, service_uuid, char_uuid).await {
            Ok(Some(peer)) => {
                info!(alias = %peer.alias, %addr, "ble peer");
                node.inject_peer(peer).await;
            }
            Ok(None) => warn!(%addr, "ble GATT had no omashare presence"),
            Err(e) => warn!(%addr, "ble connect/read: {e:#}"),
        }
        if let Ok(dev) = adapter.device(addr) {
            let _ = dev.disconnect().await;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Run LE discovery until an Omashare advertiser appears, then drop the
/// stream so BlueZ stops scanning before we GATT-connect.
async fn wait_candidate(
    adapter: &Adapter,
    self_addr: Address,
    service_uuid: Uuid,
    last: &HashMap<Address, Instant>,
) -> Result<Option<Address>> {
    let events = adapter.discover_devices_with_changes().await?;
    pin_mut!(events);
    while let Some(evt) = events.next().await {
        let AdapterEvent::DeviceAdded(addr) = evt else {
            continue;
        };
        if addr == self_addr {
            continue;
        }
        if last.get(&addr).is_some_and(|t| t.elapsed() < REREAD) {
            continue;
        }
        if is_omashare_adv(adapter, addr, service_uuid).await {
            return Ok(Some(addr));
        }
    }
    Ok(None)
}

async fn is_omashare_adv(adapter: &Adapter, addr: Address, service_uuid: Uuid) -> bool {
    let Ok(device) = adapter.device(addr) else {
        return false;
    };
    let present = device.rssi().await.ok().flatten().is_some()
        || device.is_connected().await.unwrap_or(false);
    if !present {
        return false;
    }
    let uuids = device.uuids().await.ok().flatten().unwrap_or_default();
    if uuids.contains(&service_uuid) {
        return true;
    }
    let sd = device
        .service_data()
        .await
        .ok()
        .flatten()
        .unwrap_or_default();
    sd.contains_key(&service_uuid)
}

async fn read_peer(
    adapter: &Adapter,
    addr: Address,
    service_uuid: Uuid,
    char_uuid: Uuid,
) -> Result<Option<NearbyPeer>> {
    let device = adapter.device(addr)?;
    if !device.is_connected().await? {
        device.connect().await?;
    }
    for service in device.services().await? {
        if service.uuid().await? != service_uuid {
            continue;
        }
        for ch in service.characteristics().await? {
            if ch.uuid().await? != char_uuid {
                continue;
            }
            let bytes = ch.read().await?;
            let presence: omashare_core::Presence = serde_json::from_slice(&bytes)?;
            return Ok(Some(NearbyPeer::from_presence(
                presence,
                DiscoverySource::Bluetooth,
            )));
        }
    }
    Ok(None)
}
