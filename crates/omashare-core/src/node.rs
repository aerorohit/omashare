use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use iroh::address_lookup::memory::MemoryLookup;
use iroh::endpoint::{Connection, PathEvent, RecvStream, SendStream};
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use iroh::{Endpoint, EndpointAddr, EndpointId, SecretKey, TransportAddr};
use iroh_blobs::api::blobs::BlobStatus;
use iroh_blobs::api::remote::GetProgressItem;
use iroh_blobs::store::fs::FsStore;
use iroh_blobs::Hash;
use iroh_mdns_address_lookup::{DiscoveryEvent, MdnsAddressLookup};
use n0_future::StreamExt;
use tokio::sync::{broadcast, oneshot, watch, Mutex, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use uuid::Uuid;

use crate::addrs::{
    classify_transport, connection_dial_hints, endpoint_addr_from_hints, ip_hint_list,
    is_relay_hint, merge_relay_hints, next_download_backoff, relays_from_endpoint,
};
use crate::discovery::Discovery;
use crate::events::{NearbyPeer, NodeConfig, NodeEvent, TransferPath};
use crate::framing::{read_msg, write_msg};
use crate::protocol::{FileOffer, NetworkHint, Presence, WireMsg, DISCOVERY_PORT, OFFER_ALPN};

const DOWNLOAD_DEADLINE: Duration = Duration::from_secs(20 * 60);
const DOWNLOAD_IDLE: Duration = Duration::from_secs(60);
const PROGRESS_MIN: Duration = Duration::from_millis(200);

#[derive(Clone)]
struct TransferSession {
    last_path: TransferPath,
    bytes_done: u64,
    bytes_total: u64,
    token: CancellationToken,
    blobs: Vec<(Hash, u64)>,
    last_progress_emit: Instant,
    last_wire_progress: Instant,
    peer: Option<EndpointId>,
}

pub struct Node {
    inner: Arc<NodeInner>,
    router: Router,
}

pub struct NodeInner {
    pub endpoint: Endpoint,
    pub store: FsStore,
    pub discovery: Discovery,
    pub config: NodeConfig,
    pub events: broadcast::Sender<NodeEvent>,
    pub pending_offers: Mutex<HashMap<Uuid, oneshot::Sender<bool>>>,
    pub pending_metered: Mutex<HashMap<Uuid, oneshot::Sender<bool>>>,
    pub network_hint: RwLock<NetworkHint>,
    pub memory_lookup: MemoryLookup,
    pub known: Mutex<HashMap<String, NearbyPeer>>,
    sessions: Mutex<HashMap<Uuid, TransferSession>>,
    offer_send: Mutex<HashMap<Uuid, iroh::endpoint::SendStream>>,
    offer_conn: Mutex<HashMap<Uuid, Connection>>,
    blob_sessions: Mutex<HashMap<Hash, Uuid>>,
    offer_reopen: Mutex<()>,
    blob_pulls: Mutex<HashMap<Hash, Arc<Mutex<()>>>>,
}

impl NodeInner {
    fn peer_addr(&self, id: EndpointId, hints: &[String]) -> EndpointAddr {
        let extra = if hints.iter().any(|h| is_relay_hint(h)) {
            Vec::new()
        } else {
            relays_from_endpoint(&self.endpoint)
        };
        let merged = merge_relay_hints(hints, &extra);
        endpoint_addr_from_hints(id, &merged)
    }

    async fn set_peer_lookup(&self, remote: EndpointId, hints: &[String]) {
        let addr = self.peer_addr(remote, hints);
        self.memory_lookup.remove_endpoint_info(remote);
        if !addr.addrs.is_empty() {
            self.memory_lookup.add_endpoint_info(addr);
        }
    }

    async fn remember_peer(&self, peer: &NearbyPeer) {
        let Ok(id) = peer.endpoint_id.parse::<EndpointId>() else {
            return;
        };
        self.set_peer_lookup(id, &peer.addrs).await;
    }

    async fn refresh_peer_lookup(&self, remote: &EndpointId) {
        let id = remote.to_string();
        let hints = if let Some(p) = self
            .discovery
            .peers()
            .await
            .into_iter()
            .find(|p| p.endpoint_id == id)
        {
            p.addrs
        } else if let Some(p) = self.known.lock().await.get(&id) {
            p.addrs.clone()
        } else {
            Vec::new()
        };
        self.set_peer_lookup(*remote, &hints).await;
    }

    async fn touch_session(&self, session_id: Uuid, bytes_total: u64) {
        let mut sessions = self.sessions.lock().await;
        sessions
            .entry(session_id)
            .and_modify(|s| s.bytes_total = bytes_total.max(s.bytes_total))
            .or_insert(TransferSession {
                last_path: TransferPath::Unknown,
                bytes_done: 0,
                bytes_total,
                token: CancellationToken::new(),
                blobs: Vec::new(),
                last_progress_emit: Instant::now(),
                last_wire_progress: Instant::now() - PROGRESS_MIN,
                peer: None,
            });
    }

    fn new_session(bytes_total: u64) -> TransferSession {
        TransferSession {
            last_path: TransferPath::Unknown,
            bytes_done: 0,
            bytes_total,
            token: CancellationToken::new(),
            blobs: Vec::new(),
            last_progress_emit: Instant::now(),
            last_wire_progress: Instant::now() - PROGRESS_MIN,
            peer: None,
        }
    }

    async fn set_session_peer(&self, session_id: Uuid, peer: EndpointId) {
        let mut sessions = self.sessions.lock().await;
        let s = sessions
            .entry(session_id)
            .or_insert_with(|| Self::new_session(0));
        s.peer = Some(peer);
    }

    fn is_outbound_session(s: &TransferSession) -> bool {
        !s.blobs.is_empty()
    }

    async fn write_offer_msg(&self, session_id: Uuid, msg: &WireMsg) -> Result<()> {
        {
            let mut map = self.offer_send.lock().await;
            if let Some(send) = map.get_mut(&session_id) {
                if write_msg(send, msg).await.is_ok() {
                    return Ok(());
                }
                map.remove(&session_id);
            }
        }
        // Progress must not open a new offer connection — that races the blobs
        // GET on the same peer and aborts it as "Receiver closed".
        if !matches!(msg, WireMsg::Complete { .. }) {
            return Ok(());
        }
        let _guard = self.offer_reopen.lock().await;
        self.reopen_offer_and_write(session_id, msg).await
    }

    async fn blob_pull_lock(&self, hash: Hash) -> Arc<Mutex<()>> {
        let mut map = self.blob_pulls.lock().await;
        map.entry(hash)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    async fn reopen_offer_and_write(&self, session_id: Uuid, msg: &WireMsg) -> Result<()> {
        let (peer, token) = {
            let sessions = self.sessions.lock().await;
            let s = sessions.get(&session_id).context("no session")?;
            let peer = s.peer.context("no peer for offer reconnect")?;
            (peer, s.token.clone())
        };
        self.refresh_peer_lookup(&peer).await;
        let addr = self.endpoint_addr_for(peer).await;
        info!(%session_id, peer = %peer, "reopening offer stream after path change");
        let conn = connect_offer(self, peer, &peer.to_string(), addr, &token).await?;
        let (mut send, _recv) = conn.open_bi().await.context("reopen offer stream")?;
        write_msg(&mut send, msg).await?;
        if matches!(msg, WireMsg::Complete { .. }) {
            let _ = send.finish();
            self.offer_send.lock().await.remove(&session_id);
        } else {
            self.offer_send.lock().await.insert(session_id, send);
            self.offer_conn.lock().await.insert(session_id, conn);
        }
        Ok(())
    }

    async fn on_outbound_complete(&self, session_id: Uuid) {
        let snap = {
            let sessions = self.sessions.lock().await;
            sessions.get(&session_id).and_then(|s| {
                Self::is_outbound_session(s).then_some((s.bytes_total, s.last_path))
            })
        };
        let Some((bytes_total, path)) = snap else {
            return;
        };
        if bytes_total > 0 {
            self.emit_progress(session_id, bytes_total, bytes_total, path, true)
                .await;
        }
        self.finish_session_streams(session_id).await;
        let _ = self.events.send(NodeEvent::TransferComplete {
            session_id: session_id.to_string(),
            saved_to: None,
        });
    }

    async fn on_outbound_cancel(&self, session_id: Uuid, error: String) {
        self.session_token(session_id).await.cancel();
        self.finish_session_streams(session_id).await;
        let _ = self.events.send(NodeEvent::TransferFailed {
            session_id: session_id.to_string(),
            error,
        });
    }

    async fn apply_outbound_wire(&self, session_id: Uuid, msg: WireMsg) -> bool {
        match msg {
            WireMsg::Progress {
                bytes_done,
                bytes_total,
                ..
            } => {
                self.emit_progress(
                    session_id,
                    bytes_done,
                    bytes_total,
                    TransferPath::Unknown,
                    true,
                )
                .await;
                false
            }
            WireMsg::Complete { .. } => {
                info!("offer {session_id} complete (receiver saved)");
                self.on_outbound_complete(session_id).await;
                true
            }
            WireMsg::Cancel { .. } => {
                self.on_outbound_cancel(session_id, "cancelled".into()).await;
                true
            }
            _ => false,
        }
    }

    async fn remember_live_paths(&self, conn: &Connection) {
        let id = conn.remote_id();
        let mut hints = connection_dial_hints(conn);
        for r in relays_from_endpoint(&self.endpoint) {
            if !hints.contains(&r) {
                hints.push(r);
            }
        }
        if hints.is_empty() {
            return;
        }
        info!(peer = %id, addrs = ?hints, "live connection paths");
        self.set_peer_lookup(id, &hints).await;
    }

    async fn blobs_addr(&self, session_id: Uuid, remote: EndpointId) -> EndpointAddr {
        let mut hints = Vec::new();
        if let Some(conn) = self.offer_conn.lock().await.get(&session_id) {
            hints.extend(connection_dial_hints(conn));
        }
        for r in relays_from_endpoint(&self.endpoint) {
            if !hints.contains(&r) {
                hints.push(r);
            }
        }
        if hints.is_empty() {
            return self.endpoint_addr_for(remote).await;
        }
        self.set_peer_lookup(remote, &hints).await;
        self.peer_addr(remote, &hints)
    }

    async fn endpoint_addr_for(&self, remote: EndpointId) -> EndpointAddr {
        self.refresh_peer_lookup(&remote).await;
        let id = remote.to_string();
        let hints = if let Some(p) = self
            .discovery
            .peers()
            .await
            .into_iter()
            .find(|p| p.endpoint_id == id)
        {
            p.addrs
        } else if let Some(p) = self.known.lock().await.get(&id) {
            p.addrs.clone()
        } else {
            Vec::new()
        };
        self.peer_addr(remote, &hints)
    }

    async fn emit_progress(
        &self,
        session_id: Uuid,
        bytes_done: u64,
        bytes_total: u64,
        path: TransferPath,
        force: bool,
    ) {
        let snap = {
            let mut sessions = self.sessions.lock().await;
            let s = sessions
                .entry(session_id)
                .or_insert_with(|| Self::new_session(bytes_total));
            s.bytes_total = bytes_total.max(s.bytes_total);
            if path != TransferPath::Unknown {
                s.last_path = path;
            }
            // Per-GET counters restart at 0 on path switch; keep a high-watermark.
            if bytes_done < s.bytes_done {
                if !force {
                    return;
                }
            } else {
                s.bytes_done = bytes_done;
            }
            let total = s.bytes_total;
            let first = s.bytes_done == 0;
            let complete = total > 0 && s.bytes_done >= total;
            let force = force || first || complete;
            let now = Instant::now();
            if !force && now.duration_since(s.last_progress_emit) < PROGRESS_MIN {
                return;
            }
            s.last_progress_emit = now;
            (s.bytes_done, s.bytes_total.max(1), s.last_path)
        };
        let _ = self.events.send(NodeEvent::TransferProgress {
            session_id: session_id.to_string(),
            bytes_done: snap.0,
            bytes_total: snap.1,
            path: snap.2,
        });
    }

    async fn register_send_blobs(&self, session_id: Uuid, files: &[FileOffer]) {
        let mut blobs = Vec::new();
        let mut map = self.blob_sessions.lock().await;
        for f in files {
            if let Ok(hash) = f.hash.parse::<Hash>() {
                blobs.push((hash, f.size));
                map.insert(hash, session_id);
            }
        }
        drop(map);
        let mut sessions = self.sessions.lock().await;
        let s = sessions
            .entry(session_id)
            .or_insert_with(|| Self::new_session(0));
        s.blobs = blobs;
        s.bytes_total = files.iter().map(|f| f.size).sum::<u64>().max(s.bytes_total);
    }

    async fn finish_session_streams(&self, session_id: Uuid) {
        self.offer_send.lock().await.remove(&session_id);
        self.offer_conn.lock().await.remove(&session_id);
        if let Some(s) = self.sessions.lock().await.remove(&session_id) {
            let mut map = self.blob_sessions.lock().await;
            for (h, _) in s.blobs {
                map.remove(&h);
            }
        }
    }

    async fn tell_sender_progress(&self, session_id: Uuid, bytes_done: u64, bytes_total: u64) {
        {
            let mut sessions = self.sessions.lock().await;
            let Some(s) = sessions.get_mut(&session_id) else {
                return;
            };
            if bytes_done < s.bytes_done {
                return;
            }
            let force = bytes_total > 0 && bytes_done >= bytes_total;
            let now = Instant::now();
            if !force && now.duration_since(s.last_wire_progress) < PROGRESS_MIN {
                return;
            }
            s.last_wire_progress = now;
        }
        let msg = WireMsg::Progress {
            session_id,
            bytes_done,
            bytes_total,
        };
        if let Err(e) = self.write_offer_msg(session_id, &msg).await {
            warn!(%session_id, "could not send progress to sender: {e:#}");
        }
    }

    async fn tell_sender_complete(&self, session_id: Uuid) {
        let msg = WireMsg::Complete { session_id };
        for attempt in 1..=8u32 {
            if self.write_offer_msg(session_id, &msg).await.is_ok() {
                break;
            }
            warn!(%session_id, "could not send complete to sender (attempt {attempt}/8)");
            tokio::time::sleep(Duration::from_millis(400 * u64::from(attempt))).await;
        }
        self.offer_send.lock().await.remove(&session_id);
        self.offer_conn.lock().await.remove(&session_id);
        if let Some(s) = self.sessions.lock().await.remove(&session_id) {
            let mut map = self.blob_sessions.lock().await;
            for (h, _) in s.blobs {
                map.remove(&h);
            }
        }
    }

    async fn session_token(&self, session_id: Uuid) -> CancellationToken {
        let mut sessions = self.sessions.lock().await;
        sessions
            .entry(session_id)
            .or_insert_with(|| Self::new_session(0))
            .token
            .clone()
    }

    async fn set_session_path(&self, session_id: Uuid, path: TransferPath) -> TransferSession {
        let mut sessions = self.sessions.lock().await;
        let s = sessions.entry(session_id).or_insert_with(|| Self::new_session(0));
        s.last_path = path;
        s.clone()
    }
}

impl std::fmt::Debug for OfferHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OfferHandler").finish()
    }
}

#[derive(Clone)]
struct OfferHandler {
    inner: Arc<NodeInner>,
}

impl ProtocolHandler for OfferHandler {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        if let Err(e) = self.handle(connection).await {
            warn!("offer handler: {e:#}");
        }
        Ok(())
    }
}

impl OfferHandler {
    async fn handle(&self, connection: Connection) -> Result<()> {
        let remote = connection.remote_id();
        let (send, mut recv) = connection.accept_bi().await?;
        let msg = read_msg(&mut recv).await?;
        match msg {
            WireMsg::Offer {
                session_id,
                alias,
                files,
                pin,
                metered_policy: _,
            } => {
                self.handle_offer(
                    session_id,
                    alias,
                    files,
                    pin,
                    remote,
                    connection,
                    send,
                    recv,
                )
                .await
            }
            WireMsg::Progress {
                session_id,
                bytes_done,
                bytes_total,
            } => {
                self.follow_outbound_offer(
                    session_id,
                    connection,
                    recv,
                    Some((bytes_done, bytes_total)),
                )
                .await
            }
            WireMsg::Complete { session_id } => {
                if self
                    .inner
                    .sessions
                    .lock()
                    .await
                    .get(&session_id)
                    .is_some_and(NodeInner::is_outbound_session)
                {
                    self.inner.on_outbound_complete(session_id).await;
                }
                Ok(())
            }
            WireMsg::Cancel { session_id } => {
                if self
                    .inner
                    .sessions
                    .lock()
                    .await
                    .get(&session_id)
                    .is_some_and(NodeInner::is_outbound_session)
                {
                    self.inner
                        .on_outbound_cancel(session_id, "cancelled".into())
                        .await;
                }
                Ok(())
            }
            other => {
                warn!(?other, "unexpected first message");
                Ok(())
            }
        }
    }

    async fn follow_outbound_offer(
        &self,
        session_id: Uuid,
        connection: Connection,
        mut recv: RecvStream,
        initial_progress: Option<(u64, u64)>,
    ) -> Result<()> {
        let outbound = self
            .inner
            .sessions
            .lock()
            .await
            .get(&session_id)
            .is_some_and(NodeInner::is_outbound_session);
        if !outbound {
            warn!(%session_id, "ignoring offer reconnect for unknown send session");
            return Ok(());
        }
        watch_paths(connection.clone(), self.inner.clone(), session_id);
        self.inner.remember_live_paths(&connection).await;
        if let Some((bytes_done, bytes_total)) = initial_progress {
            self.inner
                .emit_progress(
                    session_id,
                    bytes_done,
                    bytes_total,
                    TransferPath::Unknown,
                    true,
                )
                .await;
        }
        loop {
            match read_msg(&mut recv).await {
                Ok(msg) => {
                    if self.inner.apply_outbound_wire(session_id, msg).await {
                        break;
                    }
                }
                Err(_) => {
                    info!("offer {session_id} reconnect stream ended; still serving blobs");
                    break;
                }
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_offer(
        &self,
        session_id: Uuid,
        alias: String,
        files: Vec<FileOffer>,
        pin: Option<String>,
        remote: EndpointId,
        connection: Connection,
        mut send: SendStream,
        mut recv: RecvStream,
    ) -> Result<()> {
        if let Some(expected) = &self.inner.config.pin {
            if pin.as_deref() != Some(expected.as_str()) {
                write_msg(
                    &mut send,
                    &WireMsg::Decline {
                        session_id,
                        reason: "bad pin".into(),
                    },
                )
                .await?;
                return Ok(());
            }
        }

        let (tx, rx) = oneshot::channel();
        self.inner
            .pending_offers
            .lock()
            .await
            .insert(session_id, tx);

        let _ = self.inner.events.send(NodeEvent::IncomingOffer {
            session_id: session_id.to_string(),
            from_alias: alias,
            from_endpoint: remote.to_string(),
            files: files.clone(),
            pin_required: self.inner.config.pin.is_some(),
        });

        let accepted = tokio::time::timeout(Duration::from_secs(120), rx)
            .await
            .ok()
            .and_then(|r| r.ok())
            .unwrap_or(false);

        let _ = self.inner.events.send(NodeEvent::OfferResolved {
            session_id: session_id.to_string(),
            accepted,
        });

        if !accepted {
            write_msg(
                &mut send,
                &WireMsg::Decline {
                    session_id,
                    reason: "declined".into(),
                },
            )
            .await?;
            return Ok(());
        }

        write_msg(&mut send, &WireMsg::Accept { session_id }).await?;
        self.inner.set_session_peer(session_id, remote).await;
        self.inner
            .offer_send
            .lock()
            .await
            .insert(session_id, send);
        self.inner
            .offer_conn
            .lock()
            .await
            .insert(session_id, connection.clone());
        let token = self.inner.session_token(session_id).await;
        let cancel_watch = token.clone();
        tokio::spawn(async move {
            loop {
                match read_msg(&mut recv).await {
                    Ok(WireMsg::Cancel { .. }) => {
                        cancel_watch.cancel();
                        break;
                    }
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
        });
        watch_paths(connection.clone(), self.inner.clone(), session_id);
        self.inner.remember_live_paths(&connection).await;
        info!(
            %session_id,
            peer = %remote,
            files = files.len(),
            "offer accepted, pulling blobs"
        );

        let file_count = files.len();
        let mut last_saved = None;
        for file in files {
            if token.is_cancelled() {
                break;
            }
            match self.receive_file(session_id, remote, file).await {
                Ok(dest) => last_saved = Some(dest),
                Err(e) => {
                    if token.is_cancelled() || is_cancel_err(&e) {
                        break;
                    }
                    let _ = self.inner.events.send(NodeEvent::TransferFailed {
                        session_id: session_id.to_string(),
                        error: e.to_string(),
                    });
                    self.inner.finish_session_streams(session_id).await;
                    return Err(e);
                }
            }
        }
        if token.is_cancelled() {
            self.inner.finish_session_streams(session_id).await;
            let _ = self.inner.events.send(NodeEvent::TransferFailed {
                session_id: session_id.to_string(),
                error: "cancelled".into(),
            });
            return Ok(());
        }
        if last_saved.is_none() && file_count > 0 {
            self.inner.finish_session_streams(session_id).await;
            let _ = self.inner.events.send(NodeEvent::TransferFailed {
                session_id: session_id.to_string(),
                error: "download incomplete".into(),
            });
            return Ok(());
        }
        let total = self
            .inner
            .sessions
            .lock()
            .await
            .get(&session_id)
            .map(|s| s.bytes_total.max(s.bytes_done))
            .unwrap_or(0);
        if total > 0 {
            self.inner
                .tell_sender_progress(session_id, total, total)
                .await;
        }
        self.inner.tell_sender_complete(session_id).await;
        let _ = self.inner.events.send(NodeEvent::TransferComplete {
            session_id: session_id.to_string(),
            saved_to: last_saved,
        });
        Ok(())
    }

    async fn receive_file(
        &self,
        session_id: Uuid,
        remote: EndpointId,
        file: FileOffer,
    ) -> Result<String> {
        let hash: Hash = file.hash.parse().context("blob hash")?;
        let dest = unique_dest(&self.inner.config.receive_dir, &file.name)?;
        tokio::fs::create_dir_all(&self.inner.config.receive_dir).await?;
        self.inner.touch_session(session_id, file.size).await;
        self.inner.refresh_peer_lookup(&remote).await;
        info!(
            %session_id,
            name = %file.name,
            bytes = file.size,
            hash = %file.hash,
            "pulling blob"
        );
        self.inner
            .emit_progress(session_id, 0, file.size, TransferPath::Unknown, true)
            .await;

        let token = self.inner.session_token(session_id).await;
        let started = Instant::now();
        let mut backoff = Duration::from_millis(400);
        loop {
            if token.is_cancelled() {
                bail!("cancelled");
            }
            match self.download_once(session_id, remote, hash, file.size).await {
                Ok(()) => {
                    let mut export_backoff = Duration::from_millis(400);
                    let mut export_err = None;
                    for _ in 0..5 {
                        if !self.blob_fully_present(hash, file.size).await {
                            break;
                        }
                        match export_blob(&self.inner.store, hash, &dest).await {
                            Ok(()) => return Ok(dest.display().to_string()),
                            Err(e) => {
                                warn!(%session_id, "export blob failed ({e:#}); retrying");
                                export_err = Some(e);
                                tokio::select! {
                                    _ = token.cancelled() => bail!("cancelled"),
                                    _ = tokio::time::sleep(export_backoff) => {}
                                }
                                export_backoff = next_download_backoff(export_backoff);
                            }
                        }
                    }
                    if self.blob_fully_present(hash, file.size).await {
                        if let Some(e) = export_err {
                            bail!("{e:#}");
                        }
                    }
                    warn!(%session_id, "blob not complete after pull; resuming download");
                }
                Err(e) if token.is_cancelled() || is_cancel_err(&e) => {
                    bail!("cancelled");
                }
                Err(e) if started.elapsed() >= DOWNLOAD_DEADLINE => {
                    bail!("download timed out after {:?}: {e:#}", DOWNLOAD_DEADLINE);
                }
                Err(e) => {
                    warn!(
                        %session_id,
                        "download interrupted ({e:#}); retrying in {backoff:?}"
                    );
                }
            }
            if started.elapsed() >= DOWNLOAD_DEADLINE {
                bail!("download timed out after {:?}", DOWNLOAD_DEADLINE);
            }
            self.inner.refresh_peer_lookup(&remote).await;
            tokio::select! {
                _ = token.cancelled() => bail!("cancelled"),
                _ = tokio::time::sleep(backoff) => {}
            }
            backoff = next_download_backoff(backoff);
        }
    }

    async fn download_once(
        &self,
        session_id: Uuid,
        remote: EndpointId,
        hash: Hash,
        total: u64,
    ) -> Result<()> {
        if let Some(conn) = self.inner.offer_conn.lock().await.get(&session_id) {
            self.inner.remember_live_paths(conn).await;
        }
        self.inner.refresh_peer_lookup(&remote).await;
        let lock = self.inner.blob_pull_lock(hash).await;
        let _guard = lock.lock().await;
        if self.blob_fully_present(hash, total).await {
            return Ok(());
        }
        self.pull_blobs_alpn(session_id, remote, hash, total).await
    }

    async fn pull_blobs_alpn(
        &self,
        session_id: Uuid,
        remote: EndpointId,
        hash: Hash,
        total: u64,
    ) -> Result<()> {
        let token = self.inner.session_token(session_id).await;
        let already = match self.inner.store.blobs().status(hash).await {
            Ok(BlobStatus::Complete { size }) if total == 0 || size == total => return Ok(()),
            Ok(BlobStatus::Complete { size }) => size,
            Ok(BlobStatus::Partial { size }) => size.unwrap_or(0),
            _ => 0,
        };
        if already > 0 {
            self.inner
                .emit_progress(session_id, already, total, TransferPath::Unknown, true)
                .await;
            self.inner
                .tell_sender_progress(session_id, already, total)
                .await;
        }
        let addr = self.inner.blobs_addr(session_id, remote).await;
        info!(
            %session_id,
            peer = %remote,
            addrs = ?addr.addrs,
            have = already,
            "connecting blobs ALPN"
        );
        let conn = tokio::time::timeout(
            Duration::from_secs(20),
            self.inner.endpoint.connect(addr, iroh_blobs::ALPN),
        )
        .await
        .context("blobs connect timeout")?
        .map_err(|e| anyhow::anyhow!("blobs connect: {e:#}"))?;
        self.inner.remember_live_paths(&conn).await;
        let store = self.inner.store.clone();
        let (prog_tx, mut prog_rx) = watch::channel(already);
        let (done_tx, done_rx) = oneshot::channel();
        tokio::spawn(async move {
            let progress = store.remote().fetch(conn, hash);
            let stream = progress.stream();
            futures_util::pin_mut!(stream);
            let outcome = loop {
                match stream.next().await {
                    Some(GetProgressItem::Progress(n)) => {
                        let _ = prog_tx.send(already.saturating_add(n));
                    }
                    Some(GetProgressItem::Done(_)) => break Ok(()),
                    Some(GetProgressItem::Error(e)) => {
                        break Err(anyhow::anyhow!("blobs get: {e:#}"));
                    }
                    None => break Err(anyhow::anyhow!("blobs ALPN stream ended incomplete")),
                }
            };
            let _ = done_tx.send(outcome);
        });
        let mut last_prog = Instant::now();
        let mut done_rx = done_rx;
        loop {
            tokio::select! {
                _ = token.cancelled() => bail!("cancelled"),
                _ = tokio::time::sleep(DOWNLOAD_IDLE.saturating_sub(last_prog.elapsed()).max(Duration::from_millis(200))) => {
                    if last_prog.elapsed() < DOWNLOAD_IDLE {
                        continue;
                    }
                    if self.blob_fully_present(hash, total).await {
                        return Ok(());
                    }
                    bail!("blobs ALPN stalled (no progress for {DOWNLOAD_IDLE:?})");
                }
                outcome = &mut done_rx => {
                    outcome.context("blob get task")??;
                    if self.blob_fully_present(hash, total).await {
                        return Ok(());
                    }
                    anyhow::bail!("blobs ALPN finished but blob missing");
                }
                Ok(()) = prog_rx.changed() => {
                    last_prog = Instant::now();
                    let n = *prog_rx.borrow_and_update();
                    self.inner
                        .emit_progress(session_id, n.min(total.max(n)), total, TransferPath::Unknown, false)
                        .await;
                    self.inner.tell_sender_progress(session_id, n, total).await;
                }
            }
        }
    }

    async fn blob_fully_present(&self, hash: Hash, expected: u64) -> bool {
        match self.inner.store.blobs().status(hash).await {
            Ok(BlobStatus::Complete { size }) => expected == 0 || size == expected,
            _ => false,
        }
    }
}

impl Node {
    pub async fn start(config: NodeConfig) -> Result<Self> {
        tokio::fs::create_dir_all(&config.data_dir).await?;
        tokio::fs::create_dir_all(&config.receive_dir).await?;
        let blobs_dir = config.data_dir.join("blobs");
        tokio::fs::create_dir_all(&blobs_dir).await?;

        let secret = persistent_secret(&config.data_dir)?;
        let memory_lookup = MemoryLookup::new();
        let mut builder = if config.use_relays {
            Endpoint::builder(iroh::endpoint::presets::N0)
        } else {
            Endpoint::builder(iroh::endpoint::presets::Minimal)
        }
        .secret_key(secret)
        .address_lookup(memory_lookup.clone());
        if let Ok(ud) = config.alias.parse() {
            builder = builder.user_data_for_address_lookup(ud);
        }
        let endpoint = builder.bind().await.context("bind iroh endpoint")?;

        let store = FsStore::load(&blobs_dir)
            .await
            .context("open blob store")?;

        let presence = Presence {
            alias: config.alias.clone(),
            device_kind: config.device_kind,
            endpoint_id: endpoint.id().to_string(),
            pin_required: config.pin.is_some(),
            addrs: Vec::new(),
        };
        let port = if config.discovery_port == 0 {
            DISCOVERY_PORT
        } else {
            config.discovery_port
        };
        let discovery = Discovery::new(presence, port);
        for _ in 0..40 {
            let addrs = ip_hint_list(&endpoint);
            discovery.set_direct_addrs(addrs.clone()).await;
            if !addrs.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        if config.enable_lan_beacon {
            discovery.spawn(config.enable_subnet_scan);
        }

        if config.enable_mdns {
            if let Ok(mdns) = MdnsAddressLookup::builder().build(endpoint.id()) {
                if let Ok(services) = endpoint.address_lookup() {
                    services.add(mdns.clone());
                }
                let d = discovery.clone();
                tokio::spawn(async move {
                    watch_mdns(mdns, d).await;
                });
            }
        }

        let (events, _) = broadcast::channel(4096);
        let inner = Arc::new(NodeInner {
            endpoint: endpoint.clone(),
            store: store.clone(),
            discovery: discovery.clone(),
            config,
            events,
            pending_offers: Mutex::new(HashMap::new()),
            pending_metered: Mutex::new(HashMap::new()),
            network_hint: RwLock::new(NetworkHint::Wifi),
            memory_lookup,
            known: Mutex::new(HashMap::new()),
            sessions: Mutex::new(HashMap::new()),
            offer_send: Mutex::new(HashMap::new()),
            offer_conn: Mutex::new(HashMap::new()),
            blob_sessions: Mutex::new(HashMap::new()),
            offer_reopen: Mutex::new(()),
            blob_pulls: Mutex::new(HashMap::new()),
        });

        // mDNS peer names get merged via discovery upsert from a task below
        let disc = discovery.clone();
        let inner_mdns = inner.clone();
        tokio::spawn(async move {
            let mut last_fp: Vec<String> = Vec::new();
            loop {
                tokio::time::sleep(Duration::from_secs(2)).await;
                disc.set_direct_addrs(ip_hint_list(&inner_mdns.endpoint))
                    .await;
                let peers = disc.peers().await;
                let mut fp: Vec<String> = peers.iter().map(|p| p.endpoint_id.clone()).collect();
                fp.sort();
                if fp != last_fp {
                    last_fp = fp;
                    let _ = inner_mdns
                        .events
                        .send(NodeEvent::NearbyUpdated { peers: peers.clone() });
                }
                let mut known = inner_mdns.known.lock().await;
                known.retain(|id, _| peers.iter().any(|p| p.endpoint_id == *id));
                for p in peers {
                    inner_mdns.remember_peer(&p).await;
                    known.insert(p.endpoint_id.clone(), p);
                }
            }
        });

        let blobs = iroh_blobs::BlobsProtocol::new(&store, None);
        let handler = OfferHandler {
            inner: inner.clone(),
        };
        let router = Router::builder(endpoint)
            .accept(iroh_blobs::ALPN, blobs)
            .accept(OFFER_ALPN, handler)
            .spawn();

        info!("omashare listening as {}", inner.endpoint.id());

        Ok(Self { inner, router })
    }

    pub fn subscribe(&self) -> broadcast::Receiver<NodeEvent> {
        self.inner.events.subscribe()
    }

    pub fn endpoint_id(&self) -> String {
        self.inner.endpoint.id().to_string()
    }

    pub fn endpoint_addr(&self) -> EndpointAddr {
        self.inner.endpoint.addr()
    }

    pub fn alias(&self) -> &str {
        &self.inner.config.alias
    }

    pub fn receive_dir(&self) -> &std::path::Path {
        &self.inner.config.receive_dir
    }

    pub async fn nearby(&self) -> Vec<NearbyPeer> {
        self.inner.discovery.peers().await
    }

    /// Android NsdManager / extra discovery feeds peers here.
    pub async fn inject_peer(&self, peer: NearbyPeer) {
        self.inner.discovery.upsert(peer.clone()).await;
        self.inner.remember_peer(&peer).await;
    }

    pub async fn set_network_hint(&self, hint: NetworkHint) {
        *self.inner.network_hint.write().await = hint;
    }

    /// Rebind LAN multicast and scan now (call after Wi-Fi returns).
    pub async fn refresh_lan(&self) {
        self.inner
            .discovery
            .set_direct_addrs(ip_hint_list(&self.inner.endpoint))
            .await;
        self.inner.discovery.refresh().await;
    }

    pub async fn accept_offer(&self, session_id: &str) -> Result<()> {
        self.resolve_offer(session_id, true).await
    }

    pub async fn decline_offer(&self, session_id: &str) -> Result<()> {
        self.resolve_offer(session_id, false).await
    }

    async fn resolve_offer(&self, session_id: &str, accept: bool) -> Result<()> {
        let id: Uuid = session_id
            .trim()
            .parse()
            .context("session id (use the OFFER uuid, not a path line)")?;
        let Some(tx) = self.inner.pending_offers.lock().await.remove(&id) else {
            anyhow::bail!("no pending offer {id} (expired or already answered)");
        };
        let _ = tx.send(accept);
        Ok(())
    }

    pub async fn consent_metered(&self, session_id: &str, allow: bool) -> Result<()> {
        let id: Uuid = session_id.parse()?;
        if let Some(tx) = self.inner.pending_metered.lock().await.remove(&id) {
            let _ = tx.send(allow);
        }
        Ok(())
    }

    /// Abort an in-flight send or receive. `None` / empty cancels every open session.
    pub async fn cancel_transfer(&self, session_id: Option<&str>) -> Result<()> {
        let want = match session_id.map(str::trim).filter(|s| !s.is_empty()) {
            None => None,
            Some(s) => Some(self.resolve_session_id(s).await?),
        };
        let mut ids: Vec<Uuid> = Vec::new();
        if let Some(id) = want {
            ids.push(id);
        } else {
            ids.extend(self.inner.sessions.lock().await.keys().copied());
            ids.extend(self.inner.pending_offers.lock().await.keys().copied());
        }
        ids.sort();
        ids.dedup();
        if ids.is_empty() {
            anyhow::bail!("no transfer in progress");
        }
        for id in ids {
            self.cancel_one(id).await;
        }
        Ok(())
    }

    async fn resolve_session_id(&self, s: &str) -> Result<Uuid> {
        if let Ok(id) = s.parse::<Uuid>() {
            return Ok(id);
        }
        let sessions = self.inner.sessions.lock().await;
        let pending = self.inner.pending_offers.lock().await;
        let matches: Vec<Uuid> = sessions
            .keys()
            .chain(pending.keys())
            .copied()
            .filter(|id| id.to_string().starts_with(s))
            .collect();
        match matches.as_slice() {
            [] => anyhow::bail!("no transfer matching {s}"),
            [id] => Ok(*id),
            _ => anyhow::bail!("ambiguous session prefix {s}"),
        }
    }

    async fn cancel_one(&self, session_id: Uuid) {
        if let Some(tx) = self.inner.pending_offers.lock().await.remove(&session_id) {
            let _ = tx.send(false);
        }
        let token = self.inner.session_token(session_id).await;
        token.cancel();
        if let Some(mut send) = self.inner.offer_send.lock().await.remove(&session_id) {
            let _ = write_msg(&mut send, &WireMsg::Cancel { session_id }).await;
        }
        if let Some(conn) = self.inner.offer_conn.lock().await.remove(&session_id) {
            conn.close(0u32.into(), b"cancelled");
        }
        self.inner.finish_session_streams(session_id).await;
        let _ = self.inner.events.send(NodeEvent::TransferFailed {
            session_id: session_id.to_string(),
            error: "cancelled".into(),
        });
    }

    pub async fn send_files(&self, peer_id: &str, paths: &[PathBuf]) -> Result<Uuid> {
        self.send_files_with_pin(peer_id, paths, None).await
    }

    /// Send using a PIN the receiver published as required.
    pub async fn send_files_with_pin(
        &self,
        peer_id: &str,
        paths: &[PathBuf],
        pin: Option<String>,
    ) -> Result<Uuid> {
        // store pin on config clone is awkward; include in offer by temporarily wrapping
        let peer_eid: EndpointId = peer_id.parse().context("endpoint id")?;
        let mut hints = Vec::new();
        if let Some(p) = self
            .inner
            .discovery
            .peers()
            .await
            .into_iter()
            .find(|p| p.endpoint_id == peer_id)
        {
            hints.extend(p.addrs);
        }
        if hints.is_empty() {
            if let Some(p) = self.inner.known.lock().await.get(peer_id) {
                hints.extend(p.addrs.iter().cloned());
            }
        }
        let addr = self.inner.peer_addr(peer_eid, &hints);
        if addr.addrs.is_empty() {
            anyhow::bail!(
                "no address for {peer_id} yet; keep both on the same Wi-Fi and wait a few seconds"
            );
        }
        self.inner.set_peer_lookup(peer_eid, &hints).await;
        let session_id = Uuid::new_v4();
        self.inner.touch_session(session_id, 0).await;
        let token = self.inner.session_token(session_id).await;
        let mut files = Vec::new();
        for path in paths {
            if token.is_cancelled() {
                anyhow::bail!("cancelled");
            }
            let tag = import_path(&self.inner.store, path).await?;
            let meta = tokio::fs::metadata(path).await.with_context(|| {
                format!("stat {} after import (file vanished?)", path.display())
            })?;
            files.push(FileOffer {
                name: path
                    .file_name()
                    .unwrap_or_else(|| std::ffi::OsStr::new("file"))
                    .to_string_lossy()
                    .into(),
                size: meta.len(),
                hash: tag.to_string(),
            });
        }
        if token.is_cancelled() {
            anyhow::bail!("cancelled");
        }
        let bytes_total: u64 = files.iter().map(|f| f.size).sum();
        self.inner.touch_session(session_id, bytes_total).await;
        self.inner.register_send_blobs(session_id, &files).await;
        self.inner.set_session_peer(session_id, peer_eid).await;
        info!(
            peer = %peer_id,
            addrs = ?addr.addrs,
            "connecting to send offer {session_id}"
        );
        let conn = connect_offer(&self.inner, peer_eid, peer_id, addr, &token).await?;
        if token.is_cancelled() {
            conn.close(0u32.into(), b"cancelled");
            anyhow::bail!("cancelled");
        }
        let (mut send, mut recv) = conn.open_bi().await.context("open stream")?;
        write_msg(
            &mut send,
            &WireMsg::Offer {
                session_id,
                alias: self.inner.config.alias.clone(),
                files: files.clone(),
                pin,
                metered_policy: self.inner.config.metered_policy,
            },
        )
        .await?;
        self.inner.offer_send.lock().await.insert(session_id, send);
        self.inner
            .offer_conn
            .lock()
            .await
            .insert(session_id, conn.clone());
        self.inner
            .emit_progress(session_id, 0, bytes_total, TransferPath::Unknown, true)
            .await;
        let events = self.inner.events.clone();
        let inner = self.inner.clone();
        tokio::spawn(async move {
            let outcome = read_msg(&mut recv).await;
            match outcome {
                Ok(WireMsg::Accept { .. }) => {
                    info!("offer {session_id} accepted; serving blobs");
                    watch_paths(conn.clone(), inner.clone(), session_id);
                    inner.remember_live_paths(&conn).await;
                    let _ = events.send(NodeEvent::OfferAccepted {
                        session_id: session_id.to_string(),
                    });
                    loop {
                        match read_msg(&mut recv).await {
                            Ok(msg) => {
                                if inner.apply_outbound_wire(session_id, msg).await {
                                    break;
                                }
                            }
                            Err(_) => {
                                info!(
                                    "offer {session_id} stream dropped; keeping blobs until receiver reconnects"
                                );
                                break;
                            }
                        }
                    }
                }
                Ok(WireMsg::Decline { reason, .. }) => {
                    inner.finish_session_streams(session_id).await;
                    let _ = events.send(NodeEvent::TransferFailed {
                        session_id: session_id.to_string(),
                        error: format!("declined: {reason}"),
                    });
                }
                Ok(WireMsg::Cancel { .. }) => {
                    inner.finish_session_streams(session_id).await;
                    let _ = events.send(NodeEvent::TransferFailed {
                        session_id: session_id.to_string(),
                        error: "cancelled".into(),
                    });
                }
                Ok(other) => {
                    inner.finish_session_streams(session_id).await;
                    let _ = events.send(NodeEvent::TransferFailed {
                        session_id: session_id.to_string(),
                        error: format!("unexpected reply: {other:?}"),
                    });
                }
                Err(e) => {
                    let already = token.is_cancelled();
                    inner.finish_session_streams(session_id).await;
                    if !already {
                        let _ = events.send(NodeEvent::TransferFailed {
                            session_id: session_id.to_string(),
                            error: format!("waiting for accept: {e:#}"),
                        });
                    }
                }
            }
        });
        Ok(session_id)
    }

    pub async fn shutdown(self) -> Result<()> {
        self.router.shutdown().await?;
        Ok(())
    }
}

async fn connect_offer(
    inner: &NodeInner,
    peer_eid: EndpointId,
    peer_id: &str,
    mut addr: EndpointAddr,
    token: &CancellationToken,
) -> Result<Connection> {
    let mut last = anyhow::anyhow!("connect failed");
    for attempt in 1..=4u32 {
        if token.is_cancelled() {
            anyhow::bail!("cancelled");
        }
        inner.memory_lookup.remove_endpoint_info(peer_eid);
        inner.memory_lookup.add_endpoint_info(addr.clone());
        let connect = inner.endpoint.connect(addr.clone(), OFFER_ALPN);
        match tokio::time::timeout(
            Duration::from_secs(25),
            async {
                tokio::select! {
                    _ = token.cancelled() => anyhow::bail!("cancelled"),
                    r = connect => r.map_err(|e| anyhow::anyhow!("{e:#}")),
                }
            },
        )
        .await
        {
            Ok(Ok(conn)) => return Ok(conn),
            Ok(Err(e)) if is_cancel_err(&e) => anyhow::bail!("cancelled"),
            Ok(Err(e)) => {
                last = anyhow::anyhow!("connect attempt {attempt}: {e:#}");
                warn!("{last:#}");
            }
            Err(_) => {
                last = anyhow::anyhow!(
                    "timed out connecting to peer after 25s (attempt {attempt}/4)"
                );
                warn!("{last:#}");
            }
        }
        let mut hints = Vec::new();
        if let Some(p) = inner
            .discovery
            .peers()
            .await
            .into_iter()
            .find(|p| p.endpoint_id == peer_id)
        {
            hints.extend(p.addrs);
        }
        addr = inner.peer_addr(peer_eid, &hints);
        tokio::select! {
            _ = token.cancelled() => anyhow::bail!("cancelled"),
            _ = tokio::time::sleep(Duration::from_millis(500)) => {}
        }
    }
    Err(last.context("could not reach peer over LAN or relay"))
}

async fn watch_mdns(mdns: MdnsAddressLookup, discovery: Discovery) {
    let mut events = mdns.subscribe().await;
    while let Some(ev) = events.next().await {
        match ev {
            DiscoveryEvent::Discovered { endpoint_info, .. } => {
                let id = endpoint_info.endpoint_id.to_string();
                let alias = endpoint_info
                    .data
                    .user_data()
                    .map(|u| u.to_string())
                    .filter(|s| !s.is_empty())
                    .unwrap_or_else(|| {
                        format!("device-{}", id.get(..8).unwrap_or(id.as_str()))
                    });
                let addrs = endpoint_info
                    .data
                    .addrs()
                    .filter_map(|a| match a {
                        TransportAddr::Ip(sa) => Some(sa.to_string()),
                        TransportAddr::Relay(url) => Some(url.to_string()),
                        _ => None,
                    })
                    .collect();
                discovery
                    .upsert(NearbyPeer {
                        alias,
                        device_kind: crate::protocol::DeviceKind::Unknown,
                        endpoint_id: id,
                        pin_required: false,
                        source: crate::events::DiscoverySource::Mdns,
                        addrs,
                    })
                    .await;
            }
            DiscoveryEvent::Expired { .. } => {}
            _ => {}
        }
    }
}

fn watch_paths(conn: Connection, inner: Arc<NodeInner>, session_id: Uuid) {
    tokio::spawn(async move {
        let mut events = conn.path_events();
        while let Some(ev) = events.next().await {
            if let PathEvent::Selected { remote_addr, .. } = ev {
                let path = classify_transport(&remote_addr);
                let snap = inner.set_session_path(session_id, path).await;
                let _ = inner.events.send(NodeEvent::PathChanged {
                    session_id: session_id.to_string(),
                    path,
                });
                if snap.bytes_total > 0 && snap.bytes_done > 0 {
                    inner
                        .emit_progress(
                            session_id,
                            snap.bytes_done,
                            snap.bytes_total,
                            path,
                            true,
                        )
                        .await;
                }
            }
        }
    });
}

fn persistent_secret(data_dir: &Path) -> Result<SecretKey> {
    let path = data_dir.join("endpoint.key");
    if let Ok(bytes) = std::fs::read(&path) {
        if let Ok(arr) = <[u8; 32]>::try_from(bytes.as_slice()) {
            return Ok(SecretKey::from_bytes(&arr));
        }
    }
    let key = SecretKey::generate();
    std::fs::write(&path, key.to_bytes()).context("write endpoint.key")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
    }
    Ok(key)
}

fn unique_dest(dir: &Path, name: &str) -> Result<PathBuf> {
    let safe = Path::new(name)
        .file_name()
        .unwrap_or_else(|| std::ffi::OsStr::new("file"));
    let dest = dir.join(safe);
    if !dest.exists() {
        return Ok(dest);
    }
    let stem = dest.file_stem().unwrap_or_default().to_string_lossy().into_owned();
    let ext = dest
        .extension()
        .map(|e| format!(".{}", e.to_string_lossy()))
        .unwrap_or_default();
    for i in 1..1000 {
        let candidate = dir.join(format!("{stem}-{i}{ext}"));
        if !candidate.exists() {
            return Ok(candidate);
        }
    }
    bail!("could not pick destination name");
}

fn is_cancel_err(e: &anyhow::Error) -> bool {
    format!("{e:#}").to_ascii_lowercase().contains("cancelled")
}

async fn import_path(store: &FsStore, path: &Path) -> Result<Hash> {
    let abs = tokio::fs::canonicalize(path)
        .await
        .with_context(|| format!("cannot read {}", path.display()))?;
    let meta = tokio::fs::metadata(&abs).await?;
    info!(path = %abs.display(), bytes = meta.len(), "importing blob");
    match store.blobs().add_path(&abs).await {
        Ok(tag) => {
            info!(hash = %tag.hash, "import done");
            Ok(tag.hash)
        }
        Err(e) => {
            let msg = format!("{e:#}");
            if msg.contains("No space") || msg.contains("ENOSPC") || msg.contains("storage") {
                bail!(
                    "not enough storage to hash {} ({} bytes). Clear Omashare app storage and retry.",
                    abs.display(),
                    meta.len()
                );
            }
            bail!("hashing {}: {msg}", abs.display());
        }
    }
}

async fn export_blob(store: &FsStore, hash: Hash, dest: &Path) -> Result<()> {
    if let Some(parent) = dest.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    store
        .blobs()
        .export(hash, dest)
        .await
        .map_err(|e| anyhow::anyhow!("export blob: {e}"))?;
    Ok(())
}
