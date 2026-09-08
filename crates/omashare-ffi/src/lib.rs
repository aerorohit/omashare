use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;

use omashare_core::events::{DiscoverySource, NearbyPeer, NodeConfig};
use omashare_core::protocol::{DeviceKind, MeteredPolicy, NetworkHint};
use omashare_core::{Node, NodeEvent};

uniffi::setup_scaffolding!("omashare");

#[uniffi::export(callback_interface)]
pub trait EventSink: Send + Sync {
    fn on_event_json(&self, json: String);
}

#[derive(uniffi::Object)]
pub struct OmashareNode {
    rt: tokio::runtime::Runtime,
    node: Mutex<Option<Arc<Node>>>,
}

#[uniffi::export]
impl OmashareNode {
    #[uniffi::constructor]
    pub fn new() -> Arc<Self> {
        #[cfg(target_os = "android")]
        {
            let _ = android_logger::init_once(
                android_logger::Config::default()
                    .with_max_level(log::LevelFilter::Info)
                    .with_tag("omashare"),
            );
            let _ = tracing_subscriber::fmt()
                .with_ansi(false)
                .with_target(true)
                .with_writer(LogcatWriter)
                .with_max_level(tracing::Level::INFO)
                .try_init();
        }
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("tokio");
        Arc::new(Self {
            rt,
            node: Mutex::new(None),
        })
    }

    pub fn start(
        self: Arc<Self>,
        alias: String,
        pin: Option<String>,
        receive_dir: String,
        data_dir: String,
        device_kind: String,
        use_relays: bool,
        listener: Box<dyn EventSink>,
    ) -> Result<(), OmashareError> {
        let kind = match device_kind.as_str() {
            "mobile" => DeviceKind::Mobile,
            "headless" => DeviceKind::Headless,
            _ => DeviceKind::Desktop,
        };
        let config = NodeConfig {
            alias,
            device_kind: kind,
            pin: pin.filter(|s| !s.is_empty()),
            receive_dir: PathBuf::from(receive_dir),
            data_dir: PathBuf::from(data_dir),
            discovery_port: omashare_core::DISCOVERY_PORT,
            enable_mdns: cfg!(not(target_os = "android")),
            enable_subnet_scan: true,
            enable_lan_beacon: true,
            metered_policy: MeteredPolicy::Allow,
            use_relays,
        };
        let node = self
            .rt
            .block_on(Node::start(config))
            .map_err(|e| OmashareError::Start(e.to_string()))?;
        let node = Arc::new(node);
        let mut ev = node.subscribe();
        thread::spawn(move || {
            loop {
                match ev.blocking_recv() {
                    Ok(event) => {
                        if let Ok(json) = serde_json::to_string(&event) {
                            listener.on_event_json(json);
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("event listener lagged, skipped {n}");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });
        *self.node.lock().unwrap() = Some(node);
        Ok(())
    }

    pub fn endpoint_id(&self) -> String {
        self.with_node(|n| n.endpoint_id())
            .unwrap_or_default()
    }

    pub fn nearby_json(&self) -> String {
        let peers = self
            .rt
            .block_on(async {
                match self.node.lock().unwrap().clone() {
                    Some(n) => n.nearby().await,
                    None => vec![],
                }
            });
        serde_json::to_string(&peers).unwrap_or_else(|_| "[]".into())
    }

    pub fn inject_peer(
        &self,
        alias: String,
        endpoint_id: String,
        addrs: Vec<String>,
        pin_required: bool,
    ) {
        let peer = NearbyPeer {
            alias,
            device_kind: DeviceKind::Unknown,
            endpoint_id,
            pin_required,
            source: DiscoverySource::NativeDnsSd,
            addrs,
        };
        self.rt.block_on(async {
            if let Some(n) = self.node.lock().unwrap().clone() {
                n.inject_peer(peer).await;
            }
        });
    }

    pub fn send_files(
        &self,
        peer_id: String,
        paths: Vec<String>,
        pin: Option<String>,
    ) -> Result<String, OmashareError> {
        self.rt.block_on(async {
            let n = self
                .node
                .lock()
                .unwrap()
                .clone()
                .ok_or_else(|| OmashareError::Start("not started".into()))?;
            let paths: Vec<PathBuf> = paths.into_iter().map(PathBuf::from).collect();
            n.send_files_with_pin(&peer_id, &paths, pin)
                .await
                .map(|id| id.to_string())
                .map_err(|e| OmashareError::Transfer(format!("{e:#}")))
        })
    }

    pub fn send_file(&self, peer_id: String, path: String, pin: Option<String>) -> Result<String, OmashareError> {
        self.send_files(peer_id, vec![path], pin)
    }

    pub fn accept_offer(&self, session_id: String) -> Result<(), OmashareError> {
        self.call(|n| async move {
            n.accept_offer(&session_id)
                .await
                .map_err(|e| OmashareError::Transfer(e.to_string()))
        })
    }

    pub fn decline_offer(&self, session_id: String) -> Result<(), OmashareError> {
        self.call(|n| async move {
            n.decline_offer(&session_id)
                .await
                .map_err(|e| OmashareError::Transfer(e.to_string()))
        })
    }

    pub fn consent_metered(&self, session_id: String, allow: bool) -> Result<(), OmashareError> {
        self.call(|n| async move {
            n.consent_metered(&session_id, allow)
                .await
                .map_err(|e| OmashareError::Transfer(e.to_string()))
        })
    }

    pub fn cancel_transfer(&self, session_id: Option<String>) -> Result<(), OmashareError> {
        self.call(|n| async move {
            n.cancel_transfer(session_id.as_deref())
                .await
                .map_err(|e| OmashareError::Transfer(e.to_string()))
        })
    }

    pub fn set_network_hint(&self, hint: String) {
        let h = match hint.as_str() {
            "metered" => NetworkHint::Metered,
            "wifi" => NetworkHint::Wifi,
            _ => NetworkHint::Unknown,
        };
        self.rt.block_on(async {
            if let Some(n) = self.node.lock().unwrap().clone() {
                n.set_network_hint(h).await;
            }
        });
    }

    pub fn refresh_lan(&self) {
        self.rt.block_on(async {
            if let Some(n) = self.node.lock().unwrap().clone() {
                n.refresh_lan().await;
            }
        });
    }
}

impl OmashareNode {
    fn with_node<T>(&self, f: impl FnOnce(&Node) -> T) -> Option<T> {
        let g = self.node.lock().unwrap();
        g.as_ref().map(|n| f(n))
    }

    fn call<F, Fut>(&self, f: F) -> Result<(), OmashareError>
    where
        F: FnOnce(Arc<Node>) -> Fut,
        Fut: std::future::Future<Output = Result<(), OmashareError>>,
    {
        self.rt.block_on(async {
            let n = self
                .node
                .lock()
                .unwrap()
                .clone()
                .ok_or_else(|| OmashareError::Start("not started".into()))?;
            f(n).await
        })
    }
}

#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum OmashareError {
    #[error("start: {0}")]
    Start(String),
    #[error("transfer: {0}")]
    Transfer(String),
}

#[cfg(target_os = "android")]
struct LogcatWriter;

#[cfg(target_os = "android")]
impl std::io::Write for LogcatWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        for line in buf.split(|b| *b == b'\n') {
            if line.is_empty() {
                continue;
            }
            if let Ok(s) = std::str::from_utf8(line) {
                log::info!("{s}");
            }
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(target_os = "android")]
impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogcatWriter {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        LogcatWriter
    }
}
