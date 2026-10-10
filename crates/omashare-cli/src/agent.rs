use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use omashare_core::events::NodeConfig;
use omashare_core::{Node, NodeEvent};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Mutex;

use crate::ipc::{resolve_peer_id, socket_path, AgentLine, ClientCmd, ClientKind, ServerMsg};

pub struct AgentOpts {
    pub config: NodeConfig,
    pub auto_accept: bool,
    pub enable_ble: bool,
}

pub async fn run_agent(opts: AgentOpts) -> Result<()> {
    let listener = bind_socket().await?;
    let node = Arc::new(Node::start(opts.config).await.context("start node")?);
    #[cfg(target_os = "linux")]
    let _ble = if opts.enable_ble {
        crate::ble::start(node.clone()).await
    } else {
        None
    };
    #[cfg(not(target_os = "linux"))]
    let _ = opts.enable_ble;
    println!(
        "omashare  {}  {}  auto-accept={}  socket {}",
        node.alias(),
        node.endpoint_id(),
        opts.auto_accept,
        socket_path().display()
    );
    let serve = serve_agent(listener, node.clone(), opts.auto_accept);
    let result = tokio::select! {
        r = serve => r,
        _ = tokio::signal::ctrl_c() => {
            println!("stopping");
            Ok(())
        }
    };
    let _ = std::fs::remove_file(socket_path());
    drop(node);
    result
}

pub async fn spawn_embedded_agent(opts: AgentOpts) -> Result<EmbeddedAgent> {
    let listener = bind_socket().await?;
    let node = Arc::new(Node::start(opts.config).await.context("start node")?);
    #[cfg(target_os = "linux")]
    let ble = if opts.enable_ble {
        crate::ble::start(node.clone()).await
    } else {
        None
    };
    #[cfg(not(target_os = "linux"))]
    let _ = opts.enable_ble;
    let node_stop = node.clone();
    let handle = tokio::spawn(async move {
        let _ = serve_agent(listener, node, opts.auto_accept).await;
    });
    for _ in 0..50 {
        if crate::ipc::agent_is_healthy().await {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Ok(EmbeddedAgent {
        handle,
        node: node_stop,
        #[cfg(target_os = "linux")]
        _ble: ble,
    })
}

pub struct EmbeddedAgent {
    handle: tokio::task::JoinHandle<()>,
    node: Arc<Node>,
    #[cfg(target_os = "linux")]
    _ble: Option<crate::ble::BleSession>,
}

impl EmbeddedAgent {
    pub async fn shutdown(self) {
        self.handle.abort();
        let _ = std::fs::remove_file(socket_path());
        if let Some(n) = Arc::into_inner(self.node) {
            let _ = n.shutdown().await;
        }
    }
}

async fn bind_socket() -> Result<UnixListener> {
    let path = socket_path();
    if path.exists() {
        if crate::ipc::agent_is_healthy().await {
            anyhow::bail!("omashare already running ({})", path.display());
        }
        tracing::warn!("removing stale socket {}", path.display());
        let _ = std::fs::remove_file(&path);
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    UnixListener::bind(&path).with_context(|| format!("bind {}", path.display()))
}

async fn serve_agent(listener: UnixListener, node: Arc<Node>, auto_accept: bool) -> Result<()> {
    let tui_clients = Arc::new(AtomicUsize::new(0));
    let node_ev = node.clone();
    let tui_ev = tui_clients.clone();
    tokio::spawn(async move {
        let mut events = node_ev.subscribe();
        while let Some(ev) = omashare_core::recv_event(&mut events).await {
            handle_agent_event(&node_ev, &tui_ev, auto_accept, &ev).await;
        }
    });

    loop {
        let (stream, _) = listener.accept().await?;
        let node = node.clone();
        let tui_clients = tui_clients.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_client(stream, node, tui_clients, auto_accept).await {
                tracing::debug!("ipc client: {e:#}");
            }
        });
    }
}

async fn handle_agent_event(
    node: &Node,
    tui_clients: &AtomicUsize,
    auto_accept: bool,
    ev: &NodeEvent,
) {
    match ev {
        NodeEvent::IncomingOffer {
            session_id,
            from_alias,
            from_endpoint,
            files,
            ..
        } => {
            let names: Vec<_> = files.iter().map(|f| f.name.as_str()).collect();
            tracing::info!(%session_id, from = %from_alias, "incoming offer");
            if tui_clients.load(Ordering::Relaxed) == 0 {
                let _ = notify_rust::Notification::new()
                    .summary("Omashare")
                    .body(&format!("{from_alias} wants to send {}", names.join(", ")))
                    .show();
            }
            // `--auto-accept` accepts everyone and does not write trusted.json.
            // Otherwise accept only when this endpoint id is in the trust file.
            if auto_accept || crate::trust::is_trusted(from_endpoint) {
                if !auto_accept {
                    tracing::info!(%session_id, %from_endpoint, "auto-accept trusted device");
                }
                if let Err(e) = node.accept_offer(session_id).await {
                    tracing::warn!("auto-accept failed: {e:#}");
                }
            }
        }
        NodeEvent::TransferComplete { .. } => {
            if tui_clients.load(Ordering::Relaxed) == 0 {
                let _ = notify_rust::Notification::new()
                    .summary("Omashare")
                    .body("Transfer complete")
                    .show();
            }
        }
        NodeEvent::TransferFailed { error, .. } => {
            if tui_clients.load(Ordering::Relaxed) == 0 {
                let _ = notify_rust::Notification::new()
                    .summary("Omashare")
                    .body(&format!("Transfer failed: {error}"))
                    .show();
            }
        }
        _ => {}
    }
}

async fn handle_client(
    stream: UnixStream,
    node: Arc<Node>,
    tui_clients: Arc<AtomicUsize>,
    auto_accept: bool,
) -> Result<()> {
    let (reader, writer) = tokio::io::split(stream);
    let mut reader = BufReader::new(reader);
    let writer = Arc::new(Mutex::new(writer));
    let is_tui = Arc::new(AtomicUsize::new(0));

    write_json(
        &writer,
        &ServerMsg::Hello {
            alias: node.alias().to_string(),
            endpoint_id: node.endpoint_id(),
            receive_dir: node.receive_dir().display().to_string(),
        },
    )
    .await?;

    let mut events = node.subscribe();
    let writer_ev = writer.clone();
    let pump = tokio::spawn(async move {
        while let Some(ev) = omashare_core::recv_event(&mut events).await {
            if write_event(&writer_ev, &ev).await.is_err() {
                break;
            }
        }
    });

    let mut line = String::new();
    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            break;
        }
        let cmd: ClientCmd = match serde_json::from_str(line.trim()) {
            Ok(c) => c,
            Err(e) => {
                write_json(
                    &writer,
                    &ServerMsg::Error {
                        error: format!("bad command: {e}"),
                    },
                )
                .await?;
                continue;
            }
        };
        if let ClientCmd::Attach {
            kind: ClientKind::Tui,
        } = cmd
        {
            if is_tui.swap(1, Ordering::Relaxed) == 0 {
                tui_clients.fetch_add(1, Ordering::Relaxed);
            }
            write_json(&writer, &ServerMsg::Ok { session_id: None }).await?;
            continue;
        }
        match dispatch(&node, auto_accept, cmd).await {
            Ok(msg) => write_json(&writer, &msg).await?,
            Err(e) => {
                write_json(
                    &writer,
                    &ServerMsg::Error {
                        error: format!("{e:#}"),
                    },
                )
                .await?;
            }
        }
    }

    if is_tui.load(Ordering::Relaxed) == 1 {
        tui_clients.fetch_sub(1, Ordering::Relaxed);
    }
    pump.abort();
    Ok(())
}

async fn dispatch(node: &Node, auto_accept: bool, cmd: ClientCmd) -> Result<ServerMsg> {
    match cmd {
        ClientCmd::Nearby => Ok(ServerMsg::Nearby {
            peers: node.nearby().await,
        }),
        ClientCmd::Status => Ok(ServerMsg::Status {
            alias: node.alias().to_string(),
            endpoint_id: node.endpoint_id(),
            receive_dir: node.receive_dir().display().to_string(),
            auto_accept,
            listening: true,
        }),
        ClientCmd::Send { paths, to, pin } => {
            let peers = node.nearby().await;
            let target = resolve_peer_id(&peers, to.as_deref())?;
            let files: Vec<PathBuf> = paths.into_iter().map(PathBuf::from).collect();
            let sid = node.send_files_with_pin(&target, &files, pin).await?;
            Ok(ServerMsg::Ok {
                session_id: Some(sid.to_string()),
            })
        }
        ClientCmd::Accept { session_id } => {
            node.accept_offer(&session_id).await?;
            Ok(ServerMsg::Ok {
                session_id: Some(session_id),
            })
        }
        ClientCmd::Decline { session_id } => {
            node.decline_offer(&session_id).await?;
            Ok(ServerMsg::Ok {
                session_id: Some(session_id),
            })
        }
        ClientCmd::Cancel { session_id } => {
            node.cancel_transfer(session_id.as_deref()).await?;
            Ok(ServerMsg::Ok { session_id })
        }
        ClientCmd::Attach { .. } => Ok(ServerMsg::Ok { session_id: None }),
        ClientCmd::TrustList => Ok(ServerMsg::Trusted {
            devices: crate::trust::load_default()?,
        }),
        ClientCmd::TrustAdd { endpoint_id, alias } => {
            crate::trust::add_default(endpoint_id, alias)?;
            Ok(ServerMsg::Ok { session_id: None })
        }
        ClientCmd::TrustRemove { endpoint_id } => {
            crate::trust::remove_default(&endpoint_id)?;
            Ok(ServerMsg::Ok { session_id: None })
        }
    }
}

async fn write_json<W: AsyncWriteExt + Unpin>(writer: &Mutex<W>, msg: &ServerMsg) -> Result<()> {
    let mut line = serde_json::to_string(msg)?;
    line.push('\n');
    let mut w = writer.lock().await;
    w.write_all(line.as_bytes()).await?;
    w.flush().await?;
    Ok(())
}

async fn write_event<W: AsyncWriteExt + Unpin>(writer: &Mutex<W>, ev: &NodeEvent) -> Result<()> {
    let mut line = serde_json::to_string(ev)?;
    line.push('\n');
    let mut w = writer.lock().await;
    w.write_all(line.as_bytes()).await?;
    w.flush().await?;
    Ok(())
}

/// Wait for a `send` command: accept Ok/Error, then TransferComplete/Failed.
pub async fn wait_send(client: &mut crate::ipc::IpcClient) -> Result<()> {
    let mut session_id: Option<String> = None;
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                let _ = client
                    .send_cmd(&ClientCmd::Cancel {
                        session_id: session_id.clone(),
                    })
                    .await;
                anyhow::bail!("cancelled");
            }
            line = client.read_line() => {
                match line? {
                    None => anyhow::bail!("agent disconnected"),
                    Some(AgentLine::Server(ServerMsg::Error { error })) => anyhow::bail!("{error}"),
                    Some(AgentLine::Server(ServerMsg::Ok { session_id: Some(id) })) => {
                        println!("session {id}  (Ctrl-C cancels)");
                        session_id = Some(id);
                    }
                    Some(AgentLine::Event(NodeEvent::TransferComplete { session_id: id, .. })) => {
                        if session_id.as_ref().map(|s| s == &id).unwrap_or(true) {
                            return Ok(());
                        }
                    }
                    Some(AgentLine::Event(NodeEvent::TransferFailed { session_id: id, error })) => {
                        if session_id.as_ref().map(|s| s == &id).unwrap_or(true) {
                            anyhow::bail!("{error}");
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}
