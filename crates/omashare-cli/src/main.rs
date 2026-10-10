mod agent;
mod ipc;
mod picker;
mod trust;
mod tui;

#[cfg(target_os = "linux")]
mod ble;

use std::io::IsTerminal;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use omashare_core::events::NodeConfig;
use omashare_core::protocol::{DeviceKind, MeteredPolicy, DISCOVERY_PORT};
use omashare_core::{Node, NodeEvent, TransferPath};

use crate::ipc::{ClientCmd, IpcClient, ServerMsg};

#[derive(Parser)]
#[command(name = "omashare", about = "AirDrop-like send over Iroh")]
struct Cli {
    #[arg(long)]
    alias: Option<String>,
    #[arg(long)]
    pin: Option<String>,
    #[arg(long)]
    receive_dir: Option<PathBuf>,
    #[arg(long, default_value_t = DISCOVERY_PORT)]
    discovery_port: u16,
    #[arg(long)]
    no_mdns: bool,
    #[arg(long)]
    no_scan: bool,
    #[arg(long)]
    no_ble: bool,
    #[arg(long)]
    offline: bool,
    #[arg(long)]
    verbose: bool,
    /// Auto-accept incoming offers (kiosks / tests). Off by default.
    #[arg(long, global = true)]
    auto_accept: bool,
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// List nearby peers.
    Nearby,
    /// Identity and receive directory.
    Status,
    /// Accept an incoming offer.
    Accept { session_id: String },
    /// Decline an incoming offer.
    Decline { session_id: String },
    /// Cancel a transfer.
    Cancel { session_id: Option<String> },
    /// Trusted devices. These endpoint ids are auto-accepted.
    Trust {
        #[command(subcommand)]
        cmd: TrustCmd,
    },
    /// Send files via the running node, or a one-shot node if it is down.
    Send {
        #[arg(required = true)]
        files: Vec<PathBuf>,
        #[arg(long)]
        to: Option<String>,
        #[arg(long)]
        pin: Option<String>,
    },
}

#[derive(Subcommand)]
enum TrustCmd {
    /// List trusted devices.
    List,
    /// Trust an endpoint id, or exactly one nearby alias.
    Add { endpoint_or_alias: String },
    /// Remove a trusted device by endpoint id or alias.
    Remove { endpoint_or_alias: String },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let tui = cli.command.is_none() && std::io::stdout().is_terminal();
    let log_path = init_tracing(cli.verbose, tui)?;

    let config = node_config(&cli);
    match cli.command {
        Some(Commands::Nearby) => run_agent_cmd(ClientCmd::Nearby).await?,
        Some(Commands::Status) => run_agent_cmd(ClientCmd::Status).await?,
        Some(Commands::Accept { session_id }) => {
            run_agent_cmd(ClientCmd::Accept { session_id }).await?;
        }
        Some(Commands::Decline { session_id }) => {
            run_agent_cmd(ClientCmd::Decline { session_id }).await?;
        }
        Some(Commands::Cancel { session_id }) => {
            run_agent_cmd(ClientCmd::Cancel { session_id }).await?;
        }
        Some(Commands::Trust { cmd }) => run_trust(cmd).await?,
        Some(Commands::Send { files, to, pin }) => run_send(config, files, to, pin).await?,
        None => {
            if tui {
                run_tui(config, cli.auto_accept, !cli.no_ble).await?;
                if let Some(path) = log_path {
                    eprintln!("logs {}", path.display());
                }
            } else {
                agent::run_agent(agent::AgentOpts {
                    config,
                    auto_accept: cli.auto_accept,
                    enable_ble: !cli.no_ble,
                })
                .await?;
            }
        }
    }
    Ok(())
}

/// TUI sessions log to `$XDG_DATA_HOME/omashare/omashare.log` so tracing
/// does not paint over the alternate screen. Headless runs stay on stderr.
fn init_tracing(verbose: bool, to_file: bool) -> Result<Option<PathBuf>> {
    let directive = if verbose { "omashare=debug" } else { "omashare=info" };
    let filter = tracing_subscriber::EnvFilter::from_default_env()
        .add_directive(directive.parse().context("log filter")?);
    if !to_file {
        tracing_subscriber::fmt().with_env_filter(filter).init();
        return Ok(None);
    }
    let path = trust::data_dir().join("omashare.log");
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("open log {}", path.display()))?;
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_ansi(false)
        .with_writer(std::sync::Mutex::new(file))
        .init();
    Ok(Some(path))
}

fn node_config(cli: &Cli) -> NodeConfig {
    let data = trust::data_dir();
    let receive = cli
        .receive_dir
        .clone()
        .or_else(|| dirs::download_dir().map(|d| d.join("omashare")))
        .unwrap_or_else(|| data.join("inbox"));
    NodeConfig {
        alias: cli.alias.clone().unwrap_or_else(NodeConfig::default_alias),
        device_kind: DeviceKind::Desktop,
        pin: cli.pin.clone(),
        receive_dir: receive,
        data_dir: data,
        discovery_port: cli.discovery_port,
        enable_mdns: !cli.no_mdns,
        enable_subnet_scan: !cli.no_scan,
        enable_lan_beacon: true,
        metered_policy: MeteredPolicy::Allow,
        use_relays: !cli.offline,
    }
}

async fn run_agent_cmd(cmd: ClientCmd) -> Result<()> {
    let mut client = IpcClient::connect()
        .await
        .context("omashare is not running (start `omashare`)")?;
    let msg = client.request(cmd).await?;
    match msg {
        ServerMsg::Nearby { peers } => {
            if peers.is_empty() {
                println!("(no nearby peers)");
            }
            for p in peers {
                println!(
                    "{}  {}  {:?}  pin={}",
                    p.alias, p.endpoint_id, p.device_kind, p.pin_required
                );
            }
        }
        ServerMsg::Status {
            alias,
            endpoint_id,
            receive_dir,
            auto_accept,
            listening,
        } => {
            println!("alias {alias}");
            println!("id {endpoint_id}");
            println!("receive {receive_dir}");
            println!("auto-accept {auto_accept}");
            println!("listening {listening}");
        }
        ServerMsg::Ok { session_id } => {
            if let Some(id) = session_id {
                println!("{id}");
            }
        }
        ServerMsg::Error { error } => anyhow::bail!("{error}"),
        other => anyhow::bail!("unexpected reply: {other:?}"),
    }
    Ok(())
}

async fn run_trust(cmd: TrustCmd) -> Result<()> {
    match cmd {
        TrustCmd::List => {
            let devices = if let Some(mut client) = ipc::connect_if_running().await? {
                trust_list_ipc(&mut client).await?
            } else {
                trust::load_default()?
            };
            print_trusted(&devices);
        }
        TrustCmd::Add { endpoint_or_alias } => {
            if let Some(mut client) = ipc::connect_if_running().await? {
                let peers = client.nearby().await?;
                let (endpoint_id, alias) = trust::resolve_add(&peers, &endpoint_or_alias)?;
                expect_ok(
                    client
                        .request(ClientCmd::TrustAdd {
                            endpoint_id: endpoint_id.clone(),
                            alias: alias.clone(),
                        })
                        .await?,
                )?;
                println!("trusted {alias}  {endpoint_id}");
            } else {
                let (endpoint_id, alias) = trust::resolve_add(&[], &endpoint_or_alias)
                    .context("omashare is not running; pass an endpoint id, or start `omashare` to trust by alias")?;
                trust::add_default(endpoint_id.clone(), alias.clone())?;
                println!("trusted {alias}  {endpoint_id}");
            }
        }
        TrustCmd::Remove { endpoint_or_alias } => {
            if let Some(mut client) = ipc::connect_if_running().await? {
                let devices = trust_list_ipc(&mut client).await?;
                let endpoint_id = trust::resolve_remove(&devices, &endpoint_or_alias)?;
                expect_ok(
                    client
                        .request(ClientCmd::TrustRemove {
                            endpoint_id: endpoint_id.clone(),
                        })
                        .await?,
                )?;
                println!("removed {endpoint_id}");
            } else {
                let devices = trust::load_default()?;
                let endpoint_id = trust::resolve_remove(&devices, &endpoint_or_alias)?;
                trust::remove_default(&endpoint_id)?;
                println!("removed {endpoint_id}");
            }
        }
    }
    Ok(())
}

async fn trust_list_ipc(client: &mut IpcClient) -> Result<Vec<ipc::TrustedDevice>> {
    match client.request(ClientCmd::TrustList).await? {
        ServerMsg::Trusted { devices } => Ok(devices),
        ServerMsg::Error { error } => anyhow::bail!("{error}"),
        other => anyhow::bail!("unexpected reply: {other:?}"),
    }
}

fn expect_ok(msg: ServerMsg) -> Result<()> {
    match msg {
        ServerMsg::Ok { .. } => Ok(()),
        ServerMsg::Error { error } => anyhow::bail!("{error}"),
        other => anyhow::bail!("unexpected reply: {other:?}"),
    }
}

fn print_trusted(devices: &[ipc::TrustedDevice]) {
    if devices.is_empty() {
        println!("(no trusted devices)");
        return;
    }
    for d in devices {
        println!("{}  {}", d.alias, d.endpoint_id);
    }
}

async fn run_tui(config: NodeConfig, auto_accept: bool, enable_ble: bool) -> Result<()> {
    if let Some(client) = ipc::connect_if_running().await? {
        return tui::run(client).await;
    }
    let embedded = agent::spawn_embedded_agent(agent::AgentOpts {
        config,
        auto_accept,
        enable_ble,
    })
    .await?;
    let client = IpcClient::connect()
        .await
        .context("attach to in-process node")?;
    let result = tui::run(client).await;
    embedded.shutdown().await;
    result
}

async fn run_send(
    config: NodeConfig,
    files: Vec<PathBuf>,
    to: Option<String>,
    pin: Option<String>,
) -> Result<()> {
    if let Some(mut client) = ipc::connect_if_running().await? {
        println!(
            "using omashare  {}",
            match &client.hello {
                ServerMsg::Hello { alias, .. } => alias.as_str(),
                _ => "",
            }
        );
        client
            .send_cmd(&ClientCmd::Send {
                paths: files.iter().map(|p| p.display().to_string()).collect(),
                to,
                pin,
            })
            .await?;
        agent::wait_send(&mut client).await?;
        return Ok(());
    }

    let node = Node::start(config).await.context("start node")?;
    println!("omashare id {}", node.endpoint_id());
    wait_for_peer(&node, to.as_deref()).await?;
    let target = {
        let peers = node.nearby().await;
        ipc::resolve_peer_id(&peers, to.as_deref())?
    };
    println!(
        "sending {} -> {target}",
        files
            .iter()
            .map(|p| p.display().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    let mut events = node.subscribe();
    let sid = node.send_files_with_pin(&target, &files, pin).await?;
    println!("session {sid}  (Ctrl-C cancels)");
    wait_send_or_cancel(&node, &mut events, &sid.to_string()).await?;
    node.shutdown().await?;
    Ok(())
}

async fn wait_for_peer(node: &Node, to: Option<&str>) -> Result<()> {
    for _ in 0..25 {
        let peers = node.nearby().await;
        if to.is_none() && !peers.is_empty() {
            return Ok(());
        }
        if let Some(want) = to {
            if peers
                .iter()
                .any(|p| p.alias == want || p.endpoint_id == want)
            {
                return Ok(());
            }
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
    Ok(())
}

async fn wait_send_or_cancel(
    node: &Node,
    events: &mut tokio::sync::broadcast::Receiver<NodeEvent>,
    id: &str,
) -> Result<()> {
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                let _ = node.cancel_transfer(Some(id)).await;
                anyhow::bail!("cancelled");
            }
            ev = omashare_core::recv_event(events) => {
                match ev {
                    Some(NodeEvent::TransferComplete { session_id, .. }) if session_id == id => {
                        return Ok(());
                    }
                    Some(NodeEvent::TransferFailed { session_id, error }) if session_id == id => {
                        anyhow::bail!("{error}");
                    }
                    Some(_) => {}
                    None => anyhow::bail!("event stream closed"),
                }
            }
        }
    }
}

pub fn human_bytes(n: u64) -> String {
    const K: f64 = 1024.0;
    let x = n as f64;
    if n < 1024 {
        format!("{n} B")
    } else if x < K * K {
        format!("{:.1} KB", x / K)
    } else if x < K * K * K {
        format!("{:.1} MB", x / (K * K))
    } else {
        format!("{:.2} GB", x / (K * K * K))
    }
}

pub fn path_label(path: TransferPath) -> &'static str {
    match path {
        TransferPath::Lan => "LAN",
        TransferPath::DirectWan => "WAN",
        TransferPath::Relay => "relay",
        TransferPath::Unknown => "",
    }
}
