mod agent;
mod ipc;
mod listen;
mod picker;
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
    /// Background node (discovery, offers, transfers, notifications).
    Agent,
    /// Alias of `agent`.
    Daemon,
    /// Nearby list, inbox, and progress (default).
    Tui,
    /// Interactive debug prompt; attaches to the agent when it is running.
    Listen,
    /// List nearby peers (agent).
    Nearby,
    /// Agent identity and receive directory.
    Status,
    /// Accept an incoming offer (agent).
    Accept { session_id: String },
    /// Decline an incoming offer (agent).
    Decline { session_id: String },
    /// Cancel a transfer (agent).
    Cancel { session_id: Option<String> },
    /// Send files via the agent, or a one-shot node if the agent is down.
    Send {
        #[arg(required = true)]
        files: Vec<PathBuf>,
        #[arg(long)]
        to: Option<String>,
        #[arg(long)]
        pin: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let filter = if cli.verbose {
        "omashare=debug"
    } else {
        "omashare=info"
    };
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env().add_directive(filter.parse()?),
        )
        .init();

    let config = node_config(&cli);
    match cli.command {
        Some(Commands::Agent | Commands::Daemon) => {
            agent::run_agent(agent::AgentOpts {
                config,
                auto_accept: cli.auto_accept,
                enable_ble: !cli.no_ble,
            })
            .await?;
        }
        Some(Commands::Listen) => run_listen(config).await?,
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
        Some(Commands::Send { files, to, pin }) => run_send(config, files, to, pin).await?,
        Some(Commands::Tui) | None => run_tui(config).await?,
    }
    Ok(())
}

fn node_config(cli: &Cli) -> NodeConfig {
    let data = dirs::data_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("omashare");
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
        .context("agent not running (start `omashare agent`)")?;
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

async fn run_tui(config: NodeConfig) -> Result<()> {
    if !std::io::stdout().is_terminal() {
        anyhow::bail!("not a tty; use `omashare agent` or `omashare send`");
    }
    if let Some(client) = ipc::connect_if_running().await? {
        return tui::run(client).await;
    }
    let embedded = agent::spawn_embedded_agent(agent::AgentOpts {
        config,
        auto_accept: false,
        enable_ble: true,
    })
    .await?;
    let client = IpcClient::connect()
        .await
        .context("attach to in-process node")?;
    let result = tui::run(client).await;
    embedded.shutdown().await;
    result
}

async fn run_listen(config: NodeConfig) -> Result<()> {
    if let Some(client) = ipc::connect_if_running().await? {
        return listen::run(client).await;
    }
    let embedded = agent::spawn_embedded_agent(agent::AgentOpts {
        config,
        auto_accept: false,
        enable_ble: true,
    })
    .await?;
    let client = IpcClient::connect()
        .await
        .context("attach to in-process node")?;
    let result = listen::run(client).await;
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
            "using agent  {}",
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
