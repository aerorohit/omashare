use std::path::PathBuf;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use omashare_core::events::NearbyPeer;
use omashare_core::NodeEvent;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, ReadHalf, WriteHalf};
use tokio::net::UnixStream;
use tokio::sync::Mutex;

pub fn socket_path() -> PathBuf {
    dirs::runtime_dir()
        .unwrap_or_else(std::env::temp_dir)
        .join("omashare.sock")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum ClientCmd {
    Nearby,
    Send {
        paths: Vec<String>,
        #[serde(default)]
        to: Option<String>,
        #[serde(default)]
        pin: Option<String>,
    },
    Accept {
        session_id: String,
    },
    Decline {
        session_id: String,
    },
    Cancel {
        #[serde(default)]
        session_id: Option<String>,
    },
    Status,
    Attach {
        #[serde(default)]
        kind: ClientKind,
    },
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ClientKind {
    #[default]
    Cli,
    Tui,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMsg {
    Hello {
        alias: String,
        endpoint_id: String,
        receive_dir: String,
    },
    Nearby {
        peers: Vec<NearbyPeer>,
    },
    Status {
        alias: String,
        endpoint_id: String,
        receive_dir: String,
        auto_accept: bool,
        listening: bool,
    },
    Ok {
        #[serde(default)]
        session_id: Option<String>,
    },
    Error {
        error: String,
    },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum AgentLine {
    Server(ServerMsg),
    Event(NodeEvent),
}

pub struct IpcClient {
    reader: BufReader<ReadHalf<UnixStream>>,
    writer: Mutex<WriteHalf<UnixStream>>,
    pub hello: ServerMsg,
}

impl IpcClient {
    pub async fn connect() -> Result<Self> {
        let path = socket_path();
        let stream = UnixStream::connect(&path)
            .await
            .with_context(|| format!("connect {}", path.display()))?;
        let (r, w) = tokio::io::split(stream);
        let mut reader = BufReader::new(r);
        let mut line = String::new();
        reader.read_line(&mut line).await.context("read hello")?;
        if line.is_empty() {
            bail!("agent closed before hello");
        }
        let hello: ServerMsg = serde_json::from_str(line.trim()).context("parse hello")?;
        if !matches!(hello, ServerMsg::Hello { .. }) {
            bail!("expected hello from agent, got {hello:?}");
        }
        Ok(Self {
            reader,
            writer: Mutex::new(w),
            hello,
        })
    }

    pub async fn send_cmd(&self, cmd: &ClientCmd) -> Result<()> {
        let mut w = self.writer.lock().await;
        let mut line = serde_json::to_string(cmd)?;
        line.push('\n');
        w.write_all(line.as_bytes()).await?;
        w.flush().await?;
        Ok(())
    }

    pub async fn read_line(&mut self) -> Result<Option<AgentLine>> {
        let mut line = String::new();
        let n = self.reader.read_line(&mut line).await?;
        if n == 0 {
            return Ok(None);
        }
        let parsed = serde_json::from_str(line.trim())
            .with_context(|| format!("agent line: {}", line.trim()))?;
        Ok(Some(parsed))
    }

    pub async fn request(&mut self, cmd: ClientCmd) -> Result<ServerMsg> {
        self.send_cmd(&cmd).await?;
        loop {
            match self.read_line().await? {
                None => bail!("agent disconnected"),
                Some(AgentLine::Server(
                    msg @ (ServerMsg::Ok { .. }
                    | ServerMsg::Error { .. }
                    | ServerMsg::Nearby { .. }
                    | ServerMsg::Status { .. }),
                )) => {
                    return Ok(msg);
                }
                Some(AgentLine::Server(ServerMsg::Hello { .. })) => continue,
                Some(AgentLine::Event(_)) => continue,
            }
        }
    }

    pub async fn nearby(&mut self) -> Result<Vec<NearbyPeer>> {
        match self.request(ClientCmd::Nearby).await? {
            ServerMsg::Nearby { peers } => Ok(peers),
            ServerMsg::Error { error } => bail!("{error}"),
            other => bail!("unexpected nearby reply: {other:?}"),
        }
    }

    pub async fn attach_tui(&mut self) -> Result<()> {
        match self
            .request(ClientCmd::Attach {
                kind: ClientKind::Tui,
            })
            .await?
        {
            ServerMsg::Ok { .. } => Ok(()),
            ServerMsg::Error { error } => anyhow::bail!("{error}"),
            other => anyhow::bail!("unexpected attach reply: {other:?}"),
        }
    }
}

pub async fn agent_is_healthy() -> bool {
    match tokio::time::timeout(Duration::from_millis(500), IpcClient::connect()).await {
        Ok(Ok(_)) => true,
        _ => false,
    }
}

pub async fn connect_if_running() -> Result<Option<IpcClient>> {
    if !socket_path().exists() {
        return Ok(None);
    }
    match tokio::time::timeout(Duration::from_millis(500), IpcClient::connect()).await {
        Ok(Ok(c)) => Ok(Some(c)),
        Ok(Err(_)) | Err(_) => Ok(None),
    }
}

pub fn resolve_peer_id(peers: &[NearbyPeer], to: Option<&str>) -> Result<String> {
    if let Some(want) = to {
        if let Some(p) = peers
            .iter()
            .find(|p| p.alias == want || p.endpoint_id == want || p.endpoint_id.starts_with(want))
        {
            return Ok(p.endpoint_id.clone());
        }
        bail!("peer not found: {want}");
    }
    peers
        .first()
        .map(|p| p.endpoint_id.clone())
        .context("no nearby peers")
}
