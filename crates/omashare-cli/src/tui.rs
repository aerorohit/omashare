use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use crossterm::event::{Event, EventStream, KeyCode, KeyEvent, KeyEventKind};
use futures_util::StreamExt;
use omashare_core::events::{DiscoverySource, NearbyPeer};
use omashare_core::protocol::{DeviceKind, FileOffer};
use omashare_core::{NodeEvent, TransferPath};
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, Paragraph, Wrap};
use ratatui::{DefaultTerminal, Frame};

use crate::ipc::{AgentLine, ClientCmd, IpcClient, ServerMsg};

struct Offer {
    session_id: String,
    from_alias: String,
    files: Vec<FileOffer>,
}

struct Progress {
    session_id: String,
    name: String,
    bytes_done: u64,
    bytes_total: u64,
    path: TransferPath,
}

struct Ui {
    alias: String,
    endpoint_id: String,
    peers: Vec<NearbyPeer>,
    selected: usize,
    inbox: Vec<Offer>,
    progress: Option<Progress>,
    picked: Vec<PathBuf>,
    status: String,
    quit: bool,
}

impl Ui {
    fn new(hello: &ServerMsg) -> Self {
        let (alias, endpoint_id) = match hello {
            ServerMsg::Hello {
                alias, endpoint_id, ..
            } => (alias.clone(), endpoint_id.clone()),
            _ => ("omashare".into(), String::new()),
        };
        Self {
            alias,
            endpoint_id,
            peers: Vec::new(),
            selected: 0,
            inbox: Vec::new(),
            progress: None,
            picked: Vec::new(),
            status: String::new(),
            quit: false,
        }
    }

    fn selected_peer(&self) -> Option<&NearbyPeer> {
        self.peers.get(self.selected)
    }

    fn apply_event(&mut self, ev: NodeEvent) {
        match ev {
            NodeEvent::NearbyUpdated { peers } => {
                self.peers = peers;
                if self.selected >= self.peers.len() {
                    self.selected = self.peers.len().saturating_sub(1);
                }
            }
            NodeEvent::IncomingOffer {
                session_id,
                from_alias,
                files,
                ..
            } => {
                self.inbox.retain(|o| o.session_id != session_id);
                self.inbox.push(Offer {
                    session_id,
                    from_alias,
                    files,
                });
            }
            NodeEvent::OfferResolved { session_id, .. }
            | NodeEvent::OfferAccepted { session_id } => {
                self.inbox.retain(|o| o.session_id != session_id);
            }
            NodeEvent::TransferProgress {
                session_id,
                bytes_done,
                bytes_total,
                path,
            } => {
                let name = self
                    .progress
                    .as_ref()
                    .filter(|p| p.session_id == session_id)
                    .map(|p| p.name.clone())
                    .unwrap_or_else(|| "transfer".into());
                self.progress = Some(Progress {
                    session_id,
                    name,
                    bytes_done,
                    bytes_total,
                    path,
                });
            }
            NodeEvent::PathChanged { session_id, path } => {
                if let Some(p) = self.progress.as_mut() {
                    if p.session_id == session_id {
                        p.path = path;
                    }
                }
            }
            NodeEvent::TransferComplete {
                session_id,
                saved_to,
            } => {
                self.inbox.retain(|o| o.session_id != session_id);
                self.progress = None;
                self.status = match saved_to {
                    Some(p) => format!("done → {p}"),
                    None => "done".into(),
                };
            }
            NodeEvent::TransferFailed { session_id, error } => {
                self.inbox.retain(|o| o.session_id != session_id);
                self.progress = None;
                self.status = format!("fail: {error}");
            }
            NodeEvent::MeteredConsentNeeded { .. } => {}
        }
    }
}

pub async fn run(mut client: IpcClient) -> Result<()> {
    let mut ui = Ui::new(&client.hello);
    let _ = client.attach_tui().await;
    if let Ok(peers) = client.nearby().await {
        ui.peers = peers;
    }

    let mut terminal = ratatui::init();
    let result = event_loop(&mut terminal, &mut client, &mut ui).await;
    ratatui::restore();
    result
}

async fn event_loop(
    terminal: &mut DefaultTerminal,
    client: &mut IpcClient,
    ui: &mut Ui,
) -> Result<()> {
    let mut keys = EventStream::new();
    let mut tick = tokio::time::interval(Duration::from_millis(200));
    loop {
        terminal.draw(|f| draw(f, ui))?;
        if ui.quit {
            break;
        }
        tokio::select! {
            _ = tick.tick() => {}
            line = client.read_line() => {
                match line? {
                    None => {
                        ui.status = "agent disconnected".into();
                        ui.quit = true;
                    }
                    Some(AgentLine::Event(ev)) => ui.apply_event(ev),
                    Some(AgentLine::Server(ServerMsg::Nearby { peers })) => {
                        ui.peers = peers;
                        if ui.selected >= ui.peers.len() {
                            ui.selected = ui.peers.len().saturating_sub(1);
                        }
                    }
                    Some(AgentLine::Server(ServerMsg::Error { error })) => {
                        ui.status = error;
                    }
                    Some(AgentLine::Server(ServerMsg::Ok { session_id })) => {
                        if let Some(id) = session_id {
                            ui.status = format!("session {id}");
                        }
                    }
                    Some(AgentLine::Server(_)) => {}
                }
            }
            ev = keys.next() => {
                let Some(Ok(ev)) = ev else { continue };
                if let Event::Key(key) = ev {
                    handle_key(terminal, client, ui, key).await?;
                }
            }
        }
    }
    Ok(())
}

async fn handle_key(
    terminal: &mut DefaultTerminal,
    client: &mut IpcClient,
    ui: &mut Ui,
    key: KeyEvent,
) -> Result<()> {
    if key.kind != KeyEventKind::Press {
        return Ok(());
    }
    match key.code {
        KeyCode::Char('q') | KeyCode::Esc => ui.quit = true,
        KeyCode::Char('j') | KeyCode::Down => {
            if !ui.peers.is_empty() {
                ui.selected = (ui.selected + 1) % ui.peers.len();
            }
        }
        KeyCode::Char('k') | KeyCode::Up => {
            if !ui.peers.is_empty() {
                ui.selected = (ui.selected + ui.peers.len() - 1) % ui.peers.len();
            }
        }
        KeyCode::Char('s') | KeyCode::Char('/') => {
            pick_files(terminal, ui).await?;
        }
        KeyCode::Enter => {
            if ui.picked.is_empty() && !pick_files(terminal, ui).await? {
                return Ok(());
            }
            send_to_selected(client, ui).await?;
        }
        KeyCode::Char('a') => {
            if let Some(offer) = ui.inbox.first() {
                let id = offer.session_id.clone();
                client
                    .send_cmd(&ClientCmd::Accept { session_id: id })
                    .await?;
            } else {
                ui.status = "no incoming offer".into();
            }
        }
        KeyCode::Char('d') => {
            if let Some(offer) = ui.inbox.first() {
                let id = offer.session_id.clone();
                client
                    .send_cmd(&ClientCmd::Decline { session_id: id })
                    .await?;
            } else {
                ui.status = "no incoming offer".into();
            }
        }
        KeyCode::Char('c') => {
            let id = ui
                .progress
                .as_ref()
                .map(|p| p.session_id.clone())
                .or_else(|| ui.inbox.first().map(|o| o.session_id.clone()));
            client
                .send_cmd(&ClientCmd::Cancel { session_id: id })
                .await?;
            ui.status = "cancelling…".into();
        }
        _ => {}
    }
    Ok(())
}

async fn send_to_selected(client: &mut IpcClient, ui: &mut Ui) -> Result<()> {
    let Some(peer) = ui.selected_peer() else {
        ui.status = "no peer selected".into();
        return Ok(());
    };
    let cmd = ClientCmd::Send {
        paths: ui.picked.iter().map(|p| p.display().to_string()).collect(),
        to: Some(peer.endpoint_id.clone()),
        pin: None,
    };
    ui.status = format!("sending to {}", peer.alias);
    client.send_cmd(&cmd).await?;
    Ok(())
}

async fn pick_files(terminal: &mut DefaultTerminal, ui: &mut Ui) -> Result<bool> {
    ratatui::restore();
    let paths = crate::picker::pick_files().await.unwrap_or_default();
    *terminal = ratatui::init();
    if paths.is_empty() {
        ui.status = "send cancelled".into();
        return Ok(false);
    }
    ui.picked = paths;
    ui.status = format!("{} file(s) queued", ui.picked.len());
    Ok(true)
}

fn draw(frame: &mut Frame, ui: &Ui) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Min(6),
            Constraint::Length(3),
            Constraint::Length(1),
        ])
        .split(frame.area());

    let header = Line::from(vec![
        Span::styled("omashare", Style::default().add_modifier(Modifier::BOLD)),
        Span::raw("  ·  "),
        Span::raw(&ui.alias),
        Span::raw("  ·  "),
        Span::raw(short_id(&ui.endpoint_id)),
        Span::raw("  ·  listening"),
    ]);
    frame.render_widget(Paragraph::new(header), chunks[0]);

    let body = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
        .split(chunks[1]);

    let items: Vec<ListItem> = if ui.peers.is_empty() {
        vec![ListItem::new("  (searching…)")]
    } else {
        ui.peers
            .iter()
            .enumerate()
            .map(|(i, p)| {
                let mark = if i == ui.selected { "●" } else { " " };
                let kind = kind_label(p.device_kind);
                let src = source_label(p.source);
                ListItem::new(format!("{mark} {:<18} {kind:<8} {src}", p.alias))
            })
            .collect()
    };
    frame.render_widget(
        List::new(items).block(Block::default().borders(Borders::ALL).title("Nearby")),
        body[0],
    );

    let inbox_items: Vec<ListItem> = if ui.inbox.is_empty() {
        vec![ListItem::new("(empty)")]
    } else {
        ui.inbox
            .iter()
            .map(|o| {
                let names: Vec<_> = o.files.iter().map(|f| f.name.as_str()).collect();
                ListItem::new(format!("{}: {}", o.from_alias, names.join(", ")))
            })
            .collect()
    };
    frame.render_widget(
        List::new(inbox_items).block(Block::default().borders(Borders::ALL).title("Inbox")),
        body[1],
    );

    let progress = if let Some(p) = &ui.progress {
        let total = p.bytes_total.max(1);
        let pct = (p.bytes_done.saturating_mul(100) / total).min(100);
        format!(
            "transfer  {}  {} / {}  {pct}%  {}",
            p.name,
            crate::human_bytes(p.bytes_done),
            crate::human_bytes(p.bytes_total),
            crate::path_label(p.path)
        )
    } else if !ui.status.is_empty() {
        ui.status.clone()
    } else if !ui.picked.is_empty() {
        format!("{} file(s) ready — enter to send", ui.picked.len())
    } else {
        String::new()
    };
    frame.render_widget(
        Paragraph::new(progress).block(Block::default().borders(Borders::ALL)),
        chunks[2],
    );

    frame.render_widget(
        Paragraph::new("[s] send   [enter] send to selected   [a] accept   [d] decline   [c] cancel   [q] quit"),
        chunks[3],
    );

    if let Some(offer) = ui.inbox.first() {
        draw_modal(frame, offer);
    }
}

fn draw_modal(frame: &mut Frame, offer: &Offer) {
    let area = centered(frame.area(), 60, 8);
    let names: Vec<_> = offer.files.iter().map(|f| f.name.as_str()).collect();
    let sizes: u64 = offer.files.iter().map(|f| f.size).sum();
    let text = format!(
        "{} wants to send\n{}\n{}\n\n[a] accept   [d] decline",
        offer.from_alias,
        names.join(", "),
        crate::human_bytes(sizes)
    );
    frame.render_widget(Clear, area);
    frame.render_widget(
        Paragraph::new(text)
            .wrap(Wrap { trim: true })
            .block(Block::default().borders(Borders::ALL).title("Incoming")),
        area,
    );
}

fn centered(area: Rect, width: u16, height: u16) -> Rect {
    let width = width.min(area.width);
    let height = height.min(area.height);
    Rect {
        x: area.x + (area.width.saturating_sub(width)) / 2,
        y: area.y + (area.height.saturating_sub(height)) / 2,
        width,
        height,
    }
}

fn short_id(id: &str) -> &str {
    id.get(..8).unwrap_or(id)
}

fn source_label(source: DiscoverySource) -> &'static str {
    match source {
        DiscoverySource::Bluetooth => "bt",
        DiscoverySource::Mdns | DiscoverySource::NativeDnsSd => "mdns",
        DiscoverySource::Multicast | DiscoverySource::SubnetScan => "lan",
        DiscoverySource::Manual => "",
    }
}

fn kind_label(kind: DeviceKind) -> &'static str {
    match kind {
        DeviceKind::Mobile => "mobile",
        DeviceKind::Desktop => "desktop",
        DeviceKind::Headless => "headless",
        DeviceKind::Unknown => "",
    }
}
