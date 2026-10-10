use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use crossterm::event::{Event, EventStream, KeyCode, KeyEvent, KeyEventKind};
use futures_util::StreamExt;
use omashare_core::events::{DiscoverySource, NearbyPeer};
use omashare_core::protocol::{DeviceKind, FileOffer};
use omashare_core::{NodeEvent, TransferPath};
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, Paragraph, Wrap};
use ratatui::{DefaultTerminal, Frame};

use crate::ipc::{AgentLine, ClientCmd, IpcClient, ServerMsg, TrustedDevice};

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

struct PendingTrust {
    endpoint_id: String,
    alias: String,
}

struct Ui {
    alias: String,
    endpoint_id: String,
    peers: Vec<NearbyPeer>,
    selected: usize,
    trusted: HashSet<String>,
    pending_trust: Option<PendingTrust>,
    offer: Option<Offer>,
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
            trusted: HashSet::new(),
            pending_trust: None,
            offer: None,
            progress: None,
            picked: Vec::new(),
            status: String::new(),
            quit: false,
        }
    }

    fn selected_peer(&self) -> Option<&NearbyPeer> {
        self.peers.get(self.selected)
    }

    fn replace_trusted(&mut self, devices: Vec<TrustedDevice>) {
        self.trusted = devices.into_iter().map(|d| d.endpoint_id).collect();
        let Some(pending) = self.pending_trust.take() else {
            return;
        };
        if self.trusted.contains(&pending.endpoint_id) {
            self.status = format!("trusted {}", pending.alias);
        } else {
            self.status = format!("untrusted {}", pending.alias);
        }
    }

    fn set_offer(&mut self, session_id: String, from_alias: String, files: Vec<FileOffer>) {
        let replace = self
            .offer
            .as_ref()
            .is_none_or(|o| o.session_id == session_id);
        if replace {
            self.offer = Some(Offer {
                session_id,
                from_alias,
                files,
            });
        }
    }

    fn clear_offer(&mut self, session_id: &str) {
        if self
            .offer
            .as_ref()
            .is_some_and(|o| o.session_id == session_id)
        {
            self.offer = None;
        }
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
            } => self.set_offer(session_id, from_alias, files),
            NodeEvent::OfferResolved { session_id, .. }
            | NodeEvent::OfferAccepted { session_id } => {
                self.clear_offer(&session_id);
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
                self.clear_offer(&session_id);
                self.progress = None;
                self.status = match saved_to {
                    Some(p) => format!("done → {p}"),
                    None => "done".into(),
                };
            }
            NodeEvent::TransferFailed { session_id, error } => {
                self.clear_offer(&session_id);
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
    if let Ok(ServerMsg::Trusted { devices }) = client.request(ClientCmd::TrustList).await {
        ui.replace_trusted(devices);
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
                    Some(AgentLine::Server(ServerMsg::Trusted { devices })) => {
                        ui.replace_trusted(devices);
                    }
                    Some(AgentLine::Server(ServerMsg::Error { error })) => {
                        ui.pending_trust = None;
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
        KeyCode::Char('t') => {
            toggle_trust(client, ui).await?;
        }
        KeyCode::Char('a') => {
            if let Some(offer) = &ui.offer {
                let id = offer.session_id.clone();
                client
                    .send_cmd(&ClientCmd::Accept { session_id: id })
                    .await?;
            }
        }
        KeyCode::Char('d') => {
            if let Some(offer) = &ui.offer {
                let id = offer.session_id.clone();
                client
                    .send_cmd(&ClientCmd::Decline { session_id: id })
                    .await?;
            }
        }
        KeyCode::Char('c') => {
            let id = ui
                .progress
                .as_ref()
                .map(|p| p.session_id.clone())
                .or_else(|| ui.offer.as_ref().map(|o| o.session_id.clone()));
            client
                .send_cmd(&ClientCmd::Cancel { session_id: id })
                .await?;
            ui.status = "cancelling…".into();
        }
        _ => {}
    }
    Ok(())
}

async fn toggle_trust(client: &mut IpcClient, ui: &mut Ui) -> Result<()> {
    if ui.pending_trust.is_some() {
        return Ok(());
    }
    let Some(peer) = ui.selected_peer() else {
        ui.status = "no peer selected".into();
        return Ok(());
    };
    let endpoint_id = peer.endpoint_id.clone();
    let alias = peer.alias.clone();
    let add = !ui.trusted.contains(&endpoint_id);
    let cmd = if add {
        ClientCmd::TrustAdd {
            endpoint_id: endpoint_id.clone(),
            alias: alias.clone(),
        }
    } else {
        ClientCmd::TrustRemove {
            endpoint_id: endpoint_id.clone(),
        }
    };
    ui.pending_trust = Some(PendingTrust { endpoint_id, alias });
    client.send_cmd(&cmd).await?;
    client.send_cmd(&ClientCmd::TrustList).await?;
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
        Span::styled("omashare", paint(Color::Cyan).add_modifier(Modifier::BOLD)),
        Span::styled("  ·  ", dim()),
        Span::styled(&ui.alias, Style::default().add_modifier(Modifier::BOLD)),
        Span::styled("  ·  ", dim()),
        Span::styled(short_id(&ui.endpoint_id), dim()),
        Span::styled("  ·  ", dim()),
        Span::styled("listening", paint(Color::Green)),
    ]);
    frame.render_widget(Paragraph::new(header), chunks[0]);

    let items: Vec<ListItem> = if ui.peers.is_empty() {
        vec![ListItem::new(Span::styled("  (searching…)", dim()))]
    } else {
        ui.peers
            .iter()
            .enumerate()
            .map(|(i, p)| ListItem::new(peer_line(ui, i, p)))
            .collect()
    };
    frame.render_widget(
        List::new(items).block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(paint(Color::Blue))
                .title(Span::styled(
                    " Nearby ",
                    paint(Color::Cyan).add_modifier(Modifier::BOLD),
                )),
        ),
        chunks[1],
    );

    let (progress, progress_border) = if let Some(p) = &ui.progress {
        let total = p.bytes_total.max(1);
        let pct = (p.bytes_done.saturating_mul(100) / total).min(100);
        let line = Line::from(vec![
            Span::styled("transfer  ", dim()),
            Span::styled(p.name.as_str(), paint(Color::Cyan)),
            Span::raw("  "),
            Span::raw(crate::human_bytes(p.bytes_done)),
            Span::styled(" / ", dim()),
            Span::raw(crate::human_bytes(p.bytes_total)),
            Span::raw("  "),
            Span::styled(format!("{pct}%"), paint(Color::Green).add_modifier(Modifier::BOLD)),
            Span::raw("  "),
            Span::styled(
                crate::path_label(p.path).to_string(),
                paint(Color::Yellow),
            ),
        ]);
        (line, Color::Green)
    } else if !ui.status.is_empty() {
        let color = if ui.status.starts_with("fail") {
            Color::Red
        } else {
            Color::Reset
        };
        (Line::from(Span::styled(ui.status.as_str(), paint(color))), Color::Blue)
    } else if !ui.picked.is_empty() {
        (
            Line::from(Span::styled(
                format!("{} file(s) ready — enter to send", ui.picked.len()),
                paint(Color::Yellow),
            )),
            Color::Yellow,
        )
    } else {
        (Line::from(""), Color::Blue)
    };
    frame.render_widget(
        Paragraph::new(progress).block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(paint(progress_border)),
        ),
        chunks[2],
    );

    frame.render_widget(Paragraph::new(key_hint()), chunks[3]);

    if let Some(offer) = &ui.offer {
        draw_modal(frame, offer);
    }
}

fn draw_modal(frame: &mut Frame, offer: &Offer) {
    let area = centered(frame.area(), 60, 8);
    let names: Vec<_> = offer.files.iter().map(|f| f.name.as_str()).collect();
    let sizes: u64 = offer.files.iter().map(|f| f.size).sum();
    let text = vec![
        Line::from(vec![
            Span::styled(offer.from_alias.as_str(), paint(Color::Cyan).add_modifier(Modifier::BOLD)),
            Span::raw(" wants to send"),
        ]),
        Line::from(names.join(", ")),
        Line::from(Span::styled(crate::human_bytes(sizes), dim())),
        Line::from(""),
        Line::from(vec![
            Span::styled("[a]", paint(Color::Green).add_modifier(Modifier::BOLD)),
            Span::raw(" accept   "),
            Span::styled("[d]", paint(Color::Red).add_modifier(Modifier::BOLD)),
            Span::raw(" decline"),
        ]),
    ];
    frame.render_widget(Clear, area);
    frame.render_widget(
        Paragraph::new(text)
            .wrap(Wrap { trim: true })
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(paint(Color::Yellow))
                    .title(Span::styled(
                        " Incoming ",
                        paint(Color::Yellow).add_modifier(Modifier::BOLD),
                    )),
            ),
        area,
    );
}

/// ANSI palette colors (0–15). Terminal themes remap these, so the TUI follows the theme.
fn paint(color: Color) -> Style {
    Style::default().fg(color)
}

fn dim() -> Style {
    Style::default().add_modifier(Modifier::DIM)
}

fn peer_line(ui: &Ui, index: usize, peer: &NearbyPeer) -> Line<'static> {
    let selected = index == ui.selected;
    let mark = if selected { "●" } else { " " };
    let mark_style = if selected {
        paint(Color::Cyan).add_modifier(Modifier::BOLD)
    } else {
        dim()
    };
    let name_style = if selected {
        Style::default().add_modifier(Modifier::BOLD)
    } else {
        Style::default()
    };
    let mut spans = vec![
        Span::styled(format!("{mark} "), mark_style),
        Span::styled(format!("{:<18}", peer.alias), name_style),
    ];
    if ui.trusted.contains(&peer.endpoint_id) {
        spans.push(Span::styled(
            format!("{:<8}", "trusted"),
            paint(Color::Green),
        ));
    } else {
        spans.push(Span::raw(format!("{:<8}", "")));
    }
    spans.push(Span::styled(
        format!("{:<8}", kind_label(peer.device_kind)),
        paint(kind_color(peer.device_kind)),
    ));
    spans.push(Span::styled(
        source_label(peer.source).to_string(),
        paint(source_color(peer.source)),
    ));
    Line::from(spans)
}

fn key_hint() -> Line<'static> {
    let keys = [
        ("s", "send"),
        ("enter", "send to selected"),
        ("t", "trust"),
        ("a", "accept"),
        ("d", "decline"),
        ("c", "cancel"),
        ("q", "quit"),
    ];
    let mut spans = Vec::new();
    for (i, (key, label)) in keys.into_iter().enumerate() {
        if i > 0 {
            spans.push(Span::raw("   "));
        }
        spans.push(Span::styled(
            format!("[{key}]"),
            paint(Color::Yellow).add_modifier(Modifier::BOLD),
        ));
        spans.push(Span::raw(format!(" {label}")));
    }
    Line::from(spans)
}

fn kind_color(kind: DeviceKind) -> Color {
    match kind {
        DeviceKind::Mobile => Color::Magenta,
        DeviceKind::Desktop => Color::Blue,
        DeviceKind::Headless => Color::Yellow,
        DeviceKind::Unknown => Color::Reset,
    }
}

fn source_color(source: DiscoverySource) -> Color {
    match source {
        DiscoverySource::Bluetooth => Color::Magenta,
        DiscoverySource::Mdns | DiscoverySource::NativeDnsSd => Color::Cyan,
        DiscoverySource::Multicast | DiscoverySource::SubnetScan => Color::Blue,
        DiscoverySource::Manual => Color::Reset,
    }
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
