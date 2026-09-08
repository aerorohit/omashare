use std::io::{self, BufRead, Write};

use anyhow::Result;
use omashare_core::NodeEvent;

use crate::ipc::{AgentLine, ClientCmd, IpcClient, ServerMsg};

pub async fn run(mut client: IpcClient) -> Result<()> {
    let _ = client.attach_tui().await;
    println!(
        "attached to agent  {}",
        match &client.hello {
            ServerMsg::Hello {
                alias, endpoint_id, ..
            } => format!("{alias}  {endpoint_id}"),
            _ => String::new(),
        }
    );
    println!(
        "commands: peers | send FILE [PEER] [PIN] | accept ID | decline ID | cancel [ID] | quit"
    );

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    std::thread::spawn(move || {
        let stdin = io::stdin();
        for line in stdin.lock().lines() {
            match line {
                Ok(l) => {
                    if tx.send(l).is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });

    let mut stdout = io::stdout();
    write!(stdout, "omashare> ")?;
    stdout.flush()?;

    loop {
        tokio::select! {
            line = client.read_line() => {
                match line? {
                    None => {
                        println!("agent disconnected");
                        break;
                    }
                    Some(AgentLine::Event(ev)) => print_event(&ev),
                    Some(AgentLine::Server(ServerMsg::Nearby { peers })) => {
                        for p in peers {
                            println!(
                                "  {}  {}  pin={}  {:?}",
                                p.alias, p.endpoint_id, p.pin_required, p.source
                            );
                        }
                    }
                    Some(AgentLine::Server(ServerMsg::Error { error })) => println!("{error}"),
                    Some(AgentLine::Server(ServerMsg::Ok { session_id })) => {
                        if let Some(id) = session_id {
                            println!("session {id}");
                        }
                    }
                    Some(AgentLine::Server(_)) => {}
                }
                write!(stdout, "omashare> ")?;
                stdout.flush()?;
            }
            cmd = rx.recv() => {
                let Some(line) = cmd else { break };
                if handle_repl_line(&mut client, &line).await? {
                    break;
                }
                write!(stdout, "omashare> ")?;
                stdout.flush()?;
            }
        }
    }
    Ok(())
}

async fn handle_repl_line(client: &mut IpcClient, line: &str) -> Result<bool> {
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.is_empty() {
        return Ok(false);
    }
    match parts[0] {
        "quit" | "exit" => return Ok(true),
        "peers" => client.send_cmd(&ClientCmd::Nearby).await?,
        "send" if parts.len() >= 2 => {
            client
                .send_cmd(&ClientCmd::Send {
                    paths: vec![parts[1].to_string()],
                    to: parts.get(2).map(|s| (*s).to_string()),
                    pin: parts.get(3).map(|s| (*s).to_string()),
                })
                .await?;
        }
        "accept" if parts.len() == 2 => {
            client
                .send_cmd(&ClientCmd::Accept {
                    session_id: parts[1].to_string(),
                })
                .await?;
        }
        "decline" if parts.len() == 2 => {
            client
                .send_cmd(&ClientCmd::Decline {
                    session_id: parts[1].to_string(),
                })
                .await?;
        }
        "cancel" => {
            client
                .send_cmd(&ClientCmd::Cancel {
                    session_id: parts.get(1).map(|s| (*s).to_string()),
                })
                .await?;
        }
        _ => println!("unknown command"),
    }
    Ok(false)
}

fn print_event(ev: &NodeEvent) {
    match ev {
        NodeEvent::IncomingOffer {
            session_id,
            from_alias,
            files,
            ..
        } => {
            let names: Vec<_> = files.iter().map(|f| f.name.as_str()).collect();
            println!("OFFER {session_id} from {from_alias}: {}", names.join(", "));
        }
        NodeEvent::TransferProgress {
            bytes_done,
            bytes_total,
            path,
            ..
        } => {
            println!(
                "{} / {}  {}",
                crate::human_bytes(*bytes_done),
                crate::human_bytes(*bytes_total),
                crate::path_label(*path)
            );
        }
        NodeEvent::TransferComplete {
            session_id,
            saved_to,
        } => match saved_to {
            Some(p) => println!("done {session_id}  saved {p}"),
            None => println!("done {session_id}"),
        },
        NodeEvent::TransferFailed { session_id, error } => {
            println!("fail {session_id}  {error}");
        }
        NodeEvent::NearbyUpdated { peers } if !peers.is_empty() => {
            println!(
                "nearby: {}",
                peers
                    .iter()
                    .map(|p| p.alias.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
        NodeEvent::PathChanged { path, .. } => {
            println!("path {}", crate::path_label(*path));
        }
        _ => {}
    }
}
