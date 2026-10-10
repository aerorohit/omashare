use std::io::{self, IsTerminal, Write};
use std::path::PathBuf;
use std::process::Stdio;

use anyhow::Result;

/// Open the native file chooser. Empty vec means the user cancelled.
pub async fn pick_files() -> Result<Vec<PathBuf>> {
    match portal_pick().await {
        Ok(paths) => return Ok(paths),
        Err(e) => tracing::debug!("portal file chooser: {e:#}"),
    }
    match zenity_pick().await {
        Ok(paths) => return Ok(paths),
        Err(e) => tracing::debug!("zenity: {e:#}"),
    }
    typed_paths()
}

async fn portal_pick() -> Result<Vec<PathBuf>> {
    use ashpd::desktop::file_chooser::OpenFileRequest;

    let files = OpenFileRequest::default()
        .title("Omashare — send files")
        .multiple(true)
        .modal(true)
        .send()
        .await?
        .response()?;
    Ok(files
        .uris()
        .iter()
        .filter_map(|u| url_to_path(u.as_str()))
        .collect())
}

fn url_to_path(uri: &str) -> Option<PathBuf> {
    let url = url::Url::parse(uri).ok()?;
    url.to_file_path().ok()
}

async fn zenity_pick() -> Result<Vec<PathBuf>> {
    let out = tokio::process::Command::new("zenity")
        .args([
            "--file-selection",
            "--multiple",
            "--separator=\n",
            "--title=Omashare — send files",
        ])
        .stdin(Stdio::null())
        .output()
        .await?;
    if !out.status.success() {
        return Ok(Vec::new());
    }
    let text = String::from_utf8_lossy(&out.stdout);
    Ok(text
        .lines()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .collect())
}

fn typed_paths() -> Result<Vec<PathBuf>> {
    if !io::stdin().is_terminal() {
        return Ok(Vec::new());
    }
    eprint!("file paths (one per line, empty line to finish): ");
    let _ = io::stderr().flush();
    let mut out = Vec::new();
    loop {
        let mut line = String::new();
        io::stdin().read_line(&mut line)?;
        let path = line.trim();
        if path.is_empty() {
            break;
        }
        out.push(PathBuf::from(path));
    }
    Ok(out)
}
