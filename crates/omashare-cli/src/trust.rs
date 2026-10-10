//! Local allowlist of endpoint ids the agent auto-accepts.
//! Not a wire-protocol change. Alias is a display label; matching uses
//! `endpoint_id` only.

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::{bail, Context, Result};
use omashare_core::NearbyPeer;

use crate::ipc::TrustedDevice;

/// `$XDG_DATA_HOME/omashare`, or `~/.local/share/omashare` when that is unset.
pub(crate) fn data_dir() -> PathBuf {
    dirs::data_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("omashare")
}

pub(crate) fn trust_path() -> PathBuf {
    data_dir().join("trusted.json")
}

fn file_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

pub(crate) fn load_default() -> Result<Vec<TrustedDevice>> {
    let _guard = file_lock();
    load_path(&trust_path())
}

pub(crate) fn is_trusted(endpoint_id: &str) -> bool {
    match load_default() {
        Ok(devices) => contains(&devices, endpoint_id),
        Err(e) => {
            tracing::warn!("trusted devices: {e:#}");
            false
        }
    }
}

pub(crate) fn add_default(endpoint_id: String, alias: String) -> Result<()> {
    let _guard = file_lock();
    let path = trust_path();
    let mut devices = load_path(&path)?;
    let endpoint_id = canonical_endpoint_id(&endpoint_id).unwrap_or(endpoint_id);
    if let Some(d) = devices.iter_mut().find(|d| d.endpoint_id == endpoint_id) {
        d.alias = alias;
    } else {
        devices.push(TrustedDevice { endpoint_id, alias });
    }
    save_path(&path, &devices)
}

pub(crate) fn remove_default(endpoint_id: &str) -> Result<()> {
    let _guard = file_lock();
    let path = trust_path();
    let mut devices = load_path(&path)?;
    let canon = canonical_endpoint_id(endpoint_id);
    let before = devices.len();
    devices.retain(|d| {
        if d.endpoint_id == endpoint_id {
            return false;
        }
        if let Some(c) = &canon {
            if &d.endpoint_id == c {
                return false;
            }
        }
        true
    });
    if devices.len() == before {
        bail!("not trusted: {endpoint_id}");
    }
    save_path(&path, &devices)
}

fn load_path(path: &Path) -> Result<Vec<TrustedDevice>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let raw = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    parse_trusted(&raw).with_context(|| format!("parse {}", path.display()))
}

fn save_path(path: &Path, devices: &[TrustedDevice]) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let mut body = serde_json::to_string_pretty(devices)?;
    body.push('\n');
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, &body).with_context(|| format!("write {}", tmp.display()))?;
    std::fs::rename(&tmp, path).with_context(|| format!("rename {}", path.display()))?;
    Ok(())
}

fn parse_trusted(raw: &str) -> Result<Vec<TrustedDevice>> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(Vec::new());
    }
    Ok(serde_json::from_str(raw)?)
}

/// Match only `endpoint_id`. Alias is never a trust key.
fn contains(devices: &[TrustedDevice], endpoint_id: &str) -> bool {
    let canon = canonical_endpoint_id(endpoint_id);
    devices.iter().any(|d| {
        d.endpoint_id == endpoint_id || canon.as_deref().is_some_and(|c| d.endpoint_id == c)
    })
}

/// Hex form printed by `Node::endpoint_id` (32-byte key).
fn canonical_endpoint_id(s: &str) -> Option<String> {
    if s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit()) {
        Some(s.to_ascii_lowercase())
    } else {
        None
    }
}

/// Resolve `trust add`. One nearby peer (alias, exact id, or unique id prefix).
/// A full endpoint id that is not nearby is stored as both id and alias.
pub(crate) fn resolve_add(peers: &[NearbyPeer], query: &str) -> Result<(String, String)> {
    if let Some(p) = peers.iter().find(|p| p.endpoint_id == query) {
        return Ok((p.endpoint_id.clone(), p.alias.clone()));
    }
    let alias_hits: Vec<_> = peers.iter().filter(|p| p.alias == query).collect();
    if alias_hits.len() > 1 {
        bail!("ambiguous peer: {query}");
    }
    if let Some(p) = alias_hits.first() {
        return Ok((p.endpoint_id.clone(), p.alias.clone()));
    }
    let prefix_hits: Vec<_> = peers
        .iter()
        .filter(|p| p.endpoint_id.starts_with(query) && p.endpoint_id != query)
        .collect();
    if prefix_hits.len() > 1 {
        bail!("ambiguous peer: {query}");
    }
    if let Some(p) = prefix_hits.first() {
        return Ok((p.endpoint_id.clone(), p.alias.clone()));
    }
    if let Some(id) = canonical_endpoint_id(query) {
        if let Some(p) = peers.iter().find(|p| p.endpoint_id == id) {
            return Ok((p.endpoint_id.clone(), p.alias.clone()));
        }
        return Ok((id.clone(), id));
    }
    bail!("peer not found: {query}");
}

/// Resolve `trust remove` against the trust file (the peer need not be nearby).
pub(crate) fn resolve_remove(devices: &[TrustedDevice], query: &str) -> Result<String> {
    if let Some(d) = devices.iter().find(|d| d.endpoint_id == query) {
        return Ok(d.endpoint_id.clone());
    }
    let alias_hits: Vec<_> = devices.iter().filter(|d| d.alias == query).collect();
    if alias_hits.len() > 1 {
        bail!("ambiguous device: {query}");
    }
    if let Some(d) = alias_hits.first() {
        return Ok(d.endpoint_id.clone());
    }
    let prefix_hits: Vec<_> = devices
        .iter()
        .filter(|d| d.endpoint_id.starts_with(query) && d.endpoint_id != query)
        .collect();
    if prefix_hits.len() > 1 {
        bail!("ambiguous device: {query}");
    }
    if let Some(d) = prefix_hits.first() {
        return Ok(d.endpoint_id.clone());
    }
    if let Some(id) = canonical_endpoint_id(query) {
        if let Some(d) = devices.iter().find(|d| d.endpoint_id == id) {
            return Ok(d.endpoint_id.clone());
        }
    }
    bail!("not trusted: {query}");
}

#[cfg(test)]
mod tests {
    use super::*;
    use omashare_core::{DeviceKind, DiscoverySource};

    fn peer(alias: &str, endpoint_id: &str) -> NearbyPeer {
        NearbyPeer {
            alias: alias.into(),
            device_kind: DeviceKind::Desktop,
            endpoint_id: endpoint_id.into(),
            pin_required: false,
            source: DiscoverySource::Manual,
            addrs: vec![],
        }
    }

    fn hex_id(byte: u8) -> String {
        format!("{byte:02x}").repeat(32)
    }

    #[test]
    fn load_save_contains() {
        let dir = std::env::temp_dir().join(format!(
            "omashare-trust-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _cleanup = Rm(&dir);
        let path = dir.join("omashare").join("trusted.json");

        assert!(load_path(&path).unwrap().is_empty());
        assert!(!contains(&[], "abc"));

        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, "").unwrap();
        assert!(load_path(&path).unwrap().is_empty());
        std::fs::write(&path, "[]").unwrap();
        assert!(load_path(&path).unwrap().is_empty());
        std::fs::write(&path, "not-json").unwrap();
        assert!(load_path(&path).is_err());

        let devices = vec![TrustedDevice {
            endpoint_id: "abc".into(),
            alias: "phone".into(),
        }];
        let nested = dir.join("created").join("trusted.json");
        save_path(&nested, &devices).unwrap();
        assert!(nested.is_file());
        let raw = std::fs::read_to_string(&nested).unwrap();
        assert!(raw.contains("\"endpoint_id\""));
        assert!(raw.contains("\"alias\""));
        let loaded = load_path(&nested).unwrap();
        assert_eq!(loaded, devices);
        assert!(contains(&loaded, "abc"));
        assert!(!contains(&loaded, "phone"));
        assert!(!contains(&loaded, "ab"));
    }

    #[test]
    fn resolve_add_and_remove() {
        let id_a = hex_id(0x11);
        let id_b = hex_id(0x22);
        let peers = vec![peer("laptop", &id_a), peer("phone", &id_b)];

        let (eid, alias) = resolve_add(&peers, "phone").unwrap();
        assert_eq!(eid, id_b);
        assert_eq!(alias, "phone");

        let (eid, alias) = resolve_add(&peers, &id_a).unwrap();
        assert_eq!(eid, id_a);
        assert_eq!(alias, "laptop");

        let (eid, alias) = resolve_add(&peers, &id_a[..8]).unwrap();
        assert_eq!(eid, id_a);
        assert_eq!(alias, "laptop");

        let absent = hex_id(0x33);
        let (eid, alias) = resolve_add(&peers, &absent).unwrap();
        assert_eq!(eid, absent);
        assert_eq!(alias, absent);

        let upper = absent.to_ascii_uppercase();
        let (eid, alias) = resolve_add(&[], &upper).unwrap();
        assert_eq!(eid, absent);
        assert_eq!(alias, absent);

        assert!(resolve_add(&peers, "nope").is_err());
        assert!(resolve_add(&[], "laptop").is_err());
        let ambiguous = vec![peer("phone", &id_a), peer("phone", &id_b)];
        assert!(resolve_add(&ambiguous, "phone").is_err());
        let id_c = format!("ab{}", "0".repeat(62));
        let id_d = format!("ac{}", "0".repeat(62));
        assert!(resolve_add(&[peer("one", &id_c), peer("two", &id_d)], "a").is_err());

        let devices = vec![
            TrustedDevice {
                endpoint_id: id_a.clone(),
                alias: "laptop".into(),
            },
            TrustedDevice {
                endpoint_id: id_b.clone(),
                alias: "phone".into(),
            },
        ];
        assert_eq!(resolve_remove(&devices, "phone").unwrap(), id_b);
        assert_eq!(resolve_remove(&devices, &id_a[..8]).unwrap(), id_a);
        assert!(resolve_remove(&devices, "nope").is_err());
        let dup = vec![
            TrustedDevice {
                endpoint_id: id_a,
                alias: "phone".into(),
            },
            TrustedDevice {
                endpoint_id: id_b,
                alias: "phone".into(),
            },
        ];
        assert!(resolve_remove(&dup, "phone").is_err());
    }

    struct Rm<'a>(&'a Path);

    impl Drop for Rm<'_> {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(self.0);
        }
    }
}
