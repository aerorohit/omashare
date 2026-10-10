# Omashare CLI — DESIGN

Desktop node for Omashare. One binary, `omashare`: a terminal runs the TUI
(and a node, if nothing is already listening); no terminal runs the node only.
This repo owns the protocol, Rust core, UniFFI surface, and Linux client.
The Android UI lives in the sibling repo `omashare-android`.

First iteration: **continuous sharing**. Nearby devices stay discoverable and
can send or receive at any time. No Wi-Fi / cellular gates.

## What we already have

Sample CLI + core (`omashare-core`, `omashare-cli`, `omashare-ffi`):

- LAN discovery: UDP multicast `224.0.0.169:53318`, HTTP `GET /omashare/info`,
  optional subnet scan, mDNS `_omashare._udp.local.`
- BLE discovery: advertise service UUID `6f6d6173-6861-7265-0000-000000005318`,
  GATT read of Presence JSON on `…5319`, then iroh WAN/relay (not bytes over BLE).
- Offer/consent over Iroh ALPN `omashare/offer/0` (`Offer` / `Accept` / `Decline` / `Cancel`)
- Bytes over `iroh-blobs` (resume, magicsock, optional n0 relay)
- Commands: no subcommand is the TUI (terminal) or the node (no terminal); clients are `send`, `trust`, `accept`, `decline`, `cancel`, `nearby`, `status`
- User systemd unit: `contrib/omashare.service`

Not LocalSend-compatible. Not AirDrop-compatible. Same *shape* as both.

## Lessons from AirDrop and LocalSend

### AirDrop (target UX, not the radio stack)

- A **daemon** (`sharingd`) is always listening. The picker is not the node.
- Flow: discover → **Ask** (metadata) → human accept/decline → upload.
- Receiver never needs to “start receive mode.”
- BLE is **discovery only** (AirDrop shape): learn `EndpointId` + home relay,
  then magicsock for offer/blobs. We do not implement AWDL or send files over BLE.

### LocalSend (target product loop)

- One process is both server and client. Peers appear as a live list.
- Send: pick files → pick device → wait for accept → stream.
- Receive: notification + names/sizes → Accept / Decline → save to a known folder.
- Same binary on every OS, so **phone ↔ phone** is the same as phone ↔ PC.
- Hybrid discovery (multicast + TCP/HTTP) because UDP-only fails on many LANs.
- We already use a distinct port (**53318**) so we do not collide with LocalSend (53317).

### What we copy

| Behavior | v1 |
| --- | --- |
| Always-on receiver | Agent runs in the background |
| Consent before bytes | TUI / notification Accept · Decline. Trusted devices auto-accept. |
| Live nearby list | Agent maintains peers; TUI only renders |
| Native file pick | Portal / file manager, not an in-TUI tree |
| Multi-file offer | One session, many files |
| Progress + cancel | Session id, bytes, path (LAN / WAN / relay) |
| Phone ↔ phone | Same protocol; no PC required |

### What we defer

- PIN as a required UX (keep optional `--pin` in the agent)
- Pairing ceremony, contacts sync, folders-as-trees, clipboard share

LocalSend compatibility and metered / Wi-Fi-only prompts are **out**. Leave `MeteredPolicy::Allow` and ignore `MeteredConsentNeeded`. Do not add a LocalSend dialect.

## Architecture

```
┌─────────────┐                    ┌──────────────────┐
│ omashare    │  unix socket JSON  │ omashare         │
│ (TUI)       │←──────────────────→│ (no TTY)         │
│ Omarchy     │                    │ systemd --user   │
│ shell plugin│                    │  trusted.json    │
└──────┬──────┘                    │  omashare-core   │
       │ xdg file portal           └────────┬─────────┘
       ▼                                    │ LAN / BLE / relay
  native file manager                       ▼
                                   phone, PC, or another node
```

One Iroh endpoint per machine. The TUI must **never** start a second node.
Same binary either way: a terminal attaches or embeds the node; no terminal is the node.

### Processes

| Binary / mode | Role |
| --- | --- |
| `omashare` (no terminal) | Long-lived node. Discovery, offers, transfers, notifications. Systemd and the plugin launch this. |
| `omashare` (terminal) | TUI. Attach if a node is listening; otherwise start an in-process node and shut it down when the TUI exits. |
| `omashare send …` | Attach to the node (or a one-shot node) and send. |
| `omashare-ffi` | Unchanged role: Android loads the same core. |

### IPC

Unix socket: `$XDG_RUNTIME_DIR/omashare.sock`.

JSON lines, one object per message.

Client → agent:

```json
{"cmd":"nearby"}
{"cmd":"send","paths":["/a","/b"],"to":"<endpoint_id or alias>","pin":null}
{"cmd":"accept","session_id":"..."}
{"cmd":"decline","session_id":"..."}
{"cmd":"cancel","session_id":"..."}
{"cmd":"status"}
```

Agent → client: the existing `NodeEvent` enum plus a `hello` with alias, endpoint id,
receive dir.

Single writer for the node; TUI is a subscriber. Desktop notifications still fire
when no TUI is attached (AirDrop-style).

### File picker (“search”)

Do not build a file browser in the TUI.

On **Send** or the search key:

1. Open the **native file manager** via `xdg-desktop-portal` `FileChooser`
   (`OpenFile`, multiple selection).
2. Fall back: `xdg-desktop-portal-termfilechooser`, then `zenity --file-selection`,
   then typed paths.
3. Hand the resulting paths to the agent `send` command.

This matches Android (`ACTION_OPEN_DOCUMENT` / `GetMultipleContents`) and Omarchy
desktop habits (portal + Hyprland).

## TUI (v1)

Library: **ratatui** + crossterm. One screen, no settings maze.

```
  omashare  ·  hostname  ·  listening
  ────────────────────────────────────
  Nearby                         Inbox
  ● phone-pixel     mobile       (empty)
  ● other-laptop    desktop
  ────────────────────────────────────
  transfer  photo.jpg  12.4 / 40 MB  31%  LAN
  ────────────────────────────────────
  [s] send   [enter] send to selected   [a] accept   [d] decline   [q] quit
```

Behavior:

- **Nearby** is the focus list. Enter sends the last picked files (or opens the picker first).
- Incoming offer: modal in the TUI *and* a `notify-send` / `notify-rust` action if possible.
- Default receive dir: `~/Downloads/omashare`.
- Quitting the TUI leaves an already-running node up. A node that the TUI started shuts down with it.

Out of scope for v1 TUI: theming engine, mouse-heavy layouts, in-app receive-dir browser.

## Agent details

- `Type=simple` user unit (existing `contrib/omashare.service`).
- Blanket auto-accept stays **off**. `--auto-accept` remains for kiosks and tests only.
- Trusted devices auto-accept. Everyone else still needs Accept.
- `MeteredPolicy::Allow`. No metering UI. Ignore `MeteredConsentNeeded`.
- Relays **on** by default so a walk from Wi-Fi to cellular does not kill the session.
- BLE advertise + GATT Presence while the agent is running (BlueZ). `--no-ble` skips it.
- Identity: persist Iroh secret in `$XDG_DATA_HOME/omashare`.
- Logs: journald via systemd; `--verbose` for the TUI attach session.

## Protocol (owned here)

Do not change wire constants in v1 unless a bug blocks phone↔phone:

- Discovery port **53318**, multicast `224.0.0.169`
- BLE service `6f6d6173-6861-7265-0000-000000005318`, presence char `…5319`
  (JSON `Presence`: `endpoint_id`, alias, kind, pin, `addrs` with relay URLs)
- Offer ALPN `omashare/offer/0`
- Payload ALPN `iroh-blobs`

FFI gaps the Android repo needs in the same iteration:

- `send_files(peer, paths[])` (multi-file)
- Stable `nearby` + events (already JSON)
- Optional: do not require staging if the path is readable (desktop)

Keep `Metered*` messages on the wire; clients may no-op them.

## Trusted devices

Local allowlist. Not a pairing protocol and not on the wire.

- File: `$XDG_DATA_HOME/omashare/trusted.json`
- Records: `{ "endpoint_id", "alias" }`. Match **only** `endpoint_id` (stable Iroh id). Alias is a label.
- On `IncomingOffer`, if `from_endpoint` is in the file, the agent calls `accept_offer`. No TUI required.
- Unknown senders still get the notification and must be accepted.
- CLI: `omashare trust list`, `omashare trust add <endpoint-or-alias>`, `omashare trust remove <endpoint-or-alias>`.
- IPC (same socket): `trust_list`, `trust_add` `{endpoint_id, alias}`, `trust_remove` `{endpoint_id}`.
- Adding by alias resolves one nearby peer. Ambiguous or unknown names fail.
- `--auto-accept` still accepts everyone and does not write the trust file.

## Omarchy shell plugin

Ship it in this repo at `contrib/omarchy-plugin/`. Copy that folder to `~/.config/omarchy/plugins/aerorohit.omashare` (the manifest is not at the git root). It is a Quickshell bar widget, not a second node. Validate with `omarchy plugin validate contrib/omarchy-plugin`.

Manifest: `schemaVersion` 1, id `aerorohit.omashare`, kind `bar-widget`, entry `Panel.qml`. Follow the Tailscale panel shape (`Panel`, `moduleName`, theme from `qs.Ui` / `qs.Commons`). Do not edit `/usr/share/omarchy/`.

The panel talks only to the node socket (`$XDG_RUNTIME_DIR/omashare.sock`, JSON lines) or to the `omashare` binary. It must not open its own Iroh endpoint.

| Action | How |
| --- | --- |
| Start | If `omashare` is on `PATH` and the socket is down, spawn `setsid -- omashare` (no TTY, so that process is the node). If the socket is up, do nothing. If `omashare` is not on `PATH`, show that it is not installed and do not spawn. |
| Nearby | `{"cmd":"nearby"}` |
| Accept / Decline | `accept` / `decline` with `session_id` from the offer event |
| Send | Portal or zenity file pick, then `{"cmd":"send","paths":[...],"to":"<endpoint>"}` |
| Trust | Toggle calls `trust_add` / `trust_remove` for that peer's `endpoint_id` |

Show node up/down, the nearby list, an incoming-offer row (names and Accept / Decline), and a trust mark on trusted peers. Sending while the node is down starts it first, then sends.

## Implementation order

1. **Node IPC** — socket; `omashare` with no terminal is the node.
2. **CLI attach** — `peers` / `send` / `accept` talk to the running agent.
3. **TUI** — nearby, offer modal, progress, portal file pick.
4. **systemd** — install path, documentation; disable auto-accept in the unit.
5. **Multi-file send** in core + FFI (Android depends on this).
6. **Manual matrix** — PC↔PC, PC↔phone, phone↔phone, kill TUI during transfer.
7. **Trusted devices** — persist endpoint ids; agent auto-accepts only those.
8. **Omarchy plugin** — bar panel: start `omashare`, nearby, accept, send, trust toggle.

## Non-goals

- LocalSend or AirDrop compatibility
- Metered-network prompts or Wi-Fi-only transfers
- A pairing ceremony or contacts sync (trusted devices are a local endpoint allowlist)
- Shipping the Android APK from this repo (bindings only)

## Code rules

- Protocol and transfer logic stay in `omashare-core`. CLI/TUI are thin.
- No second Iroh endpoint on one machine.
- Prefer small modules: `agent`, `ipc`, `tui`, `picker`.
- Fast path: discovery stays cheap; hashing stays off the UI thread (already true in core).
