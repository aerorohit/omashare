# Omashare CLI — DESIGN

Desktop node for Omashare: a long-running background agent plus a small TUI.
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
- Commands: `listen` (REPL), `daemon` (auto-accept), `send FILE`
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
| Consent before bytes | TUI / notification Accept · Decline (default **off** for auto-accept) |
| Live nearby list | Agent maintains peers; TUI only renders |
| Native file pick | Portal / file manager, not an in-TUI tree |
| Multi-file offer | One session, many files |
| Progress + cancel | Session id, bytes, path (LAN / WAN / relay) |
| Phone ↔ phone | Same protocol; no PC required |

### What we defer

- Wi-Fi-only / metered prompts (protocol already has `MeteredPolicy`; keep `Allow`)
- PIN as a required UX (keep optional `--pin` in the agent)
- Pairing, contacts-only visibility, folders-as-trees, clipboard share
- LocalSend / AirDrop interop

## Architecture

```
┌─────────────┐  unix socket (JSON)  ┌──────────────────┐
│ omashare tui│ ←─────────────────────→ │ omashare agent   │
│ (ratatui)   │                        │ (systemd --user) │
└──────┬──────┘                        │                  │
       │ xdg file portal               │  omashare-core  │
       ▼                               │  discovery+iroh  │
  native file manager                  └────────┬─────────┘
                                                │  LAN / BLE handshake / relay
                                                ▼
                                      phone, PC, or another agent
```

One Iroh endpoint per machine. The TUI must **never** start a second node.

### Processes

| Binary / mode | Role |
| --- | --- |
| `omashare agent` | Background node. Discovery, offers, transfers, notifications. |
| `omashare` (TUI) | If the agent is up, attach. If not, start an in-process node for that session. |
| `omashare send …` | Attach to agent (or one-shot node) and send. |
| `omashare-ffi` | Unchanged role: Android loads the same core. |

Keep `daemon` as an alias of `agent` during the transition.

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
- Agent stays running when the TUI quits.

Out of scope for v1 TUI: theming engine, mouse-heavy layouts, in-app receive-dir browser.

## Agent details

- `Type=simple` user unit (existing `contrib/omashare.service`).
- Auto-accept: **off** by default. Continuous sharing means always reachable, not
  silent accept. `--auto-accept` remains for kiosks / tests.
- `MeteredPolicy::Allow`. Ignore `MeteredConsentNeeded` in the TUI.
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

## Implementation order

1. **Agent IPC** — extract node from the REPL; socket + `omashare agent`.
2. **CLI attach** — `peers` / `send` / `accept` talk to the running agent.
3. **TUI** — nearby, offer modal, progress, portal file pick.
4. **systemd** — install path, documentation; disable auto-accept in the unit.
5. **Multi-file send** in core + FFI (Android depends on this).
6. **Manual matrix** — PC↔PC, PC↔phone, phone↔phone, kill TUI during transfer.

## Non-goals (v1)

- Restricting transfers to Wi-Fi
- A full settings UI
- Compatibility with LocalSend or AirDrop
- Shipping the Android APK from this repo (bindings only)

## Code rules

- Protocol and transfer logic stay in `omashare-core`. CLI/TUI are thin.
- No second Iroh endpoint on one machine.
- Prefer small modules: `agent`, `ipc`, `tui`, `picker`.
- Fast path: discovery stays cheap; hashing stays off the UI thread (already true in core).
