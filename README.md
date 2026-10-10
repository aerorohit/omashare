# Omashare

Send files between Linux and Android on the local network, with a relay fallback when Wi-Fi drops. One command, `omashare`, is the TUI and the node. Not LocalSend-compatible. No VPN.

## Installation

Requires Rust (stable) and Linux.

```bash
git clone https://github.com/aerorohit/omashare && cd omashare
cargo build --release -p omashare
install -Dm755 target/release/omashare ~/.local/bin/omashare
```

`~/.local/bin` must be on `PATH`.

`omashare` in a terminal starts the TUI. With no terminal the same binary stays up as the node. If a node is already running, the TUI attaches to it.

## Commands

```bash
omashare send photo.jpg --to other-laptop
omashare cancel
omashare cancel SESSION_ID
omashare accept SESSION_ID
omashare decline SESSION_ID
omashare trust list
omashare trust add ENDPOINT_OR_ALIAS
omashare trust remove ENDPOINT_OR_ALIAS
```

`send` uses the running node, or a one-shot node if it is down. `cancel` with no id cancels every open session. `trust add` takes an endpoint id, or exactly one nearby alias.

Incoming files go to `~/Downloads/omashare`.

`--offline` skips relays and stays on the LAN. `--pin 1234` requires senders to pass that PIN. `--no-ble` skips Bluetooth discovery. `--auto-accept` is for kiosks only and does not write the trust file.

Trusted devices auto-accept and live in `$XDG_DATA_HOME/omashare/trusted.json`.

## TUI

```
[s] send   [enter] send to selected   [t] trust   [a] accept   [d] decline   [c] cancel   [q] quit
```

`j` / `k` (or the arrow keys) move the selection. `s` opens the native file manager (xdg-desktop-portal, then zenity). `enter` sends the picked files to the selected device. `t` trusts or untrusts that device. `a` and `d` accept or decline an incoming offer. `c` cancels the transfer. `q` quits.

TUI logs go to `$XDG_DATA_HOME/omashare/omashare.log`, not the screen.

## systemd

Optional. Copy [`contrib/omashare.service`](contrib/omashare.service) to `~/.config/systemd/user/`. The unit starts `~/.local/bin/omashare`.

```bash
systemctl --user enable --now omashare
```

The unit has no terminal, so it is the node only. Accept offers from the TUI, the plugin, or `omashare accept`.

## Omarchy plugin

Optional. Omashare does not require Omarchy.

If you use Omarchy and `omashare` is already on `PATH`, copy [`contrib/omarchy-plugin`](contrib/omarchy-plugin) to `~/.config/omarchy/plugins/aerorohit.omashare` and enable Omashare on the bar. The plugin only talks to the installed `omashare` binary.

## Android

The Android app is a separate repo: [github.com/aerorohit/omashare-android](https://github.com/aerorohit/omashare-android). It uses the same protocol. Build instructions live in that repo.

## Protocol

- Discovery UDP/TCP port **53318**, multicast `224.0.0.169`, HTTP `GET /omashare/info`
- BLE discovery: service UUID `6f6d6173-6861-7265-0000-000000005318`, GATT Presence JSON on `…5319`. Handshake only; files still go LAN / WAN / n0 relay.
- Offer ALPN `omashare/offer/0`
- Payload ALPN `iroh-blobs`
