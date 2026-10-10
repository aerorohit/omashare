# Omashare

Omashare shares files between two devices running Omashare. Linux to Linux, Linux to Android, and Android to Android use the same protocol. Devices find each other nearby on the local network, and over Bluetooth on Linux. You pick files and a device. The other person accepts or declines, and accepted files are saved on the receiver. On Linux, received files go to `~/Downloads/omashare`.

A device you mark trusted will auto-accept future files from you. Everyone else needs an explicit accept. If Wi-Fi drops mid-transfer, the transfer can continue over another path, including a relay. `--offline` keeps it on the local network only.

## Install

Needs Rust stable and Linux.

```bash
git clone https://github.com/aerorohit/omashare && cd omashare
cargo build --release -p omashare
install -Dm755 target/release/omashare ~/.local/bin/omashare
```

`~/.local/bin` must be on `PATH`. Then run `omashare`.

## Daily use

`omashare` is one binary. In a terminal it is a small TUI: a nearby list.

`j` and `k`, or the arrow keys, move the selection. `s` opens the system file picker (xdg-desktop-portal, then zenity). Enter sends to the selected device. `t` trusts or untrusts that device. `a` and `d` accept or decline an incoming offer. `c` cancels. `q` quits.

Started by systemd, or by the optional bar plugin, the same binary stays running so the machine can still receive. If you open the TUI while that background process is already up, the TUI attaches to it.

Logs while the TUI is open go to `$XDG_DATA_HOME/omashare/omashare.log`.

```bash
omashare send FILE --to NAME
omashare cancel [SESSION_ID]
omashare accept SESSION_ID
omashare decline SESSION_ID
omashare trust list
omashare trust add ENDPOINT_OR_ALIAS
omashare trust remove ENDPOINT_OR_ALIAS
```

`send` sends `FILE` to the device named `NAME`. `cancel` stops one session, or every open session when you leave the id off. `accept` and `decline` answer an incoming offer. `trust list` shows trusted devices. `trust add` and `trust remove` take an endpoint id, or a nearby alias when exactly one device uses that name.

`--offline` stays on the local network. `--pin 1234` requires senders to pass that PIN. `--no-ble` skips Bluetooth discovery. `--auto-accept` is for unattended machines: it accepts everyone and leaves the trust list unchanged.

Trusted devices are stored in `$XDG_DATA_HOME/omashare/trusted.json`.

## How a transfer works

Devices discover each other nearby. You offer files to one of them. They accept, or a trusted device accepts on its own. The files land in Downloads. If the network changes mid-transfer, the transfer can continue on another path.

## Optional

**systemd user service.** Copy [`contrib/omashare.service`](contrib/omashare.service) to `~/.config/systemd/user/` and run:

```bash
systemctl --user enable --now omashare
```

The unit expects the binary at `~/.local/bin/omashare`.

**Omarchy.** If you use Omarchy, copy [`contrib/omarchy-plugin`](contrib/omarchy-plugin) to `~/.config/omarchy/plugins/aerorohit.omashare` and enable Omashare on the bar. The plugin uses the `omashare` binary already on `PATH`. You can share files without Omarchy and without the plugin.

## Android

The Android app is a separate repository: <https://github.com/aerorohit/omashare-android>

Install that app on a phone to send and receive with a Linux machine or with another phone. Build steps live in that repo.

## Protocol

Discovery uses port 53318 and multicast `224.0.0.169`. Bluetooth carries the handshake. The file payload goes over [Iroh](https://github.com/n0-computer/iroh).
