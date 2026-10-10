# Omashare

AirDrop-like file send on [Iroh](https://iroh.computer). In a terminal, `omashare`
is the TUI and the node. With no terminal it is the node only. Nearby devices meet on the LAN
(multicast + HTTP subnet scan + mDNS) or over **Bluetooth** (GATT Presence
handshake). After accept, bytes go over `iroh-blobs` on magicsock so the same
transfer can keep going when Wi-Fi drops (hole punch, then n0 relay).

No system VPN.

This repo owns the protocol, Rust core, UniFFI surface, and Linux client.
The Android UI lives in the sibling repo [`omashare-android`](../omashare-android).

## Linux

```bash
cargo build -p omashare --release
# In a terminal: the TUI and the node. Attaches if a node is already listening;
# otherwise starts one and shuts it down when the TUI exits.
./target/release/omashare
# With no terminal (the plugin or the systemd unit): the node only.
# Send via the running node (or a one-shot node if it is down)
./target/release/omashare send photo.jpg --to other-laptop
# Cancel the current transfer (omit the id to cancel every open session)
./target/release/omashare cancel
./target/release/omashare cancel SESSION_ID
# Accept or decline an offer on the running node
./target/release/omashare accept SESSION_ID
./target/release/omashare decline SESSION_ID
# Trust list / add / remove. Trusted peers are auto-accepted.
./target/release/omashare trust list
./target/release/omashare trust add ENDPOINT_OR_ALIAS
./target/release/omashare trust remove ENDPOINT_OR_ALIAS
```

Incoming files go to `~/Downloads/omashare`. `--offline` skips n0 relays (LAN only). `--pin 1234` requires senders to pass that PIN. `--auto-accept` is for kiosks and tests only and does not write the trust file. Trusted endpoint ids in `$XDG_DATA_HOME/omashare/trusted.json` are auto-accepted. `--no-ble` skips Bluetooth discovery (BlueZ).

The TUI opens the native file manager (xdg-desktop-portal, then zenity) for **s** / search. It does not browse files itself. **c** cancels the in-progress transfer.

User systemd unit: copy [`contrib/omashare.service`](contrib/omashare.service) to `~/.config/systemd/user/`, put the binary on `~/.local/bin/omashare`, then `systemctl --user enable --now omashare`. The unit runs `omashare` with no terminal, so it is the node only. Offers need the TUI, the plugin, or `omashare accept`.

IPC: `$XDG_RUNTIME_DIR/omashare.sock` (JSON lines). One Iroh endpoint per machine.

Omarchy bar plugin: [`contrib/omarchy-plugin`](contrib/omarchy-plugin) (`aerorohit.omashare`).
Copy that folder to `~/.config/omarchy/plugins/aerorohit.omashare`.
The panel uses the installed `omashare` on `PATH`. It starts the node when the socket is missing, and does not start a second node when the socket is already there.
It lists nearby devices, accepts or declines offers, sends files you pick, and toggles trust.

Walk-away test: start `omashare` with no terminal (or the systemd unit), send a few MB from the phone, turn Wi-Fi off with mobile data on. Progress should continue; logs show `path=relay` (or WAN).

## Android

Use the sibling repo `../omashare-android`. Core is the same Rust crate via UniFFI
(`crates/omashare-ffi`). From that repo, generate bindings and a `jniLibs` `.so` with NDK:

```bash
cargo run -p omashare-ffi --bin uniffi-bindgen -- generate \
  --library target/release/libomashare_ffi.so \
  --language kotlin \
  --out-dir ../omashare-android/app/src/main/java
# cargo ndk -t arm64-v8a build -p omashare-ffi --release
```

## Protocol

- Discovery UDP/TCP port **53318**, multicast `224.0.0.169`, HTTP `GET /omashare/info`
- BLE discovery: service UUID `6f6d6173-6861-7265-0000-000000005318`, GATT
  Presence JSON on `…5319`. Handshake only; files still go LAN / WAN / n0 relay.
- Offer ALPN `omashare/offer/0`
- Payload ALPN `iroh-blobs`
