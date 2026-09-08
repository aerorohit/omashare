# Omashare

AirDrop-like file send on [Iroh](https://iroh.computer). A background **agent** stays
discoverable; a small **TUI** attaches to it. Nearby devices meet on the LAN
(multicast + HTTP subnet scan + mDNS) or over **Bluetooth** (GATT Presence
handshake). After accept, bytes go over `iroh-blobs` on magicsock so the same
transfer can keep going when Wi-Fi drops (hole punch, then n0 relay).

Not LocalSend-compatible. No system VPN.

This repo owns the protocol, Rust core, UniFFI surface, and Linux client.
The Android UI lives in the sibling repo [`omashare-android`](../omashare-android).

## Linux

```bash
cargo build -p omashare --release
# Background node (auto-accept off). Notifications for incoming offers.
./target/release/omashare agent
# TUI: nearby list, inbox, progress. Attaches to the agent if it is running.
./target/release/omashare
# or: ./target/release/omashare tui
# Send via the agent (or a one-shot node if the agent is down)
./target/release/omashare send photo.jpg --to other-laptop
```

Incoming files go to `~/Downloads/omashare`. `--offline` skips n0 relays (LAN only). `--pin 1234` requires senders to pass that PIN. `--auto-accept` is for kiosks and tests only. `--no-ble` skips Bluetooth discovery (BlueZ).

The TUI opens the native file manager (xdg-desktop-portal, then zenity) for **s** / search. It does not browse files itself.

User systemd unit: copy [`contrib/omashare.service`](contrib/omashare.service) to `~/.config/systemd/user/`, put the binary on `~/.local/bin/omashare`, then `systemctl --user enable --now omashare`. The unit runs `omashare agent` (consent required).

IPC: `$XDG_RUNTIME_DIR/omashare.sock` (JSON lines). One Iroh endpoint per machine.

Walk-away test: start `omashare agent`, send a few MB from the phone, turn Wi-Fi off with mobile data on. Progress should continue; logs show `path=relay` (or WAN).

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
