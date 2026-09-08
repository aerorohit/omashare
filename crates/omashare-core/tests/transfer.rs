use std::path::PathBuf;
use std::time::Duration;

use iroh::TransportAddr;
use omashare_core::events::NodeConfig;
use omashare_core::protocol::{DeviceKind, MeteredPolicy, DISCOVERY_PORT};
use omashare_core::{DiscoverySource, NearbyPeer, Node, NodeEvent};

fn test_config(root: PathBuf, alias: &str, port: u16) -> NodeConfig {
    NodeConfig {
        alias: alias.into(),
        device_kind: DeviceKind::Desktop,
        pin: None,
        receive_dir: root.join("inbox"),
        data_dir: root.join("data"),
        discovery_port: port,
        enable_mdns: false,
        enable_subnet_scan: false,
        enable_lan_beacon: false,
        metered_policy: MeteredPolicy::Allow,
        use_relays: false,
    }
}

fn ip_hints(node: &Node) -> Vec<String> {
    node.endpoint_addr()
        .addrs
        .iter()
        .filter_map(|a| match a {
            TransportAddr::Ip(sa) => Some(sa.to_string()),
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs unrestricted UDP (iroh bind); run locally with: cargo test -p omashare-core -- --ignored"]
async fn offer_accept_and_blob_resume_path() {
    let a_dir = tempfile::tempdir().unwrap();
    let b_dir = tempfile::tempdir().unwrap();
    let port_a = DISCOVERY_PORT + 40;
    let port_b = DISCOVERY_PORT + 41;
    let a = tokio::time::timeout(
        Duration::from_secs(15),
        Node::start(test_config(a_dir.path().to_path_buf(), "alpha", port_a)),
    )
    .await
    .expect("start alpha timeout")
    .unwrap();
    let b = tokio::time::timeout(
        Duration::from_secs(15),
        Node::start(test_config(b_dir.path().to_path_buf(), "beta", port_b)),
    )
    .await
    .expect("start beta timeout")
    .unwrap();

    a.inject_peer(NearbyPeer {
        alias: "beta".into(),
        device_kind: DeviceKind::Desktop,
        endpoint_id: b.endpoint_id(),
        pin_required: false,
        source: DiscoverySource::Manual,
        addrs: ip_hints(&b),
    })
    .await;
    b.inject_peer(NearbyPeer {
        alias: "alpha".into(),
        device_kind: DeviceKind::Desktop,
        endpoint_id: a.endpoint_id(),
        pin_required: false,
        source: DiscoverySource::Manual,
        addrs: ip_hints(&a),
    })
    .await;

    let mut events = b.subscribe();
    let payload = a_dir.path().join("hello.txt");
    tokio::fs::write(&payload, b"omashare-walk-away").await.unwrap();

    let send = tokio::spawn({
        let id = b.endpoint_id();
        async move { a.send_files(&id, &[payload]).await }
    });

    let session = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match events.recv().await.unwrap() {
                NodeEvent::IncomingOffer { session_id, .. } => break session_id,
                _ => {}
            }
        }
    })
    .await
    .expect("incoming offer");

    b.accept_offer(&session).await.unwrap();

    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match events.recv().await.unwrap() {
                NodeEvent::TransferComplete { saved_to, .. } => {
                    let saved = saved_to.expect("path");
                    let bytes = tokio::fs::read(saved).await.unwrap();
                    assert_eq!(bytes, b"omashare-walk-away");
                    break;
                }
                NodeEvent::TransferFailed { error, .. } => panic!("{error}"),
                _ => {}
            }
        }
    })
    .await
    .expect("complete");

    send.await.unwrap().unwrap();
    let _ = b.shutdown().await;
}

#[test]
fn default_discovery_port_not_localsend() {
    assert_ne!(DISCOVERY_PORT, 53317);
}

#[test]
fn offer_wire_json_roundtrip() {
    use omashare_core::protocol::{FileOffer, MeteredPolicy, WireMsg};
    use uuid::Uuid;
    let msg = WireMsg::Offer {
        session_id: Uuid::nil(),
        alias: "phone".into(),
        files: vec![FileOffer {
            name: "a.bin".into(),
            size: 3,
            hash: "abcd".into(),
        }],
        pin: Some("1234".into()),
        metered_policy: MeteredPolicy::Ask,
    };
    let back: WireMsg = serde_json::from_slice(&serde_json::to_vec(&msg).unwrap()).unwrap();
    match back {
        WireMsg::Offer { pin, .. } => assert_eq!(pin.as_deref(), Some("1234")),
        _ => panic!(),
    }
    let done = WireMsg::Complete {
        session_id: Uuid::nil(),
    };
    let back: WireMsg = serde_json::from_slice(&serde_json::to_vec(&done).unwrap()).unwrap();
    assert!(matches!(back, WireMsg::Complete { .. }));
    let prog = WireMsg::Progress {
        session_id: Uuid::nil(),
        bytes_done: 10,
        bytes_total: 100,
    };
    let back: WireMsg = serde_json::from_slice(&serde_json::to_vec(&prog).unwrap()).unwrap();
    match back {
        WireMsg::Progress {
            bytes_done,
            bytes_total,
            ..
        } => {
            assert_eq!(bytes_done, 10);
            assert_eq!(bytes_total, 100);
        }
        _ => panic!(),
    }
}
