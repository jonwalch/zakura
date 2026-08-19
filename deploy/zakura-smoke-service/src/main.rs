//! Smoke-test application that embeds `zakurad` and registers a custom Zakura
//! p2p service.
//!
//! The app exists to prove one property in a live network: a node reaches
//! another node for a single service. It advertises `zakura.smoke_echo.v1`,
//! seeks the same id when told to, and exchanges ping/pong frames with every
//! peer that opens the stream. Peers that do not run this service never open
//! it, so the pong counters distinguish "connected" from "connected *for this
//! service*".
//!
//! Usage:
//!
//! ```console
//! zakura-smoke-service --config /root/zakura.toml
//! ```
//!
//! Environment:
//!
//! - `SMOKE_PROVIDE=0` stops the app advertising the service (default: advertise).
//! - `SMOKE_SEEK=0` stops the discovery dialer preferring peers that advertise
//!   it (default: prefer).
//! - `SMOKE_PING_INTERVAL_SECS` sets the ping period (default: 15).
//! - `SMOKE_STATUS_PATH` sets the status JSON path (default:
//!   `/root/logs/smoke-service.json`).

use std::{
    collections::BTreeMap,
    fmt::Write as _,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use color_eyre::{
    eyre::{eyre, WrapErr},
    Report,
};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use zakura_network::zakura::{
    CustomService, Frame, FramedRecv, FramedSend, Peer, Service, Stream, StreamMode, ZakuraConnId,
    ZakuraPeerId, ZakuraServiceId,
};
use zakurad::config::ZakuradConfig;

/// Service id this app advertises and seeks.
const SMOKE_SERVICE_ID: &str = "zakura.smoke_echo.v1";

/// Ordered stream the service owns.
///
/// Kind 64 and capability bit 16 are outside every native service's range
/// (`ZAKURA_CAP_LEGACY_GOSSIP` .. `ZAKURA_CAP_HEADER_SYNC`, bits 0-5), so
/// registering it cannot collide with a native stream kind or capability.
const SMOKE_STREAM: Stream = Stream {
    kind: 64,
    version: 1,
    frame_cap: 64 * 1024,
    capability: 1 << 16,
    mode: StreamMode::Ordered,
};

/// Frame type for a ping carrying the sender's millisecond timestamp.
const MESSAGE_TYPE_PING: u16 = 1;
/// Frame type for the echoed ping.
const MESSAGE_TYPE_PONG: u16 = 2;

/// Counters the status file reports.
#[derive(Debug, Default)]
struct Counters {
    /// Stream sessions opened, i.e. peers reached *for this service*.
    sessions_opened: AtomicU64,
    /// Peers the transport handed this service, whether or not the stream opened.
    peers_added: AtomicU64,
    /// Peers the transport removed.
    peers_removed: AtomicU64,
    /// Pings sent to peers.
    pings_sent: AtomicU64,
    /// Pings answered by peers.
    pongs_received: AtomicU64,
    /// Pings answered for peers.
    pings_answered: AtomicU64,
    /// Last observed round trip, in milliseconds.
    last_rtt_millis: AtomicU64,
    /// Peers with a live stream session, keyed by peer id.
    live_peers: Mutex<BTreeMap<String, u64>>,
}

impl Counters {
    fn snapshot(&self) -> serde_json::Value {
        let live_peers = self
            .live_peers
            .lock()
            .expect("smoke service peer map is never poisoned");
        serde_json::json!({
            "service_id": SMOKE_SERVICE_ID,
            "stream_kind": SMOKE_STREAM.kind,
            "unix_millis": unix_millis(),
            "sessions_opened": self.sessions_opened.load(Ordering::Relaxed),
            "peers_added": self.peers_added.load(Ordering::Relaxed),
            "peers_removed": self.peers_removed.load(Ordering::Relaxed),
            "pings_sent": self.pings_sent.load(Ordering::Relaxed),
            "pongs_received": self.pongs_received.load(Ordering::Relaxed),
            "pings_answered": self.pings_answered.load(Ordering::Relaxed),
            "last_rtt_millis": self.last_rtt_millis.load(Ordering::Relaxed),
            "live_peer_count": live_peers.len(),
            "live_peers": live_peers.clone(),
        })
    }
}

/// The custom service itself.
#[derive(Debug)]
struct SmokeEchoService {
    counters: Arc<Counters>,
    ping_interval: Duration,
}

impl Service for SmokeEchoService {
    fn name(&self) -> &'static str {
        "smoke-echo"
    }

    fn streams(&self) -> &[Stream] {
        std::slice::from_ref(&SMOKE_STREAM)
    }

    /// Hold the connection while this service has a live session on it.
    ///
    /// The trait default is `false`. A custom service that keeps the default
    /// never claims its connection, so discovery closes it as a discovery-only
    /// connection (`closed.neutral`, `reason: discovery_exchange_complete`) the
    /// moment the exchange finishes -- about a millisecond after the custom
    /// stream opens. Every embedding application that wants a durable session
    /// has to override this.
    fn owns_connection_for_peer(&self, peer: &ZakuraPeerId, conn_id: ZakuraConnId) -> bool {
        self.counters
            .live_peers
            .lock()
            .expect("smoke service peer map is never poisoned")
            .get(&peer_id_hex(peer))
            .is_some_and(|live_conn_id| *live_conn_id == conn_id)
    }

    fn add_peer(&self, mut peer: Peer) {
        let peer_id = peer_id_hex(&peer.id);
        self.counters.peers_added.fetch_add(1, Ordering::Relaxed);
        let Some((recv, send)) = peer.take_stream(SMOKE_STREAM.kind) else {
            // The peer negotiated the connection but not this service's stream.
            info!(
                peer = %peer_id,
                negotiated = peer.negotiated,
                "smoke-echo peer arrived without the smoke stream",
            );
            return;
        };
        self.counters.sessions_opened.fetch_add(1, Ordering::Relaxed);
        info!(
            peer = %peer_id,
            conn_id = peer.conn_id,
            direction = ?peer.direction,
            remote_ip = ?peer.remote_ip,
            "smoke-echo stream opened",
        );
        self.counters
            .live_peers
            .lock()
            .expect("smoke service peer map is never poisoned")
            .insert(peer_id.clone(), peer.conn_id);

        tokio::spawn(run_session(
            peer_id,
            recv,
            send,
            self.counters.clone(),
            self.ping_interval,
        ));
    }

    fn remove_peer(&self, peer: &ZakuraPeerId, conn_id: ZakuraConnId) {
        let peer_id = peer_id_hex(peer);
        self.counters.peers_removed.fetch_add(1, Ordering::Relaxed);
        // Remove only this connection's entry: a reconnect can register the new
        // connection before the old one is torn down.
        let mut live_peers = self
            .counters
            .live_peers
            .lock()
            .expect("smoke service peer map is never poisoned");
        if live_peers.get(&peer_id) == Some(&conn_id) {
            live_peers.remove(&peer_id);
        }
        drop(live_peers);
        info!(peer = %peer_id, conn_id, "smoke-echo stream closed");
    }
}

/// Ping every peer on an interval and answer the pings it sends back.
async fn run_session(
    peer_id: String,
    mut recv: FramedRecv,
    send: FramedSend,
    counters: Arc<Counters>,
    ping_interval: Duration,
) {
    let mut ticker = tokio::time::interval(ping_interval);
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let frame = Frame {
                    message_type: MESSAGE_TYPE_PING,
                    flags: 0,
                    payload: unix_millis().to_be_bytes().to_vec(),
                };
                if send.send(frame).await.is_err() {
                    break;
                }
                counters.pings_sent.fetch_add(1, Ordering::Relaxed);
            }
            frame = recv.recv() => {
                let Some(frame) = frame else { break };
                match frame.message_type {
                    MESSAGE_TYPE_PING => {
                        let echo = Frame {
                            message_type: MESSAGE_TYPE_PONG,
                            flags: 0,
                            payload: frame.payload,
                        };
                        if send.send(echo).await.is_err() {
                            break;
                        }
                        counters.pings_answered.fetch_add(1, Ordering::Relaxed);
                    }
                    MESSAGE_TYPE_PONG => {
                        counters.pongs_received.fetch_add(1, Ordering::Relaxed);
                        if let Some(sent_at) = payload_millis(&frame.payload) {
                            let rtt = unix_millis().saturating_sub(sent_at);
                            counters.last_rtt_millis.store(rtt, Ordering::Relaxed);
                            info!(peer = %peer_id, rtt_millis = rtt, "smoke-echo round trip");
                        }
                    }
                    other => warn!(peer = %peer_id, message_type = other, "smoke-echo unknown frame"),
                }
            }
        }
    }
    info!(peer = %peer_id, "smoke-echo session ended");
}

/// Hex-encode a peer id: `ZakuraPeerId` is raw bytes with no `Display`.
fn peer_id_hex(peer: &ZakuraPeerId) -> String {
    peer.as_bytes().iter().fold(String::new(), |mut out, byte| {
        let _ = write!(out, "{byte:02x}");
        out
    })
}

fn payload_millis(payload: &[u8]) -> Option<u64> {
    payload.try_into().ok().map(u64::from_be_bytes)
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or_default()
}

/// Read `--config <path>` from the command line.
fn config_path() -> Result<PathBuf, Report> {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                return args
                    .next()
                    .map(PathBuf::from)
                    .ok_or_else(|| eyre!("--config needs a path"))
            }
            other => return Err(eyre!("unexpected argument {other:?}; usage: --config <path>")),
        }
    }
    Err(eyre!("--config <path> is required"))
}

fn env_flag(name: &str, default: bool) -> bool {
    match std::env::var(name) {
        Ok(value) => !matches!(value.trim(), "0" | "false" | "no" | ""),
        Err(_) => default,
    }
}

#[tokio::main]
async fn main() -> Result<(), Report> {
    color_eyre::install()?;
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config_path = config_path()?;
    let config_text = std::fs::read_to_string(&config_path)
        .wrap_err_with(|| format!("reading {}", config_path.display()))?;
    let config: ZakuradConfig = toml::from_str(&config_text)
        .wrap_err_with(|| format!("parsing {}", config_path.display()))?;

    let service_id = ZakuraServiceId::new(SMOKE_SERVICE_ID)
        .map_err(|error| eyre!("smoke service id is invalid: {error}"))?;
    let provides = env_flag("SMOKE_PROVIDE", true);
    let seeks = env_flag("SMOKE_SEEK", true);
    let ping_interval = Duration::from_secs(
        std::env::var("SMOKE_PING_INTERVAL_SECS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(15),
    );
    let status_path = PathBuf::from(
        std::env::var("SMOKE_STATUS_PATH")
            .unwrap_or_else(|_| "/root/logs/smoke-service.json".to_owned()),
    );

    let counters = Arc::new(Counters::default());
    let custom = CustomService {
        service: Arc::new(SmokeEchoService {
            counters: counters.clone(),
            ping_interval,
        }),
        provides: provides.then(|| service_id.clone()).into_iter().collect(),
        seeks: seeks.then_some(service_id).into_iter().collect(),
    };

    info!(
        config = %config_path.display(),
        service_id = SMOKE_SERVICE_ID,
        provides,
        seeks,
        ping_interval_secs = ping_interval.as_secs(),
        status = %status_path.display(),
        "starting embedded Zakura node with the smoke-echo service",
    );

    let shutdown = CancellationToken::new();
    tokio::spawn(write_status(counters, status_path, shutdown.clone()));
    tokio::spawn(shutdown_on_signal(shutdown.clone()));

    zakurad::node::run_with_services(config, vec![custom], shutdown).await
}

/// Write the counter snapshot to disk so the fleet can collect it.
async fn write_status(counters: Arc<Counters>, path: PathBuf, shutdown: CancellationToken) {
    let mut ticker = tokio::time::interval(Duration::from_secs(15));
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = ticker.tick() => {}
        }
        let snapshot = counters.snapshot();
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        if let Err(error) = std::fs::write(&path, format!("{snapshot}\n")) {
            warn!(path = %path.display(), %error, "smoke-echo status write failed");
        }
    }
}

async fn shutdown_on_signal(shutdown: CancellationToken) {
    let _ = tokio::signal::ctrl_c().await;
    info!("smoke-echo received ctrl-c; shutting the embedded node down");
    shutdown.cancel();
}
