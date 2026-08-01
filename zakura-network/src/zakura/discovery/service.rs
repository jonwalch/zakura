//! Native discovery service (stream kind 4) on the Zakura transport.
//!
//! Discovery is a single long-lived ordered stream per peer. Each side runs a
//! [`DiscoverySink`] (the reader, which imports peer records and answers
//! `GetPeers`) and a [`DiscoverySource`] (the writer, which periodically gossips
//! the local self-record and asks for more peers). The wire format is the
//! [`DiscoveryMessage`] payload carried inside a generic transport [`Frame`]
//! (`message_type = DISCOVERY_FRAME_MESSAGE_TYPE`, `flags = 0`), identical to the
//! original native-discovery wire so peers interoperate.

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex as StdMutex,
    },
    time::Duration,
};

use iroh::NodeId;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::zakura::{
    handle_pipe_exit, spawn_supervised_peer_task, spawn_supervised_pipe, BlockSyncHandle,
    CloseCause, Flow, Frame, FramedRecv, FramedSend, HeaderSyncEvent, HeaderSyncHandle,
    OrderedSendError, OrderedSessionDemand, OrderedStreamOpening, OrderedStreamPolicy, Peer,
    PeerStreamSession, Pipe, Service, ServiceAdmissionDecision, ServicePeerDirection, SinkReject,
    Stream, StreamMode, ZakuraConnId, ZakuraPeerId, LOCAL_MAX_CONTROL_FRAME_BYTES,
    ZAKURA_CAP_DISCOVERY,
};

#[cfg(test)]
use super::pipe::decode_discovery_frame;
use super::pipe::{discovery_pipe, DsEnv, DsLocal, DISCOVERY_FRAME_MESSAGE_TYPE};
use super::protocol::{
    BlockSyncServiceSummary, DiscoveryBookError, DiscoveryMessage, DiscoveryRecordError,
    GetServices, HeaderSyncServiceSummary, ServiceSummaryEnvelope, Services, ZakuraDiscoveryHandle,
    ZakuraNodeRecord, ZakuraServiceId, DEFAULT_LIVE_SERVICE_SUMMARY_TTL,
    MAX_DISCOVERY_RECORDS_PER_RESPONSE, ZAKURA_DISCOVERY_STREAM_VERSION, ZAKURA_STREAM_DISCOVERY,
};

/// Maximum time discovery waits for first-party exchange responses before releasing the session.
const DISCOVERY_INITIAL_EXCHANGE_TIMEOUT: Duration = Duration::from_secs(2);

const DISCOVERY_SERVICE_STREAMS: [Stream; 1] = [Stream {
    kind: ZAKURA_STREAM_DISCOVERY,
    version: ZAKURA_DISCOVERY_STREAM_VERSION,
    // Advisory until the transport wires Stream::frame_cap end-to-end; the
    // authoritative inbound cap is app_frame_cap_for_stream_kind.
    frame_cap: LOCAL_MAX_CONTROL_FRAME_BYTES,
    capability: ZAKURA_CAP_DISCOVERY,
    mode: StreamMode::Ordered,
}];

/// Service-declared streams for native discovery.
pub(crate) fn discovery_streams() -> &'static [Stream] {
    &DISCOVERY_SERVICE_STREAMS
}

/// Cloneable typed sender for one native discovery ordered stream.
#[derive(Clone, Debug)]
pub struct DiscoveryPeerSession {
    peer_id: ZakuraPeerId,
    direction: ServicePeerDirection,
    send: FramedSend,
    cancel: CancellationToken,
}

impl DiscoveryPeerSession {
    fn new(session: &PeerStreamSession, direction: ServicePeerDirection) -> Self {
        Self {
            peer_id: session.peer_id().clone(),
            direction,
            send: session.sender(),
            cancel: session.cancel_token(),
        }
    }

    /// Authenticated peer identity for this discovery stream.
    pub fn peer_id(&self) -> &ZakuraPeerId {
        &self.peer_id
    }

    /// Direction of the underlying Zakura connection.
    pub fn direction(&self) -> ServicePeerDirection {
        self.direction
    }

    /// Peer disconnect/local shutdown cancellation token.
    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel.clone()
    }

    /// Send this node's signed self-record.
    pub fn try_send_hello(&self, record: ZakuraNodeRecord) -> Result<(), OrderedSendError> {
        self.try_send_message(DiscoveryMessage::Hello { record })
    }

    /// Ask this peer for more peer records.
    pub fn try_send_get_peers(
        &self,
        limit: u16,
        wanted_services: Vec<ZakuraServiceId>,
        exclude_node_ids: Vec<NodeId>,
    ) -> Result<(), OrderedSendError> {
        self.try_send_message(DiscoveryMessage::GetPeers {
            limit,
            wanted_services,
            exclude_node_ids,
        })
    }

    /// Send peer records to this peer.
    pub fn try_send_peers(&self, records: Vec<ZakuraNodeRecord>) -> Result<(), OrderedSendError> {
        self.try_send_message(DiscoveryMessage::Peers { records })
    }

    /// Ask this peer for its own live service summaries.
    pub fn try_send_get_services(
        &self,
        wanted_services: Vec<ZakuraServiceId>,
    ) -> Result<(), OrderedSendError> {
        self.try_send_message(DiscoveryMessage::GetServices(GetServices {
            wanted_services,
        }))
    }

    /// Send this node's first-party live service summaries.
    pub fn try_send_services(&self, services: Services) -> Result<(), OrderedSendError> {
        self.try_send_message(DiscoveryMessage::Services(services))
    }

    fn try_send_message(&self, message: DiscoveryMessage) -> Result<(), OrderedSendError> {
        let payload = message
            .encode()
            .map_err(|error| OrderedSendError::Encode(Box::new(error)))?;
        match self.send.try_send(Frame {
            message_type: DISCOVERY_FRAME_MESSAGE_TYPE,
            flags: 0,
            payload,
        }) {
            Ok(()) => Ok(()),
            Err(tokio::sync::mpsc::error::TrySendError::Full(_frame)) => {
                Err(OrderedSendError::Full)
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_frame)) => {
                Err(OrderedSendError::Closed)
            }
        }
    }
}

/// Native discovery service backed by a [`ZakuraDiscoveryHandle`] runtime.
#[derive(Clone, Debug)]
pub struct DiscoveryService {
    handle: ZakuraDiscoveryHandle,
    header_sync: Option<HeaderSyncHandle>,
    block_sync: Option<BlockSyncHandle>,
    session_states: Arc<StdMutex<SessionStateMap>>,
    connection_owners: Arc<StdMutex<Vec<Arc<dyn Service>>>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DiscoverySessionState {
    Active,
    Retired,
}

/// The session-state slot for one `(peer, connection)`, tagged with the stream
/// session that owns it so a finished exchange cannot retire a newer session
/// admitted on the same connection.
#[derive(Clone, Copy, Debug)]
struct DiscoverySessionRecord {
    session_id: u64,
    state: DiscoverySessionState,
    /// Consecutive sessions on this connection that ended without a successful
    /// exchange. Reaching `MAX_DISCOVERY_SESSION_FAILURES` retires the record so
    /// the transport stops reopening a stream the peer keeps breaking.
    failed_attempts: u32,
}

/// Consecutive failed sessions on one `(peer, connection)` before discovery
/// retires it. Generous: an honest peer completes the exchange on the first
/// session, and a fresh connection always starts a fresh record.
const MAX_DISCOVERY_SESSION_FAILURES: u32 = 3;

type SessionStateMap = HashMap<(ZakuraPeerId, ZakuraConnId), DiscoverySessionRecord>;

fn retire_discovery_session(
    session_states: &StdMutex<SessionStateMap>,
    peer: &ZakuraPeerId,
    conn_id: ZakuraConnId,
    session_id: u64,
) -> bool {
    let mut session_states = session_states
        .lock()
        .expect("discovery session-state mutex is never poisoned");
    let Some(record) = session_states.get_mut(&(peer.clone(), conn_id)) else {
        return false;
    };
    if record.session_id != session_id {
        return false;
    }
    record.state = DiscoverySessionState::Retired;
    true
}

/// Charge one failed session to the owning record; retires it at the failure
/// bound. Session-scoped like `retire_discovery_session` so a stale task cannot
/// charge a newer session admitted on the same connection. Returns whether the
/// record was retired by this failure.
fn record_discovery_session_failure(
    session_states: &StdMutex<SessionStateMap>,
    peer: &ZakuraPeerId,
    conn_id: ZakuraConnId,
    session_id: u64,
) -> bool {
    let mut session_states = session_states
        .lock()
        .expect("discovery session-state mutex is never poisoned");
    let Some(record) = session_states.get_mut(&(peer.clone(), conn_id)) else {
        return false;
    };
    if record.session_id != session_id || record.state != DiscoverySessionState::Active {
        return false;
    }
    record.failed_attempts = record.failed_attempts.saturating_add(1);
    if record.failed_attempts >= MAX_DISCOVERY_SESSION_FAILURES {
        record.state = DiscoverySessionState::Retired;
        tracing::info!(
            ?peer,
            conn_id,
            attempts = record.failed_attempts,
            "retiring Zakura discovery sessions on this connection after repeated failed exchanges"
        );
        return true;
    }
    false
}

/// Reset the owning record's failure count after a successful exchange, so the
/// intended shared-connection refresh loop never accumulates failures.
fn record_discovery_session_success(
    session_states: &StdMutex<SessionStateMap>,
    peer: &ZakuraPeerId,
    conn_id: ZakuraConnId,
    session_id: u64,
) {
    let mut session_states = session_states
        .lock()
        .expect("discovery session-state mutex is never poisoned");
    let Some(record) = session_states.get_mut(&(peer.clone(), conn_id)) else {
        return;
    };
    if record.session_id != session_id {
        return;
    }
    record.failed_attempts = 0;
}

impl DiscoveryService {
    /// Builds a discovery service driven by `handle`.
    pub fn new(handle: ZakuraDiscoveryHandle) -> Self {
        Self {
            handle,
            header_sync: None,
            block_sync: None,
            session_states: Arc::new(StdMutex::new(HashMap::new())),
            connection_owners: Arc::new(StdMutex::new(Vec::new())),
        }
    }

    /// Builds a discovery service with header-sync and block-sync summary providers.
    pub(crate) fn with_sync_services(
        handle: ZakuraDiscoveryHandle,
        header_sync: HeaderSyncHandle,
        block_sync: Option<BlockSyncHandle>,
    ) -> Self {
        Self {
            handle,
            header_sync: Some(header_sync),
            block_sync,
            session_states: Arc::new(StdMutex::new(HashMap::new())),
            connection_owners: Arc::new(StdMutex::new(Vec::new())),
        }
    }

    pub(crate) fn set_connection_owners(&self, owners: Vec<Arc<dyn Service>>) {
        *self
            .connection_owners
            .lock()
            .expect("discovery connection-owner mutex is never poisoned") = owners;
    }

    /// Returns the underlying discovery runtime handle.
    pub fn handle(&self) -> &ZakuraDiscoveryHandle {
        &self.handle
    }
}

impl Service for DiscoveryService {
    fn name(&self) -> &'static str {
        "discovery"
    }

    fn streams(&self) -> &[Stream] {
        discovery_streams()
    }

    fn ordered_stream_policy(&self, _kind: u16) -> OrderedStreamPolicy {
        OrderedStreamPolicy {
            opening: OrderedStreamOpening::InitiatorOnly,
            reopen: true,
        }
    }

    fn ordered_session_demand(
        &self,
        conn_id: ZakuraConnId,
        peer: &ZakuraPeerId,
        _negotiated: u64,
        direction: ServicePeerDirection,
    ) -> OrderedSessionDemand {
        if self
            .session_states
            .lock()
            .expect("discovery session-state mutex is never poisoned")
            .get(&(peer.clone(), conn_id))
            .is_some_and(|record| record.state == DiscoverySessionState::Retired)
        {
            return OrderedSessionDemand::Retire;
        }

        let mut peers = self.handle.subscribe_peer_snapshot();
        let snapshot = *peers.borrow_and_update();
        let slots_free = match direction {
            ServicePeerDirection::Inbound => snapshot.inbound_slots_free,
            ServicePeerDirection::Outbound => snapshot.outbound_slots_free,
        };
        if slots_free == 0 {
            return OrderedSessionDemand::WaitForChange(Box::pin(async move {
                if peers.changed().await.is_err() {
                    std::future::pending::<()>().await;
                }
            }));
        }

        OrderedSessionDemand::OpenNow
    }

    fn wants_peer(
        &self,
        _peer: &ZakuraPeerId,
        _negotiated: u64,
        direction: ServicePeerDirection,
    ) -> bool {
        // Discovery escalation only checks this reactor's local room; live
        // summaries are first-party advisory data imported by the runtime.
        let snapshot = self.handle.peer_snapshot();
        match direction {
            ServicePeerDirection::Inbound => snapshot.inbound_slots_free > 0,
            ServicePeerDirection::Outbound => snapshot.outbound_slots_free > 0,
        }
    }

    fn add_peer(&self, mut peer: Peer) {
        let Some((session_id, recv, send)) =
            peer.take_stream_with_session_id(ZAKURA_STREAM_DISCOVERY)
        else {
            return;
        };
        let Some(peer_node_id) = node_id_from_peer_id(&peer.id) else {
            // A peer id that is not a 32-byte node id cannot be a discovery
            // author; drop the stream without registering an exchange.
            return;
        };
        let session = PeerStreamSession::new(
            peer.id.clone(),
            ZAKURA_STREAM_DISCOVERY,
            ZAKURA_DISCOVERY_STREAM_VERSION,
            recv,
            send,
            peer.service_cancel_token(),
        );
        let discovery_session = DiscoveryPeerSession::new(&session, peer.direction);
        let conn_id = peer.conn_id;
        {
            let mut session_states = self
                .session_states
                .lock()
                .expect("discovery session-state mutex is never poisoned");
            let previous = session_states.get(&(peer.id.clone(), conn_id));
            if previous.is_some_and(|record| record.state == DiscoverySessionState::Retired) {
                // A retired record must not be resurrected by a new stream on the
                // same connection — the remote initiates streams on inbound
                // connections and could otherwise churn sessions at its own pace.
                return;
            }
            // Preserve the failure count across reopens so consecutive broken
            // sessions on this connection stay bounded.
            let failed_attempts = previous.map_or(0, |record| record.failed_attempts);
            session_states.insert(
                (peer.id.clone(), conn_id),
                DiscoverySessionRecord {
                    session_id,
                    state: DiscoverySessionState::Active,
                    failed_attempts,
                },
            );
        }
        let service_cancel = discovery_session.cancel_token();
        let connection_cancel = peer.cancel_token();
        let close_cause = peer.close_cause();
        let (_peer_id, _stream_kind, _stream_version, recv, _send, _session_cancel) =
            session.into_parts();

        let handle = self.handle.clone();
        let header_sync = self.header_sync.clone();
        let block_sync = self.block_sync.clone();
        let session_states = self.session_states.clone();
        let connection_owners = self
            .connection_owners
            .lock()
            .expect("discovery connection-owner mutex is never poisoned")
            .clone();
        // SR-1: a panic in the admission task (before it hands off to the
        // exchange) must still disconnect this one peer and cancel its discovery
        // session instead of leaving admitted state behind a half-live
        // connection. Normal/parked exits cancel `service_cancel` inline below;
        // `on_panic` covers the unwind path only.
        let admit_peer_id = discovery_session.peer_id().clone();
        let panic_service_cancel = service_cancel.clone();
        let panic_connection_cancel = connection_cancel.clone();
        let panic_close_cause = close_cause.clone();
        spawn_supervised_peer_task(
            admit_peer_id,
            || {},
            move || {
                panic_close_cause.record("service_panic");
                panic_service_cancel.cancel();
                panic_connection_cancel.cancel();
            },
            async move {
                let decision = handle
                    .admit_peer_session(
                        conn_id,
                        session_id,
                        discovery_session.peer_id().clone(),
                        discovery_session.direction(),
                    )
                    .await;
                if decision != ServiceAdmissionDecision::Admit {
                    metrics::counter!("zakura.discovery.peer.parked").increment(1);
                    tracing::info!(
                        peer = ?discovery_session.peer_id(),
                        direction = ?discovery_session.direction(),
                        ?decision,
                        "locally parking Zakura discovery service session"
                    );
                    // A parked session ends without an exchange; charge it so
                    // reopen-then-park cycles stay bounded on this connection.
                    record_discovery_session_failure(
                        &session_states,
                        discovery_session.peer_id(),
                        conn_id,
                        session_id,
                    );
                    service_cancel.cancel();
                    return;
                }

                spawn_discovery_exchange(DiscoveryExchangeStart {
                    handle,
                    header_sync,
                    block_sync,
                    connection_owners,
                    peer_node_id,
                    discovery_session,
                    conn_id,
                    session_id,
                    recv,
                    service_cancel,
                    connection_cancel,
                    close_cause,
                    session_states,
                });
            },
        );
    }

    fn remove_peer(&self, peer: &ZakuraPeerId, conn_id: ZakuraConnId) {
        self.session_states
            .lock()
            .expect("discovery session-state mutex is never poisoned")
            .remove(&(peer.clone(), conn_id));
        let handle = self.handle.clone();
        let peer = peer.clone();
        tokio::spawn(async move {
            handle.remove_peer(&peer, conn_id).await;
        });
    }
}

struct DiscoveryExchangeStart {
    handle: ZakuraDiscoveryHandle,
    header_sync: Option<HeaderSyncHandle>,
    block_sync: Option<BlockSyncHandle>,
    connection_owners: Vec<Arc<dyn Service>>,
    peer_node_id: NodeId,
    discovery_session: DiscoveryPeerSession,
    conn_id: ZakuraConnId,
    session_id: u64,
    recv: FramedRecv,
    service_cancel: CancellationToken,
    connection_cancel: CancellationToken,
    close_cause: CloseCause,
    session_states: Arc<StdMutex<SessionStateMap>>,
}

fn spawn_discovery_exchange(start: DiscoveryExchangeStart) {
    let DiscoveryExchangeStart {
        handle,
        header_sync,
        block_sync,
        connection_owners,
        peer_node_id,
        discovery_session,
        conn_id,
        session_id,
        recv,
        service_cancel,
        connection_cancel,
        close_cause,
        session_states,
    } = start;
    let peer_id = discovery_session.peer_id().clone();
    let progress = Arc::new(DiscoveryExchangeProgress::default());
    let sink = DiscoverySink {
        handle: handle.clone(),
        header_sync,
        block_sync,
        peer_node_id,
        session: discovery_session.clone(),
        conn_id,
        session_id,
        progress: progress.clone(),
    };
    let sink_service_cancel = service_cancel.clone();
    let reject_connection_cancel = connection_cancel.clone();
    let panic_connection_cancel = connection_cancel.clone();
    let reject_close_cause = close_cause.clone();
    let panic_close_cause = close_cause.clone();
    let sink_peer_id = peer_id.clone();
    // A protocol reject is fatal to the connection; normal/parked exits leave it
    // for the source task to tear down once it knows no other service owns the
    // peer (below). Panic teardown is in `on_panic`.
    let pipe = async move {
        let mut pipe = discovery_pipe(sink_peer_id);
        handle_pipe_exit(
            "discovery",
            &reject_connection_cancel,
            &reject_close_cause,
            run_discovery_pipe(&mut pipe, recv, sink).await,
        );
    };
    let on_panic = move || {
        panic_close_cause.record("service_panic");
        panic_connection_cancel.cancel();
    };
    // Let the returned handle drop to detach the supervised reader task; the
    // `PipeTeardown` still runs on every exit path.
    spawn_supervised_pipe(peer_id.clone(), sink_service_cancel, || {}, on_panic, pipe);

    let source = DiscoverySource {
        handle: handle.clone(),
        session: discovery_session,
        conn_id,
        session_id,
        progress,
    };
    // SR-1: a panic in the source task skips its `service_cancel.cancel()`,
    // `handle.remove_session()`, and discovery-only connection cancellation,
    // leaving admitted discovery state behind a half-live connection. On the
    // unwind path, disconnect this one peer; the connection teardown then drives
    // the async session removal through the registry. Normal exits run the inline
    // cleanup below, so `on_panic` is the panic-only path.
    let source_task_peer_id = peer_id.clone();
    let panic_source_service_cancel = service_cancel.clone();
    let panic_source_connection_cancel = connection_cancel.clone();
    let panic_source_close_cause = close_cause.clone();
    let source_close_cause = close_cause.clone();
    spawn_supervised_peer_task(
        source_task_peer_id,
        || {},
        move || {
            panic_source_close_cause.record("service_panic");
            panic_source_service_cancel.cancel();
            panic_source_connection_cancel.cancel();
        },
        async move {
            let exchanged = source.run_initial_exchange().await;
            if exchanged {
                record_discovery_session_success(&session_states, &peer_id, conn_id, session_id);
            } else {
                // The stream broke before the exchange completed; charge the
                // session so an attacker cannot get endless reopens by breaking
                // the stream pre-exchange.
                record_discovery_session_failure(&session_states, &peer_id, conn_id, session_id);
            }
            let mut other_service_owner =
                exchanged && peer_has_other_service_owner(&connection_owners, &peer_id, conn_id);
            while other_service_owner && source.refresh_after_interval().await.is_ok() {
                other_service_owner =
                    peer_has_other_service_owner(&connection_owners, &peer_id, conn_id);
            }
            let closes_discovery_only_connection = exchanged
                && !other_service_owner
                && handle
                    .is_current_session(&peer_id, conn_id, session_id)
                    .await;
            if closes_discovery_only_connection {
                handle.mark_short_lived_exchange(&peer_node_id).await;
                retire_discovery_session(&session_states, &peer_id, conn_id, session_id);
            }
            service_cancel.cancel();
            handle.remove_session(&peer_id, conn_id, session_id).await;
            if closes_discovery_only_connection {
                source_close_cause.record("discovery_exchange_complete");
                connection_cancel.cancel();
            }
        },
    );
}

/// Reader half of the discovery stream: imports peer records and answers queries.
struct DiscoverySink {
    handle: ZakuraDiscoveryHandle,
    header_sync: Option<HeaderSyncHandle>,
    block_sync: Option<BlockSyncHandle>,
    peer_node_id: NodeId,
    session: DiscoveryPeerSession,
    conn_id: ZakuraConnId,
    session_id: u64,
    progress: Arc<DiscoveryExchangeProgress>,
}

async fn run_discovery_pipe(
    pipe: &mut Pipe<DsLocal, DsEnv>,
    mut recv: FramedRecv,
    sink: DiscoverySink,
) -> Result<(), SinkReject> {
    let cancel = sink.session.cancel_token();
    loop {
        let frame = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(()),
            frame = recv.recv() => frame,
        };
        let Some(frame) = frame else {
            return Ok(());
        };

        match pipe.run_one(frame) {
            Flow::Continue(()) | Flow::Done => {}
            Flow::Reject(reject) => return Err(reject),
        }

        let Some(message) = pipe.local_mut().take_decoded() else {
            continue;
        };
        sink.handle_message(message).await?;
    }
}

impl DiscoverySink {
    async fn handle_message(&self, message: DiscoveryMessage) -> Result<(), SinkReject> {
        if !self
            .handle
            .is_current_session(self.session.peer_id(), self.conn_id, self.session_id)
            .await
        {
            return Ok(());
        }

        match message {
            DiscoveryMessage::Hello { record } => self.handle_hello(record).await,
            DiscoveryMessage::GetPeers {
                limit,
                wanted_services,
                exclude_node_ids,
            } => {
                let records = self
                    .handle
                    .sample_peers(usize::from(limit), &wanted_services, &exclude_node_ids)
                    .await;
                self.send_peers(records)
            }
            DiscoveryMessage::Peers { records } => {
                self.handle
                    .import_peer_records(records, Some(self.peer_node_id))
                    .await;
                self.progress.mark_peers();
                Ok(())
            }
            DiscoveryMessage::GetServices(query) => {
                let services = self.local_services_response(query).await?;
                self.send_services(services)
            }
            DiscoveryMessage::Services(services) => self.handle_services(services).await,
        }
    }

    async fn local_services_response(&self, query: GetServices) -> Result<Services, SinkReject> {
        let mut summaries = Vec::new();

        if service_wanted(&query.wanted_services, &ZakuraServiceId::header_sync()) {
            if let Some(header_sync) = &self.header_sync {
                let (best_height, best_hash) = header_sync.best_header_tip();
                let summary = HeaderSyncServiceSummary::from_snapshot(
                    best_height,
                    best_hash,
                    None,
                    true,
                    header_sync.peer_snapshot(),
                );
                summaries.push(
                    ServiceSummaryEnvelope::header_sync(&summary).map_err(SinkReject::local)?,
                );
            }
        }

        if service_wanted(&query.wanted_services, &ZakuraServiceId::discovery()) {
            let summary = self.handle.local_discovery_summary().await;
            summaries.push(ServiceSummaryEnvelope::discovery(&summary).map_err(SinkReject::local)?);
        }

        if service_wanted(&query.wanted_services, &ZakuraServiceId::block_sync()) {
            if let Some(block_sync) = &self.block_sync {
                let summary = BlockSyncServiceSummary::from_status_and_snapshot(
                    block_sync.local_status(),
                    block_sync.peer_snapshot(),
                );
                summaries
                    .push(ServiceSummaryEnvelope::block_sync(&summary).map_err(SinkReject::local)?);
            }
        }

        Ok(self.handle.local_services_response(summaries))
    }

    async fn handle_services(&self, services: Services) -> Result<(), SinkReject> {
        if services.node_id != self.peer_node_id {
            return Err(SinkReject::protocol(
                "Zakura discovery SERVICES authored by a different node id",
            ));
        }

        let header_summaries =
            decode_header_sync_summaries(&services).map_err(SinkReject::protocol)?;
        self.handle
            .import_connected_peer_services(services, self.peer_node_id)
            .await
            .map_err(SinkReject::protocol)?;
        self.progress.mark_services();

        if let Some(header_sync) = &self.header_sync {
            for summary in header_summaries {
                if let Err(error) = header_sync
                    .send(HeaderSyncEvent::AdvisoryHeaderSummary {
                        peer: self.session.peer_id().clone(),
                        summary,
                    })
                    .await
                {
                    tracing::debug!(
                        ?error,
                        peer = ?self.session.peer_id(),
                        "failed to queue first-party Zakura header-sync advisory summary"
                    );
                    break;
                }
            }
        }

        Ok(())
    }

    async fn handle_hello(&self, record: ZakuraNodeRecord) -> Result<(), SinkReject> {
        if record.body.node_id != self.peer_node_id {
            return Err(SinkReject::protocol(
                "Zakura discovery hello authored by a different node id",
            ));
        }
        match self
            .handle
            .import_connected_peer_record(record, self.peer_node_id)
            .await
        {
            Ok(_) => Ok(()),
            Err(error) if is_advisory_self_record_import_error(&error) => {
                tracing::debug!(?error, "ignoring advisory discovery hello import error");
                Ok(())
            }
            Err(error) => Err(SinkReject::protocol(error)),
        }?;
        self.progress.mark_hello();
        Ok(())
    }

    fn send_peers(&self, records: Vec<ZakuraNodeRecord>) -> Result<(), SinkReject> {
        match self.session.try_send_peers(records) {
            Ok(()) | Err(OrderedSendError::Full) => Ok(()),
            Err(OrderedSendError::Closed) => {
                Err(SinkReject::local("Zakura discovery send channel closed"))
            }
            Err(OrderedSendError::Encode(error)) => Err(SinkReject::local(error)),
        }
    }

    fn send_services(&self, services: Services) -> Result<(), SinkReject> {
        match self.session.try_send_services(services) {
            Ok(()) | Err(OrderedSendError::Full) => Ok(()),
            Err(OrderedSendError::Closed) => {
                Err(SinkReject::local("Zakura discovery send channel closed"))
            }
            Err(OrderedSendError::Encode(error)) => Err(SinkReject::local(error)),
        }
    }
}

fn service_wanted(wanted_services: &[ZakuraServiceId], service_id: &ZakuraServiceId) -> bool {
    wanted_services.is_empty() || wanted_services.iter().any(|wanted| wanted == service_id)
}

fn decode_header_sync_summaries(
    services: &Services,
) -> Result<Vec<HeaderSyncServiceSummary>, crate::BoxError> {
    let mut summaries = Vec::new();
    for envelope in &services.summaries {
        if let Some(summary) = envelope.decode_header_sync()? {
            summaries.push(summary);
        }
    }
    Ok(summaries)
}

/// Writer half of the discovery stream: periodic self-record gossip + peer asks.
struct DiscoverySource {
    handle: ZakuraDiscoveryHandle,
    session: DiscoveryPeerSession,
    conn_id: ZakuraConnId,
    session_id: u64,
    progress: Arc<DiscoveryExchangeProgress>,
}

impl DiscoverySource {
    async fn run_initial_exchange(&self) -> bool {
        if self.exchange().await.is_err() {
            return false;
        }
        let cancel = self.session.cancel_token();
        let completed = tokio::select! {
            biased;
            _ = cancel.cancelled() => false,
            _ = self.progress.wait_complete() => true,
            _ = tokio::time::sleep(DISCOVERY_INITIAL_EXCHANGE_TIMEOUT) => false,
        };
        if !completed {
            return false;
        }

        self.handle
            .is_current_session(self.session.peer_id(), self.conn_id, self.session_id)
            .await
    }

    async fn refresh_after_interval(&self) -> Result<(), ()> {
        let cancel = self.session.cancel_token();
        let refresh_interval = discovery_exchange_interval(self.handle.refresh_interval().await);
        tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(()),
            _ = tokio::time::sleep(refresh_interval) => self.exchange().await,
        }
    }

    /// Gossips the current self-record and asks the peer for records and services.
    ///
    /// Returns `Err(())` once the stream's send side is gone, so the caller
    /// stops the periodic loop.
    async fn exchange(&self) -> Result<(), ()> {
        if !self
            .handle
            .is_current_session(self.session.peer_id(), self.conn_id, self.session_id)
            .await
        {
            return Err(());
        }

        let record = self
            .handle
            .current_self_record_for_gossip()
            .await
            .map_err(|error| {
                tracing::debug!(
                    ?error,
                    peer = ?self.session.peer_id(),
                    "failed to refresh Zakura discovery self-record"
                );
            })?;
        let record = (*record).clone();
        self.handle_send_result(self.session.try_send_hello(record))?;

        let limit = self
            .handle
            .peer_sample_limit()
            .await
            .min(MAX_DISCOVERY_RECORDS_PER_RESPONSE);
        // `peer_sample_limit` is bounded by MAX_DISCOVERY_RECORDS_PER_RESPONSE
        // (<= u16::MAX), so the cast cannot truncate.
        let exclude_node_ids = self.handle.peer_sample_exclusions().await;
        self.handle_send_result(self.session.try_send_get_peers(
            limit as u16,
            Vec::new(),
            exclude_node_ids,
        ))?;

        self.handle_send_result(self.session.try_send_get_services(Vec::new()))
    }

    fn handle_send_result(&self, result: Result<(), OrderedSendError>) -> Result<(), ()> {
        match result {
            Ok(()) | Err(OrderedSendError::Full) => Ok(()),
            Err(OrderedSendError::Closed) => Err(()),
            Err(OrderedSendError::Encode(error)) => {
                tracing::debug!(
                    ?error,
                    peer = ?self.session.peer_id(),
                    "failed to encode Zakura discovery message"
                );
                Ok(())
            }
        }
    }
}

#[derive(Default)]
struct DiscoveryExchangeProgress {
    hello: AtomicBool,
    peers: AtomicBool,
    services: AtomicBool,
    notify: Notify,
}

impl DiscoveryExchangeProgress {
    fn mark_hello(&self) {
        self.hello.store(true, Ordering::Relaxed);
        self.notify.notify_waiters();
    }

    fn mark_peers(&self) {
        self.peers.store(true, Ordering::Relaxed);
        self.notify.notify_waiters();
    }

    fn mark_services(&self) {
        self.services.store(true, Ordering::Relaxed);
        self.notify.notify_waiters();
    }

    fn complete(&self) -> bool {
        self.hello.load(Ordering::Relaxed)
            && self.peers.load(Ordering::Relaxed)
            && self.services.load(Ordering::Relaxed)
    }

    async fn wait_complete(&self) {
        while !self.complete() {
            self.notify.notified().await;
        }
    }
}

fn peer_has_other_service_owner(
    connection_owners: &[Arc<dyn Service>],
    peer_id: &ZakuraPeerId,
    conn_id: ZakuraConnId,
) -> bool {
    connection_owners
        .iter()
        .any(|owner| owner.owns_connection_for_peer(peer_id, conn_id))
}

fn discovery_exchange_interval(record_refresh_interval: Duration) -> Duration {
    record_refresh_interval.min(DEFAULT_LIVE_SERVICE_SUMMARY_TTL / 2)
}

/// Returns the iroh node id encoded by a discovery peer id, if it is a 32-byte
/// node id.
fn node_id_from_peer_id(peer_id: &ZakuraPeerId) -> Option<NodeId> {
    let bytes: [u8; 32] = peer_id.as_bytes().try_into().ok()?;
    NodeId::from_bytes(&bytes).ok()
}

/// A peer-hello import error that should be logged and ignored rather than
/// closing the live connection. These mean the peer's record is not locally
/// dialable or has drifted out of the freshness window, neither of which is the
/// connected peer's fault.
fn is_advisory_self_record_import_error(error: &DiscoveryBookError) -> bool {
    matches!(
        error,
        DiscoveryBookError::NoUsableDirectAddress
            | DiscoveryBookError::NonDialableDirectAddress { .. }
            | DiscoveryBookError::Record(DiscoveryRecordError::Expired)
            | DiscoveryBookError::Record(DiscoveryRecordError::FarFutureExpiry)
    )
}

#[cfg(test)]
#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use iroh::SecretKey;
    use tokio::{sync::watch, task::JoinHandle};

    use super::*;
    use crate::zakura::discovery::protocol::{
        DiscoveryServiceSummary, ZakuraLiveServiceSummary, ZakuraNodeRecordBody,
    };
    use crate::zakura::{
        framed_channel, spawn_block_sync_reactor, spawn_header_sync_reactor, BlockSyncFrontiers,
        BlockSyncStartup, FullStateFrontiers, HeaderSyncAction, HeaderSyncPeerSession,
        HeaderSyncStartup, ServicePeerLimits, ZakuraBlockSyncConfig, ZakuraDiscoveryConfig,
        ZakuraDiscoveryLocalConfig, ZakuraHandshakeConfig, ZakuraHeaderSyncConfig,
        LOCAL_MAX_MESSAGE_BYTES, MAX_BS_RESPONSE_BYTES, ZAKURA_CAP_BLOCK_SYNC,
        ZAKURA_CAP_DISCOVERY, ZAKURA_CAP_HEADER_SYNC,
    };
    use zakura_chain::{block, parameters::Network};

    #[test]
    fn periodic_exchange_refreshes_before_live_service_summaries_expire() {
        assert_eq!(
            discovery_exchange_interval(Duration::from_secs(10 * 60)),
            Duration::from_secs(15)
        );
        assert_eq!(
            discovery_exchange_interval(Duration::from_secs(10)),
            Duration::from_secs(10)
        );
    }

    #[test]
    fn connection_ownership_is_scoped_to_the_exact_connection() {
        let peer = ZakuraPeerId::new(vec![37; 32]).expect("test peer id is within bounds");
        let owners: Vec<Arc<dyn Service>> = vec![Arc::new(TestConnectionOwner {
            peer: peer.clone(),
            conn_id: 2,
        })];

        assert!(!peer_has_other_service_owner(&owners, &peer, 1));
        assert!(peer_has_other_service_owner(&owners, &peer, 2));
    }

    #[derive(Debug)]
    struct TestConnectionOwner {
        peer: ZakuraPeerId,
        conn_id: ZakuraConnId,
    }

    impl Service for TestConnectionOwner {
        fn name(&self) -> &'static str {
            "test-connection-owner"
        }

        fn streams(&self) -> &[Stream] {
            &[]
        }

        fn owns_connection_for_peer(&self, peer: &ZakuraPeerId, conn_id: ZakuraConnId) -> bool {
            peer == &self.peer && conn_id == self.conn_id
        }

        fn add_peer(&self, _peer: Peer) {}

        fn remove_peer(&self, _peer: &ZakuraPeerId, _conn_id: ZakuraConnId) {}
    }

    fn current_test_unix_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after Unix epoch")
            .as_secs()
    }

    fn spawn_test_header_sync() -> Result<
        (
            HeaderSyncHandle,
            tokio::sync::mpsc::Receiver<HeaderSyncAction>,
            JoinHandle<()>,
        ),
        crate::BoxError,
    > {
        let network = Network::new_regtest(Default::default());
        let anchor = (block::Height(0), network.genesis_hash());
        let startup = HeaderSyncStartup::new(
            network,
            anchor,
            FullStateFrontiers {
                finalized_height: anchor.0,
                verified_block_tip: anchor.0,
                verified_block_hash: anchor.1,
            },
            Some(anchor),
            ZakuraHeaderSyncConfig::default(),
            LOCAL_MAX_MESSAGE_BYTES,
        );
        spawn_header_sync_reactor(startup).map_err(Into::into)
    }

    fn discovery_frame(message: DiscoveryMessage) -> Result<Frame, crate::BoxError> {
        Ok(Frame {
            message_type: DISCOVERY_FRAME_MESSAGE_TYPE,
            flags: 0,
            payload: message.encode()?,
        })
    }

    fn signed_discovery_record(
        secret_key: &SecretKey,
        handshake: &ZakuraHandshakeConfig,
    ) -> Result<ZakuraNodeRecord, crate::BoxError> {
        let body = ZakuraNodeRecordBody {
            node_id: secret_key.public(),
            direct_addrs: vec![SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(45, 33, 30, 45)),
                8233,
            )],
            services: vec![ZakuraServiceId::discovery()],
            zakura_protocol_min: handshake.zakura_protocol_min,
            zakura_protocol_max: handshake.zakura_protocol_max,
            network_id: handshake.network_id,
            chain_id: handshake.chain_id,
            sequence: 1,
            expires_at_unix_secs: current_test_unix_secs().saturating_add(60),
        };
        Ok(ZakuraNodeRecord::sign(body, secret_key)?)
    }

    async fn complete_peer_side_discovery_exchange(
        peer_send: &FramedSend,
        peer_recv: &mut FramedRecv,
        peer_secret: &SecretKey,
        handshake: &ZakuraHandshakeConfig,
    ) -> Result<(), crate::BoxError> {
        let mut saw_hello = false;
        let mut saw_get_peers = false;
        let mut saw_get_services = false;
        while !(saw_hello && saw_get_peers && saw_get_services) {
            let frame = tokio::time::timeout(Duration::from_secs(2), peer_recv.recv())
                .await?
                .expect("discovery source sends exchange frames");
            match decode_discovery_frame(&frame)? {
                DiscoveryMessage::Hello { .. } => saw_hello = true,
                DiscoveryMessage::GetPeers { .. } => saw_get_peers = true,
                DiscoveryMessage::GetServices(_) => saw_get_services = true,
                DiscoveryMessage::Peers { .. } | DiscoveryMessage::Services(_) => {}
            }
        }

        peer_send
            .send(discovery_frame(DiscoveryMessage::Hello {
                record: signed_discovery_record(peer_secret, handshake)?,
            })?)
            .await?;
        peer_send
            .send(discovery_frame(DiscoveryMessage::Peers {
                records: Vec::new(),
            })?)
            .await?;
        let summary = DiscoveryServiceSummary {
            peer_exchange_slots_free: 1,
            max_records_per_response: 1,
            expected_disconnect_after_exchange: true,
        };
        peer_send
            .send(discovery_frame(DiscoveryMessage::Services(Services {
                node_id: peer_secret.public(),
                expires_at_unix_secs: u64::MAX,
                summaries: vec![ServiceSummaryEnvelope::discovery(&summary)?],
            }))?)
            .await?;

        Ok(())
    }

    async fn wait_for_discovery_inbound_peers(handle: &ZakuraDiscoveryHandle, expected: usize) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if handle.peer_snapshot().inbound_peers == expected {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("discovery peer snapshot reaches expected inbound count");
    }

    #[tokio::test]
    async fn get_services_returns_local_first_party_discovery_summary(
    ) -> Result<(), crate::BoxError> {
        let (_connected_tx, connected_rx) = watch::channel(Vec::new());
        let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        let local_secret = SecretKey::from_bytes(&[21u8; 32]);
        let handle = ZakuraDiscoveryHandle::new(
            ZakuraDiscoveryLocalConfig {
                secret_key: local_secret.clone(),
                direct_addrs: Vec::new(),
                services: vec![ZakuraServiceId::discovery()],
                zakura_protocol_min: handshake.zakura_protocol_min,
                zakura_protocol_max: handshake.zakura_protocol_max,
                network_id: handshake.network_id,
                chain_id: handshake.chain_id,
                last_authored_sequence: None,
            },
            ZakuraDiscoveryConfig {
                peer_limits: ServicePeerLimits {
                    max_inbound_peers: 4,
                    ..ServicePeerLimits::default()
                },
                ..ZakuraDiscoveryConfig::default()
            },
            connected_rx,
        )?;
        let service = DiscoveryService::new(handle.clone());
        let peer_node_id = SecretKey::from_bytes(&[22u8; 32]).public();
        let peer_id = ZakuraPeerId::new(peer_node_id.as_bytes().to_vec())?;
        let (peer_send, service_recv) = framed_channel(8);
        let (service_send, mut peer_recv) = framed_channel(8);
        let streams = HashMap::from([(ZAKURA_STREAM_DISCOVERY, (service_recv, service_send))]);

        service.add_peer(Peer::new(
            peer_id,
            None,
            ZAKURA_CAP_DISCOVERY,
            streams,
            CancellationToken::new(),
        ));

        peer_send
            .send(Frame {
                message_type: DISCOVERY_FRAME_MESSAGE_TYPE,
                flags: 0,
                payload: DiscoveryMessage::GetServices(GetServices {
                    wanted_services: vec![ZakuraServiceId::discovery()],
                })
                .encode()?,
            })
            .await?;

        let services = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let frame = peer_recv.recv().await.expect("discovery stream stays open");
                let message = decode_discovery_frame(&frame).expect("outbound frame decodes");
                if let DiscoveryMessage::Services(services) = message {
                    return services;
                }
            }
        })
        .await
        .expect("service response is sent");

        assert_eq!(services.node_id, local_secret.public());
        assert_eq!(services.summaries.len(), 1);
        assert_eq!(
            services.summaries[0].service_id,
            ZakuraServiceId::discovery()
        );
        let summary = services.summaries[0]
            .decode_discovery()?
            .expect("discovery summary tag decodes");
        assert_eq!(summary.peer_exchange_slots_free, 3);
        assert!(summary.expected_disconnect_after_exchange);
        assert_eq!(
            summary.max_records_per_response,
            u16::try_from(MAX_DISCOVERY_RECORDS_PER_RESPONSE)
                .expect("record response cap fits in u16")
        );

        Ok(())
    }

    #[tokio::test]
    async fn get_services_returns_local_first_party_block_sync_summary(
    ) -> Result<(), crate::BoxError> {
        let (_connected_tx, connected_rx) = watch::channel(Vec::new());
        let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        let local_secret = SecretKey::from_bytes(&[24u8; 32]);
        let discovery_handle = ZakuraDiscoveryHandle::new(
            ZakuraDiscoveryLocalConfig {
                secret_key: local_secret.clone(),
                direct_addrs: Vec::new(),
                services: vec![ZakuraServiceId::discovery(), ZakuraServiceId::block_sync()],
                zakura_protocol_min: handshake.zakura_protocol_min,
                zakura_protocol_max: handshake.zakura_protocol_max,
                network_id: handshake.network_id,
                chain_id: handshake.chain_id,
                last_authored_sequence: None,
            },
            ZakuraDiscoveryConfig::default(),
            connected_rx,
        )?;
        let (header_sync, _header_actions, header_task) = spawn_test_header_sync()?;
        let (tip_tx, tip_rx) = watch::channel((block::Height(5), block::Hash([5; 32])));
        drop(tip_tx);
        let (block_sync, _block_actions, block_task) =
            spawn_block_sync_reactor(BlockSyncStartup::new(
                BlockSyncFrontiers {
                    finalized_height: block::Height(0),
                    verified_block_tip: block::Height(5),
                    verified_block_hash: block::Hash([5; 32]),
                },
                (block::Height(5), block::Hash([5; 32])),
                tip_rx,
                ZakuraBlockSyncConfig::default(),
            ));
        let service = DiscoveryService::with_sync_services(
            discovery_handle,
            header_sync,
            Some(block_sync.clone()),
        );
        let peer_node_id = SecretKey::from_bytes(&[25u8; 32]).public();
        let peer_id = ZakuraPeerId::new(peer_node_id.as_bytes().to_vec())?;
        let (peer_send, service_recv) = framed_channel(8);
        let (service_send, mut peer_recv) = framed_channel(8);
        let streams = HashMap::from([(ZAKURA_STREAM_DISCOVERY, (service_recv, service_send))]);

        service.add_peer(Peer::new(
            peer_id,
            None,
            ZAKURA_CAP_DISCOVERY | ZAKURA_CAP_BLOCK_SYNC,
            streams,
            CancellationToken::new(),
        ));

        peer_send
            .send(Frame {
                message_type: DISCOVERY_FRAME_MESSAGE_TYPE,
                flags: 0,
                payload: DiscoveryMessage::GetServices(GetServices {
                    wanted_services: vec![ZakuraServiceId::block_sync()],
                })
                .encode()?,
            })
            .await?;

        let services = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let frame = peer_recv.recv().await.expect("discovery stream stays open");
                let message = decode_discovery_frame(&frame).expect("outbound frame decodes");
                if let DiscoveryMessage::Services(services) = message {
                    return services;
                }
            }
        })
        .await
        .expect("service response is sent");

        assert_eq!(services.node_id, local_secret.public());
        assert_eq!(services.summaries.len(), 1);
        assert_eq!(
            services.summaries[0].service_id,
            ZakuraServiceId::block_sync()
        );
        let summary = services.summaries[0]
            .decode_block_sync()?
            .expect("block summary tag decodes");
        assert_eq!(summary.servable_low, block::Height(0));
        assert_eq!(summary.servable_high, block::Height(5));
        assert_eq!(summary.tip_hash, block::Hash([5; 32]));
        assert_eq!(
            usize::from(summary.free_slots),
            block_sync.peer_snapshot().inbound_slots_free
        );
        assert_eq!(
            summary.max_blocks_per_response,
            ZakuraBlockSyncConfig::default().advertised_max_blocks_per_response()
        );
        assert_eq!(summary.max_response_bytes, MAX_BS_RESPONSE_BYTES);

        header_task.abort();
        block_task.abort();
        Ok(())
    }

    #[tokio::test]
    async fn inbound_services_updates_first_party_live_summary_cache() -> Result<(), crate::BoxError>
    {
        let (connected_tx, connected_rx) = watch::channel(Vec::new());
        let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        let local_secret = SecretKey::from_bytes(&[23u8; 32]);
        let handle = ZakuraDiscoveryHandle::new(
            ZakuraDiscoveryLocalConfig {
                secret_key: local_secret,
                direct_addrs: Vec::new(),
                services: vec![ZakuraServiceId::discovery()],
                zakura_protocol_min: handshake.zakura_protocol_min,
                zakura_protocol_max: handshake.zakura_protocol_max,
                network_id: handshake.network_id,
                chain_id: handshake.chain_id,
                last_authored_sequence: None,
            },
            ZakuraDiscoveryConfig::default(),
            connected_rx,
        )?;
        let service = DiscoveryService::new(handle.clone());
        let peer_node_id = SecretKey::from_bytes(&[24u8; 32]).public();
        let peer_id = ZakuraPeerId::new(peer_node_id.as_bytes().to_vec())?;
        connected_tx.send_replace(vec![peer_id.clone()]);

        let (peer_send, service_recv) = framed_channel(8);
        let (service_send, _peer_recv) = framed_channel(8);
        let streams = HashMap::from([(ZAKURA_STREAM_DISCOVERY, (service_recv, service_send))]);

        service.add_peer(Peer::new(
            peer_id,
            None,
            ZAKURA_CAP_DISCOVERY,
            streams,
            CancellationToken::new(),
        ));

        let summary = DiscoveryServiceSummary {
            peer_exchange_slots_free: 7,
            max_records_per_response: 11,
            expected_disconnect_after_exchange: false,
        };
        peer_send
            .send(Frame {
                message_type: DISCOVERY_FRAME_MESSAGE_TYPE,
                flags: 0,
                payload: DiscoveryMessage::Services(Services {
                    node_id: peer_node_id,
                    expires_at_unix_secs: u64::MAX,
                    summaries: vec![ServiceSummaryEnvelope::discovery(&summary)?],
                })
                .encode()?,
            })
            .await?;

        let cached = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Some(cached) = handle.live_service_summaries(peer_node_id).await {
                    if !cached.is_empty() {
                        return cached;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("inbound SERVICES is imported");

        assert_eq!(cached.len(), 1);
        assert_eq!(
            cached[0].summary,
            ZakuraLiveServiceSummary::Discovery(summary)
        );

        Ok(())
    }

    #[tokio::test]
    async fn discovery_only_short_lived_exchange_closes_connection_and_backs_off(
    ) -> Result<(), crate::BoxError> {
        let (connected_tx, connected_rx) = watch::channel(Vec::new());
        let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        let local_secret = SecretKey::from_bytes(&[40u8; 32]);
        let handle = ZakuraDiscoveryHandle::new(
            ZakuraDiscoveryLocalConfig {
                secret_key: local_secret,
                direct_addrs: Vec::new(),
                services: vec![ZakuraServiceId::discovery()],
                zakura_protocol_min: handshake.zakura_protocol_min,
                zakura_protocol_max: handshake.zakura_protocol_max,
                network_id: handshake.network_id,
                chain_id: handshake.chain_id,
                last_authored_sequence: None,
            },
            ZakuraDiscoveryConfig::default(),
            connected_rx,
        )?;
        let service = DiscoveryService::new(handle.clone());
        let peer_secret = SecretKey::from_bytes(&[41u8; 32]);
        let peer_node_id = peer_secret.public();
        let peer_id = ZakuraPeerId::new(peer_node_id.as_bytes().to_vec())?;
        connected_tx.send_replace(vec![peer_id.clone()]);

        let connection_cancel = CancellationToken::new();
        let (peer_send, service_recv) = framed_channel(16);
        let (service_send, mut peer_recv) = framed_channel(16);
        let streams = HashMap::from([(ZAKURA_STREAM_DISCOVERY, (service_recv, service_send))]);

        service.add_peer(Peer::new(
            peer_id,
            None,
            ZAKURA_CAP_DISCOVERY,
            streams,
            connection_cancel.clone(),
        ));

        wait_for_discovery_inbound_peers(&handle, 1).await;
        complete_peer_side_discovery_exchange(&peer_send, &mut peer_recv, &peer_secret, &handshake)
            .await?;
        tokio::time::timeout(Duration::from_secs(2), connection_cancel.cancelled())
            .await
            .expect("discovery-only exchange closes the shared connection");
        wait_for_discovery_inbound_peers(&handle, 0).await;

        connected_tx.send_replace(Vec::new());
        assert!(handle
            .dial_candidates(&[ZakuraServiceId::discovery()], &[])
            .await
            .is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn discovery_short_lived_exchange_keeps_header_sync_connection(
    ) -> Result<(), crate::BoxError> {
        let (connected_tx, connected_rx) = watch::channel(Vec::new());
        let handshake = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        let local_secret = SecretKey::from_bytes(&[42u8; 32]);
        let discovery_handle = ZakuraDiscoveryHandle::new(
            ZakuraDiscoveryLocalConfig {
                secret_key: local_secret,
                direct_addrs: Vec::new(),
                services: vec![ZakuraServiceId::discovery()],
                zakura_protocol_min: handshake.zakura_protocol_min,
                zakura_protocol_max: handshake.zakura_protocol_max,
                network_id: handshake.network_id,
                chain_id: handshake.chain_id,
                last_authored_sequence: None,
            },
            ZakuraDiscoveryConfig::default(),
            connected_rx,
        )?;
        let (header_sync, _header_actions, header_task) = spawn_test_header_sync()?;
        let service = DiscoveryService::with_sync_services(
            discovery_handle.clone(),
            header_sync.clone(),
            None,
        );
        let peer_secret = SecretKey::from_bytes(&[43u8; 32]);
        let peer_node_id = peer_secret.public();
        let peer_id = ZakuraPeerId::new(peer_node_id.as_bytes().to_vec())?;
        connected_tx.send_replace(vec![peer_id.clone()]);

        let (header_send, _header_recv) = framed_channel(8);
        let header_session = HeaderSyncPeerSession::from_parts_with_direction(
            peer_id.clone(),
            ServicePeerDirection::Inbound,
            header_send,
            CancellationToken::new(),
        );
        header_sync
            .send(HeaderSyncEvent::PeerConnected(header_session))
            .await?;
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if header_sync
                    .candidate_state()
                    .admitted_node_ids
                    .contains(&peer_node_id)
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("header sync admits the peer");

        let connection_cancel = CancellationToken::new();
        let (peer_send, service_recv) = framed_channel(16);
        let (service_send, mut peer_recv) = framed_channel(16);
        let streams = HashMap::from([(ZAKURA_STREAM_DISCOVERY, (service_recv, service_send))]);
        // Connection ownership is claimed by the owning service, not inferred
        // from the negotiated capability bits: stand in for the header-sync
        // service that owns this connection in production.
        service.set_connection_owners(vec![Arc::new(TestConnectionOwner {
            peer: peer_id.clone(),
            conn_id: 0,
        })]);
        service.add_peer(Peer::new(
            peer_id,
            None,
            ZAKURA_CAP_DISCOVERY | ZAKURA_CAP_HEADER_SYNC,
            streams,
            connection_cancel.clone(),
        ));

        wait_for_discovery_inbound_peers(&discovery_handle, 1).await;
        complete_peer_side_discovery_exchange(&peer_send, &mut peer_recv, &peer_secret, &handshake)
            .await?;
        // While another service owns the connection the exchange refreshes on an
        // interval instead of releasing after one round, so discovery keeps its
        // session admitted rather than dropping back to zero.
        wait_for_discovery_inbound_peers(&discovery_handle, 1).await;
        assert_eq!(header_sync.peer_snapshot().inbound_peers, 1);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), connection_cancel.cancelled())
                .await
                .is_err(),
            "discovery releases only its own session while header sync owns the connection"
        );

        header_task.abort();
        Ok(())
    }
}
