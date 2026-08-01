use std::sync::Arc;

use futures::StreamExt;
use tokio::sync::Notify;

use super::*;
use crate::zakura::CloseCause;
use zakura_node_services::header_chain as port;

#[derive(Debug)]
struct PanickingPort {
    release: Option<Arc<Notify>>,
}

impl port::HeaderChainPort for PanickingPort {
    fn continuation_locator(
        &self,
    ) -> port::HeaderChainFuture<
        '_,
        Result<Option<zakura_header_chain::HeaderLocator>, port::HeaderChainPortError>,
    > {
        let release = self.release.clone();
        Box::pin(async move {
            if let Some(release) = release {
                release.notified().await;
            }
            panic!("unbounded panic payload must remain inside the port boundary")
        })
    }

    fn vct_repair_context(
        &self,
        _owner: zakura_header_chain::WorkOwner,
        _height: block::Height,
    ) -> port::HeaderChainFuture<'_, Result<port::VctRepairContextReply, port::HeaderChainPortError>>
    {
        Box::pin(async { panic!("internal repair port panic") })
    }

    fn acquire_header_path(
        &self,
        _request: port::AcquireHeaderPath,
    ) -> port::HeaderChainFuture<'_, Result<port::AcquireHeaderPathReply, port::HeaderChainPortError>>
    {
        Box::pin(async { Ok(port::AcquireHeaderPathReply::TargetNotRetained) })
    }

    fn read_header_path(
        &self,
        _path: port::RetainedHeaderPath,
        _request: port::ReadHeaderPath,
    ) -> port::HeaderChainFuture<'_, Result<port::ReadHeaderPathReply, port::HeaderChainPortError>>
    {
        Box::pin(async { Ok(port::ReadHeaderPathReply::Unavailable) })
    }

    fn release_header_path(
        &self,
        _path: port::RetainedHeaderPath,
    ) -> port::HeaderChainFuture<'_, Result<(), port::HeaderChainPortError>> {
        Box::pin(async { Ok(()) })
    }

    fn prepare_header_target(
        &self,
        request: port::PrepareHeaderTarget,
    ) -> port::HeaderChainFuture<'_, port::PrepareHeaderTargetReply> {
        Box::pin(async move {
            Err(Arc::new(
                zakura_header_chain::HeaderChainError::local_resource(
                    zakura_header_chain::ErrorSubject::Branch(request.owner.branch),
                    None,
                ),
            ))
        })
    }

    fn apply_header_target(
        &self,
        target: port::PreparedHeaderTarget,
    ) -> port::HeaderChainFuture<'_, port::ApplyHeaderTargetReply> {
        let owner = target.into_insert().owner;
        Box::pin(async move {
            Err(Arc::new(
                zakura_header_chain::HeaderChainError::local_resource(
                    zakura_header_chain::ErrorSubject::Branch(owner.branch),
                    None,
                ),
            ))
        })
    }
}

fn direct_reactor(port: Arc<dyn port::HeaderChainPort>) -> HeaderSyncReactor {
    let mut startup = startup(CancellationToken::new());
    let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
    let snapshot = committed_snapshot(anchor);
    let (_snapshots_tx, snapshots_rx) = watch::channel(Some(snapshot));
    startup.committed_snapshots = Some(snapshots_rx);
    startup.header_chain_port = port;
    startup.use_direct_port();
    let (_, _, reactor) =
        build_header_sync_reactor(startup).expect("the direct-port fixture builds");
    reactor
}

fn connected_session(
    reactor: &mut HeaderSyncReactor,
    peer: ZakuraPeerId,
    session_id: u64,
) -> (CancellationToken, CloseCause) {
    let (send, _outbound) = crate::zakura::framed_channel(8);
    let service_cancel = CancellationToken::new();
    let connection_cancel = CancellationToken::new();
    let close_cause = CloseCause::new();
    reactor.handle_peer_connected(HeaderSyncPeerSession::from_parts_with_connection(
        peer,
        session_id,
        send,
        service_cancel,
        connection_cancel.clone(),
        close_cause.clone(),
    ));
    (connection_cancel, close_cause)
}

#[tokio::test]
async fn port_future_panic_disconnects_exact_session_and_reactor_survives() {
    let mut reactor = direct_reactor(Arc::new(PanickingPort { release: None }));
    let peer = peer();
    let session_id = 7;
    let (connection_cancel, close_cause) =
        connected_session(&mut reactor, peer.clone(), session_id);
    let snapshot = reactor
        .committed_snapshot
        .clone()
        .expect("the fixture has a committed snapshot");
    let target = block::Hash([0x81; 32]);
    let scope = zakura_header_chain::WorkScope::for_header_target(&snapshot, target);

    assert!(
        reactor.dispatch_action(HeaderPortOperation::QueryHeaderLocator {
            peer: peer.clone(),
            session_id,
            target_tip_hash: target,
            scope,
        })
    );
    let completion = reactor
        .pending_port_operations
        .next()
        .await
        .expect("the panicking operation completes at its unwind boundary");
    reactor.handle_port_completion(completion);

    assert!(connection_cancel.is_cancelled());
    assert_eq!(close_cause.get_or("missing"), "header_port_panic");
    assert!(!reactor.request_deadlines.contains_key(&peer));

    let second_peer =
        ZakuraPeerId::new(vec![0x82; 32]).expect("the second peer ID has the required length");
    let (second_cancel, _) = connected_session(&mut reactor, second_peer.clone(), 8);
    assert!(!second_cancel.is_cancelled());
    assert!(reactor.peer_state.contains_key(&second_peer));
}

#[tokio::test]
async fn stale_port_panic_does_not_disconnect_replacement_session() {
    let release = Arc::new(Notify::new());
    let mut reactor = direct_reactor(Arc::new(PanickingPort {
        release: Some(release.clone()),
    }));
    let peer = peer();
    let (old_cancel, _) = connected_session(&mut reactor, peer.clone(), 10);
    let snapshot = reactor
        .committed_snapshot
        .clone()
        .expect("the fixture has a committed snapshot");
    let target = block::Hash([0x83; 32]);
    let scope = zakura_header_chain::WorkScope::for_header_target(&snapshot, target);
    assert!(
        reactor.dispatch_action(HeaderPortOperation::QueryHeaderLocator {
            peer: peer.clone(),
            session_id: 10,
            target_tip_hash: target,
            scope,
        })
    );

    let (replacement_cancel, _) = connected_session(&mut reactor, peer.clone(), 11);
    release.notify_one();
    let completion = reactor
        .pending_port_operations
        .next()
        .await
        .expect("the stale operation reaches its panic boundary");
    reactor.handle_port_completion(completion);

    assert!(old_cancel.is_cancelled());
    assert!(!replacement_cancel.is_cancelled());
    assert_eq!(
        reactor
            .peer_state
            .get(&peer)
            .expect("the replacement remains admitted")
            .session
            .session_id(),
        11
    );
}

#[tokio::test]
async fn internal_vct_port_panic_is_contained_without_peer_disconnect() {
    let mut reactor = direct_reactor(Arc::new(PanickingPort { release: None }));
    let peer = peer();
    let (connection_cancel, _) = connected_session(&mut reactor, peer.clone(), 7);
    let snapshot = reactor
        .committed_snapshot
        .clone()
        .expect("the fixture has a committed snapshot");
    let owner = zakura_header_chain::WorkScope::for_body_work(&snapshot).bind(
        INTERNAL_VCT_REPAIR_SESSION_ID,
        std::num::NonZeroU64::new(1).expect("one is nonzero"),
    );

    assert!(
        reactor.dispatch_action(HeaderPortOperation::QueryVctRepairContext {
            owner,
            height: block::Height(1),
        })
    );
    let completion = reactor
        .pending_port_operations
        .next()
        .await
        .expect("the repair panic reaches its unwind boundary");
    reactor.handle_port_completion(completion);

    assert!(!connection_cancel.is_cancelled());
    assert!(reactor.peer_state.contains_key(&peer));
}

