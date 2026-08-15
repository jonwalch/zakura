use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use tokio::time::Instant;
use zakura_chain::block;
use zakura_header_chain::{AuxDelivery, Frontier, HeaderWorkAuthority, SourceId, StoreError};

use crate::{
    RetainedPathLease, RetainedPathLeaseOutcome, RetainedPathPage, RetainedPathReadOutcome,
    MAX_RETAINED_PATH_LEASES,
};

use super::super::{HeaderChainStoreError, ReadDisk, RETAINED_PATH_LEASE_IDLE};
use super::HeaderChainReader;

#[derive(Debug, Default)]
pub(in crate::service::finalized_state::header_chain) struct RetainedPathLeaseRegistry {
    next_lease_id: u64,
    next_reservation_id: u64,
    by_peer: HashMap<SourceId, CanonicalHeaderPathCursor>,
    reservations: HashMap<SourceId, u64>,
    reference_counts: HashMap<block::Hash, usize>,
    cached_references: Arc<[block::Hash]>,
    references_dirty: bool,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum CanonicalHeaderPathPosition {
    Finalized {
        next: block::Height,
        end: block::Height,
    },
    Retained {
        next: usize,
    },
    Complete,
}

#[derive(Clone, Debug)]
struct CanonicalHeaderPathCursor {
    lease_id: u64,
    peer: SourceId,
    session_id: u64,
    target: Frontier,
    common_ancestor: Frontier,
    scope: HeaderWorkAuthority,
    position: CanonicalHeaderPathPosition,
    last_frontier: Frontier,
    retained_path: Arc<[block::Hash]>,
    idle_deadline: Instant,
}

impl CanonicalHeaderPathCursor {
    fn lease(&self) -> RetainedPathLease {
        RetainedPathLease {
            lease_id: self.lease_id,
            peer: self.peer,
            session_id: self.session_id,
            target: self.target,
            common_ancestor: self.common_ancestor,
            scope: self.scope,
            idle_deadline: self.idle_deadline,
        }
    }
}

#[derive(Debug)]
struct RetainedPathLeaseSpec {
    peer: SourceId,
    session_id: u64,
    target: Frontier,
    common_ancestor: Frontier,
    scope: HeaderWorkAuthority,
    position: CanonicalHeaderPathPosition,
    retained_path: Arc<[block::Hash]>,
}

#[derive(Copy, Clone, Debug)]
struct CanonicalHeaderPathAdvance {
    expected_after: Frontier,
    position: CanonicalHeaderPathPosition,
    last_frontier: Frontier,
    now: Instant,
}

#[derive(Debug)]
struct RetainedPathReservation {
    leases: Arc<Mutex<RetainedPathLeaseRegistry>>,
    peer: SourceId,
    reservation_id: u64,
    active: bool,
}

impl RetainedPathReservation {
    fn commit(
        mut self,
        spec: RetainedPathLeaseSpec,
        now: Instant,
    ) -> Result<RetainedPathLeaseOutcome, HeaderChainStoreError> {
        let outcome = self
            .leases
            .lock()
            .map_err(|_| HeaderChainStoreError::SynchronizationPoisoned)?
            .commit_reservation(self.peer, self.reservation_id, spec, now);
        self.active = false;
        Ok(outcome)
    }
}

impl Drop for RetainedPathReservation {
    fn drop(&mut self) {
        if self.active {
            if let Ok(mut leases) = self.leases.lock() {
                leases.release_reservation(self.peer, self.reservation_id);
            }
        }
    }
}

impl RetainedPathLeaseRegistry {
    fn expire(&mut self, now: Instant) {
        let expired: Vec<_> = self
            .by_peer
            .iter()
            .filter_map(|(peer, cursor)| (cursor.idle_deadline <= now).then_some(*peer))
            .collect();
        for peer in expired {
            self.remove_peer(peer);
        }
    }

    fn add_references(&mut self, cursor: &CanonicalHeaderPathCursor) {
        // The retention algorithm walks from each target to finality.
        // This registry counts the target hash because it protects the full immutable suffix.
        // A path-hash entry would grow the bounded owner list with chain length.
        // That growth would reject ordinary transitions.
        *self.reference_counts.entry(cursor.target.hash).or_default() += 1;
        self.references_dirty = true;
    }

    fn remove_peer(&mut self, peer: SourceId) -> Option<CanonicalHeaderPathCursor> {
        let cursor = self.by_peer.remove(&peer)?;
        let hash = cursor.target.hash;
        let remove = {
            let Some(count) = self.reference_counts.get_mut(&hash) else {
                panic!("every installed lease target has a registry count");
            };
            let Some(next_count) = count.checked_sub(1) else {
                panic!("a lease target reference count cannot underflow");
            };
            *count = next_count;
            *count == 0
        };
        if remove {
            self.reference_counts.remove(&hash);
        }
        self.references_dirty = true;
        Some(cursor)
    }

    fn reserve(&mut self, peer: SourceId, now: Instant) -> Option<u64> {
        self.expire(now);
        if self.by_peer.contains_key(&peer)
            || self.reservations.contains_key(&peer)
            || self.by_peer.len().saturating_add(self.reservations.len())
                >= MAX_RETAINED_PATH_LEASES
        {
            return None;
        }
        let reservation_id = self.next_reservation_id.checked_add(1)?;
        self.next_reservation_id = reservation_id;
        self.reservations.insert(peer, reservation_id);
        Some(reservation_id)
    }

    fn release_reservation(&mut self, peer: SourceId, reservation_id: u64) {
        if self.reservations.get(&peer) == Some(&reservation_id) {
            self.reservations.remove(&peer);
        }
    }

    fn commit_reservation(
        &mut self,
        peer: SourceId,
        reservation_id: u64,
        spec: RetainedPathLeaseSpec,
        now: Instant,
    ) -> RetainedPathLeaseOutcome {
        if peer != spec.peer || self.reservations.get(&peer) != Some(&reservation_id) {
            return RetainedPathLeaseOutcome::Busy;
        }
        self.reservations.remove(&peer);
        if self.by_peer.contains_key(&peer) {
            return RetainedPathLeaseOutcome::Busy;
        }
        let Some(lease_id) = self.next_lease_id.checked_add(1) else {
            return RetainedPathLeaseOutcome::Busy;
        };
        self.next_lease_id = lease_id;
        let cursor = CanonicalHeaderPathCursor {
            lease_id,
            peer: spec.peer,
            session_id: spec.session_id,
            target: spec.target,
            common_ancestor: spec.common_ancestor,
            scope: spec.scope,
            position: spec.position,
            last_frontier: spec.common_ancestor,
            retained_path: spec.retained_path,
            idle_deadline: now + RETAINED_PATH_LEASE_IDLE,
        };
        let lease = cursor.lease();
        self.add_references(&cursor);
        self.by_peer.insert(spec.peer, cursor);
        RetainedPathLeaseOutcome::Acquired(Box::new(lease))
    }

    fn get(
        &mut self,
        peer: SourceId,
        session_id: u64,
        lease_id: u64,
        now: Instant,
    ) -> Option<CanonicalHeaderPathCursor> {
        self.expire(now);
        let cursor = self.by_peer.get(&peer)?;
        if cursor.session_id != session_id || cursor.lease_id != lease_id {
            return None;
        }
        Some(cursor.clone())
    }

    fn advance(
        &mut self,
        peer: SourceId,
        session_id: u64,
        lease_id: u64,
        advance: CanonicalHeaderPathAdvance,
    ) -> bool {
        if self
            .by_peer
            .get(&peer)
            .is_some_and(|cursor| cursor.idle_deadline <= advance.now)
        {
            self.remove_peer(peer);
            return false;
        }
        let Some(cursor) = self.by_peer.get_mut(&peer) else {
            return false;
        };
        if cursor.session_id != session_id
            || cursor.lease_id != lease_id
            || cursor.last_frontier != advance.expected_after
        {
            return false;
        }
        cursor.position = advance.position;
        cursor.last_frontier = advance.last_frontier;
        cursor.idle_deadline = advance.now + RETAINED_PATH_LEASE_IDLE;
        true
    }

    fn release(
        &mut self,
        peer: SourceId,
        session_id: u64,
        lease_id: u64,
        scope: HeaderWorkAuthority,
    ) -> bool {
        let matches = self.by_peer.get(&peer).is_some_and(|cursor| {
            cursor.session_id == session_id && cursor.lease_id == lease_id && cursor.scope == scope
        });
        if matches {
            self.remove_peer(peer);
        }
        matches
    }

    pub(in crate::service::finalized_state::header_chain) fn active_references(
        &mut self,
        now: Instant,
    ) -> Arc<[block::Hash]> {
        self.expire(now);
        if self.references_dirty {
            let mut references: Vec<_> = self.reference_counts.keys().copied().collect();
            references.sort_unstable_by_key(|hash| hash.0);
            self.cached_references = references.into();
            self.references_dirty = false;
        }
        self.cached_references.clone()
    }
}

impl HeaderChainReader {
    /// Reserve capacity, capture one engine version, then recheck it before installing the lease.
    ///
    /// The installed lease owns an immutable hash suffix. Later pages read exactly that suffix;
    /// a version change during acquisition or paging makes the operation unavailable for retry.
    pub(crate) fn acquire_retained_path(
        &self,
        peer: SourceId,
        session_id: u64,
        target_tip_hash: block::Hash,
        locator_hashes: &[block::Hash],
        scope: HeaderWorkAuthority,
    ) -> Result<RetainedPathLeaseOutcome, HeaderChainStoreError> {
        if locator_hashes.is_empty()
            || locator_hashes.len() > zakura_header_chain::MAX_HEADER_LOCATOR_HASHES
        {
            return Err(HeaderChainStoreError::Store(StoreError::Incoherent(
                "retained path locator count is outside protocol bounds",
            )));
        }
        let reservation_id = self
            .leases
            .lock()
            .map_err(|_| HeaderChainStoreError::SynchronizationPoisoned)?
            .reserve(peer, Instant::now());
        let Some(reservation_id) = reservation_id else {
            return Ok(RetainedPathLeaseOutcome::Busy);
        };
        let reservation = RetainedPathReservation {
            leases: self.leases.clone(),
            peer,
            reservation_id,
            active: true,
        };
        let (snapshot, target, mut reverse_path) = {
            let engine = self
                .transition_engine
                .lock()
                .map_err(|_| HeaderChainStoreError::SynchronizationPoisoned)?;
            let snapshot = engine.snapshot();
            if scope != HeaderWorkAuthority::for_target(&snapshot, target_tip_hash) {
                return Ok(RetainedPathLeaseOutcome::Busy);
            }
            let Some(target_node) = engine.graph().header_node(target_tip_hash) else {
                return Ok(RetainedPathLeaseOutcome::TargetNotRetained);
            };
            let target = Frontier::new(target_node.height, target_tip_hash);
            let mut reverse_path = vec![target];
            let mut current = target_node;
            while current.height > snapshot.frontiers.finalized.height {
                let Some(parent) = engine.graph().header_node(current.parent_hash) else {
                    return Ok(RetainedPathLeaseOutcome::HistoryPruned);
                };
                if parent.height.next().ok() != Some(current.height) {
                    return Err(HeaderChainStoreError::Store(StoreError::Incoherent(
                        "retained target path has non-contiguous heights",
                    )));
                }
                reverse_path.push(Frontier::new(parent.height, parent.hash));
                current = parent;
            }
            (snapshot, target, reverse_path)
        };
        if reverse_path.last().copied() != Some(snapshot.frontiers.finalized) {
            return Ok(RetainedPathLeaseOutcome::HistoryPruned);
        }
        reverse_path.reverse();
        let mut intersection = None;
        for locator_hash in locator_hashes {
            if let Some(common_index) = reverse_path
                .iter()
                .position(|frontier| frontier.hash == *locator_hash)
            {
                intersection = Some((
                    reverse_path[common_index],
                    CanonicalHeaderPathPosition::Retained { next: 0 },
                    common_index.saturating_add(1),
                ));
                break;
            }
            if let Some(frontier) = self.finalized_frontier(*locator_hash)? {
                if frontier.height < snapshot.frontiers.finalized.height {
                    let next = frontier.height.next().map_err(|_| {
                        StoreError::Incoherent("canonical header cursor start height overflowed")
                    })?;
                    intersection = Some((
                        frontier,
                        CanonicalHeaderPathPosition::Finalized {
                            next,
                            end: snapshot.frontiers.finalized.height,
                        },
                        1,
                    ));
                    break;
                }
            }
        }
        let Some((common_ancestor, mut position, retained_start)) = intersection else {
            return Ok(RetainedPathLeaseOutcome::NoLocatorIntersection);
        };
        let retained_path: Arc<[block::Hash]> = reverse_path[retained_start..]
            .iter()
            .map(|frontier| frontier.hash)
            .collect();
        if retained_path.is_empty()
            && matches!(position, CanonicalHeaderPathPosition::Retained { .. })
        {
            position = CanonicalHeaderPathPosition::Complete;
        }
        let _writer = self
            .store
            .writer
            .lock()
            .map_err(|_| HeaderChainStoreError::SynchronizationPoisoned)?;
        let current_snapshot = self
            .transition_engine
            .lock()
            .map_err(|_| HeaderChainStoreError::SynchronizationPoisoned)?
            .snapshot();
        if current_snapshot.state_version != snapshot.state_version
            || scope != HeaderWorkAuthority::for_target(&current_snapshot, target_tip_hash)
        {
            return Ok(RetainedPathLeaseOutcome::Busy);
        }
        reservation.commit(
            RetainedPathLeaseSpec {
                peer,
                session_id,
                target,
                common_ancestor,
                scope,
                position,
                retained_path,
            },
            Instant::now(),
        )
    }

    fn next_canonical_path_item(
        &self,
        cursor: &CanonicalHeaderPathCursor,
        position: &mut CanonicalHeaderPathPosition,
        previous: Frontier,
    ) -> Result<Option<(Frontier, Arc<block::Header>, Vec<AuxDelivery>)>, HeaderChainStoreError>
    {
        match *position {
            CanonicalHeaderPathPosition::Complete => Ok(None),
            CanonicalHeaderPathPosition::Finalized { next, end } => {
                if next > end || previous.height.next().ok() != Some(next) {
                    return Err(StoreError::Incoherent(
                        "finalized canonical header cursor has a non-contiguous height",
                    )
                    .into());
                }
                let hash_by_height = self.store.cf("hash_by_height")?;
                let hash: Option<block::Hash> = self.store.db.zs_get(&hash_by_height, &next);
                let hash = hash.ok_or(StoreError::Incoherent(
                    "finalized canonical header cursor has a missing hash",
                ))?;
                let frontier = Frontier::new(next, hash);
                let header = self.finalized_header(frontier)?;
                if header.previous_block_hash != previous.hash {
                    return Err(StoreError::Incoherent(
                        "finalized canonical header cursor has a non-contiguous parent",
                    )
                    .into());
                }
                *position = if next == end {
                    if cursor.retained_path.is_empty() {
                        CanonicalHeaderPathPosition::Complete
                    } else {
                        CanonicalHeaderPathPosition::Retained { next: 0 }
                    }
                } else {
                    CanonicalHeaderPathPosition::Finalized {
                        next: next.next().map_err(|_| {
                            StoreError::Incoherent(
                                "finalized canonical header cursor height overflowed",
                            )
                        })?,
                        end,
                    }
                };
                Ok(Some((frontier, header, Vec::new())))
            }
            CanonicalHeaderPathPosition::Retained { next } => {
                let Some(hash) = cursor.retained_path.get(next).copied() else {
                    return Err(StoreError::Incoherent(
                        "retained canonical header cursor exceeded its immutable suffix",
                    )
                    .into());
                };
                let node = self
                    .retained_path_node(hash)?
                    .ok_or(StoreError::Incoherent(
                        "active canonical header cursor node is absent",
                    ))?;
                if previous.height.next().ok() != Some(node.height)
                    || node.parent_hash != previous.hash
                {
                    return Err(StoreError::Incoherent(
                        "retained canonical header cursor has a non-contiguous item",
                    )
                    .into());
                }
                let deliveries =
                    self.coherent_aux_deliveries_for(node.hash, &node.aux_delivery_ids)?;
                let frontier = Frontier::new(node.height, node.hash);
                *position = if next.saturating_add(1) == cursor.retained_path.len() {
                    CanonicalHeaderPathPosition::Complete
                } else {
                    CanonicalHeaderPathPosition::Retained {
                        next: next.saturating_add(1),
                    }
                };
                Ok(Some((frontier, node.header, deliveries)))
            }
        }
    }

    pub(crate) fn read_retained_path(
        &self,
        peer: SourceId,
        session_id: u64,
        lease_id: u64,
        scope: HeaderWorkAuthority,
        after_hash: block::Hash,
        max_count: u32,
    ) -> Result<RetainedPathReadOutcome, HeaderChainStoreError> {
        if max_count == 0 || max_count > crate::constants::MAX_HEADER_SYNC_HEIGHT_RANGE {
            return Err(HeaderChainStoreError::Store(StoreError::Incoherent(
                "retained path page count is outside protocol bounds",
            )));
        }
        let lease = self
            .leases
            .lock()
            .map_err(|_| HeaderChainStoreError::SynchronizationPoisoned)?
            .get(peer, session_id, lease_id, Instant::now());
        let Some(lease) = lease else {
            return Ok(RetainedPathReadOutcome::Unavailable);
        };
        if lease.scope != scope {
            return Ok(RetainedPathReadOutcome::Unavailable);
        }
        if after_hash != lease.last_frontier.hash {
            return Ok(RetainedPathReadOutcome::Unavailable);
        }
        let read_version = self.store.snapshot()?.state_version;
        let page_ancestor = lease.last_frontier;
        let count = usize::try_from(max_count).unwrap_or(usize::MAX);
        let mut headers = Vec::with_capacity(count.min(usize::from(u16::MAX)));
        let mut aux_deliveries = Vec::with_capacity(headers.capacity());
        let mut previous = page_ancestor;
        let mut position = lease.position;
        let page_result: Result<bool, HeaderChainStoreError> = (|| {
            while headers.len() < count {
                let Some((frontier, header, deliveries)) =
                    self.next_canonical_path_item(&lease, &mut position, previous)?
                else {
                    break;
                };
                previous = frontier;
                headers.push(header);
                aux_deliveries.push(deliveries);
            }
            let complete = matches!(position, CanonicalHeaderPathPosition::Complete);
            if complete && previous != lease.target {
                return Err(StoreError::Incoherent(
                    "canonical header cursor completed before its exact target",
                )
                .into());
            }
            Ok(complete)
        })();
        let _writer = self
            .store
            .writer
            .lock()
            .map_err(|_| HeaderChainStoreError::SynchronizationPoisoned)?;
        let current_version = self.store.snapshot()?.state_version;
        if current_version != read_version {
            return Ok(RetainedPathReadOutcome::Unavailable);
        }
        let complete = page_result?;
        let advanced = self
            .leases
            .lock()
            .map_err(|_| HeaderChainStoreError::SynchronizationPoisoned)?
            .advance(
                peer,
                session_id,
                lease_id,
                CanonicalHeaderPathAdvance {
                    expected_after: page_ancestor,
                    position,
                    last_frontier: previous,
                    now: Instant::now(),
                },
            );
        if !advanced {
            return Ok(RetainedPathReadOutcome::Unavailable);
        }
        Ok(RetainedPathReadOutcome::Page(Box::new(RetainedPathPage {
            lease_id,
            common_ancestor: page_ancestor,
            target: lease.target,
            scope: lease.scope,
            headers,
            aux_deliveries,
            complete,
        })))
    }

    pub(crate) fn release_retained_path(
        &self,
        peer: SourceId,
        session_id: u64,
        lease_id: u64,
        scope: HeaderWorkAuthority,
    ) -> Result<bool, HeaderChainStoreError> {
        Ok(self
            .leases
            .lock()
            .map_err(|_| HeaderChainStoreError::SynchronizationPoisoned)?
            .release(peer, session_id, lease_id, scope))
    }
}
