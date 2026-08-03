//! An async task that regularly updates the peer cache on disk from the current address book.

use std::{
    io,
    sync::{Arc, Mutex},
};

use chrono::Utc;
use tokio::time::sleep;

use crate::{
    address_book_trace::AddressBookTrace,
    constants::{DNS_LOOKUP_TIMEOUT, PEER_DISK_CACHE_UPDATE_INTERVAL},
    meta_addr::MetaAddr,
    AddressBook, BoxError, Config,
};

/// The number of individual addresses reported in each dial candidate snapshot.
///
/// Taken in reconnection order, so these are the addresses the crawler reaches first.
/// The address book holds up to [`MAX_ADDRS_IN_ADDRESS_BOOK`], and writing every entry
/// on every interval would make the trace file grow much faster than it is read.
///
/// [`MAX_ADDRS_IN_ADDRESS_BOOK`]: crate::constants::MAX_ADDRS_IN_ADDRESS_BOOK
const SNAPSHOT_ENTRY_LIMIT: usize = 256;

/// An ongoing task that regularly caches the current `address_book` to disk, based on `config`.
#[instrument(skip(config, address_book))]
pub async fn peer_cache_updater(
    config: Config,
    address_book: Arc<Mutex<AddressBook>>,
) -> Result<(), BoxError> {
    // Wait until we've queried DNS and (hopefully) sent peers to the address book.
    // Ideally we'd wait for at least one peer crawl, but that makes tests very slow.
    //
    // TODO: turn the initial sleep time into a parameter of this function,
    //       and allow it to be set in tests
    sleep(DNS_LOOKUP_TIMEOUT * 4).await;

    let mut wrote_cache = false;

    // Snapshots share this task's timer because both summarise the whole address book,
    // and neither should hold the address book lock more often than that.
    let trace = AddressBookTrace::new(
        config.zakura.trace_dir.clone(),
        config.expose_peer_addresses,
    );

    loop {
        // Ignore errors because updating the cache is optional.
        // Errors are already logged by the functions we're calling.
        wrote_cache |= update_peer_cache_once(&config, &address_book)
            .await
            .unwrap_or(false);

        snapshot_dial_candidates_once(&trace, &address_book);

        // Right after a cold start, the address book can still be empty at the first attempt,
        // and the cache is only written once there are cacheable peers. Retry soon until the
        // first write, so the cache exists shortly after the node finds its first peers.
        let interval = if wrote_cache {
            PEER_DISK_CACHE_UPDATE_INTERVAL
        } else {
            DNS_LOOKUP_TIMEOUT * 4
        };

        sleep(interval).await;
    }
}

/// Caches peers from the current `address_book` to disk, based on `config`.
///
/// Returns `true` if the cache was written, and `false` if the cacheable peer list was empty,
/// keeping any previous cache.
pub async fn update_peer_cache_once(
    config: &Config,
    address_book: &Arc<Mutex<AddressBook>>,
) -> io::Result<bool> {
    let peer_list: std::collections::HashSet<_> = cacheable_peers(address_book)
        .iter()
        .map(|meta_addr| meta_addr.addr)
        .collect();
    let has_peers = !peer_list.is_empty();

    config.update_peer_cache(peer_list).await?;

    Ok(has_peers)
}

/// Writes one snapshot of the address book's dial candidates, if tracing is configured.
fn snapshot_dial_candidates_once(trace: &AddressBookTrace, address_book: &Arc<Mutex<AddressBook>>) {
    if !trace.is_enabled() {
        return;
    }

    let instant_now = std::time::Instant::now();
    let chrono_now = Utc::now();

    // # Concurrency
    //
    // Build the report while holding the lock, then release it before serialising.
    let report = address_book
        .lock()
        .expect("unexpected panic in previous thread while accessing the address book")
        .dial_candidate_report(instant_now, chrono_now, SNAPSHOT_ENTRY_LIMIT);

    trace.snapshot(&report);
}

/// Returns a list of cacheable peers, blocking for as short a time as possible.
fn cacheable_peers(address_book: &Arc<Mutex<AddressBook>>) -> Vec<MetaAddr> {
    // TODO: use spawn_blocking() here, if needed to handle address book mutex load
    let now = Utc::now();

    // # Concurrency
    //
    // We return from this function immediately to make sure the address book is unlocked.
    address_book
        .lock()
        .expect("unexpected panic in previous thread while accessing the address book")
        .cacheable(now)
}
