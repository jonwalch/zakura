//! Tests for the address book.

#![allow(clippy::unwrap_in_result)]

use std::net::{IpAddr, Ipv4Addr};

use chrono::Utc;
use tracing::Span;
use zakura_chain::{parameters::Network::Mainnet, serialization::DateTime32};

use crate::{
    constants::{DEFAULT_MAX_CONNS_PER_IP, MAX_ADDRS_IN_ADDRESS_BOOK, MAX_BANNED_IPS},
    meta_addr::{MetaAddr, PeerSocketAddr},
    protocol::external::types::PeerServices,
};

use super::{AddressBook, AddressMetrics, BanList, BannedIps};

mod prop;
mod vectors;

#[test]
fn ban_list_evicts_the_oldest_ip_at_capacity() {
    let mut bans = BanList::default();
    let oldest = IpAddr::V4(Ipv4Addr::from(1));

    for ip in 1..=MAX_BANNED_IPS {
        bans.insert(IpAddr::V4(Ipv4Addr::from(u32::try_from(ip).unwrap())));
    }

    let newest = IpAddr::V4(Ipv4Addr::from(u32::try_from(MAX_BANNED_IPS + 1).unwrap()));
    bans.insert(newest);

    assert!(!bans.ips.contains(&oldest));
    assert!(bans.ips.contains(&newest));
    assert_eq!(bans.ips.len(), MAX_BANNED_IPS);
    assert_eq!(bans.insertion_order.len(), MAX_BANNED_IPS);
}

#[test]
fn banned_ips_match_ipv4_and_ipv4_mapped_ipv6() {
    let ipv4 = IpAddr::V4(Ipv4Addr::LOCALHOST);
    let ipv4_mapped = IpAddr::V6(Ipv4Addr::LOCALHOST.to_ipv6_mapped());

    assert!(BannedIps::with_banned_ip(ipv4).contains(ipv4_mapped));
    assert!(BannedIps::with_banned_ip(ipv4_mapped).contains(ipv4));
}

#[test]
fn address_metrics_count_each_peer_state() {
    let addrs: [PeerSocketAddr; 4] = [
        "11.1.1.1:8233".parse().unwrap(),
        "11.1.1.2:8233".parse().unwrap(),
        "11.1.1.3:8233".parse().unwrap(),
        "11.1.1.4:8233".parse().unwrap(),
    ];
    let initial_addrs = addrs.map(|addr| {
        MetaAddr::new_gossiped_meta_addr(addr, PeerServices::NODE_NETWORK, DateTime32::MIN)
    });
    let mut address_book = AddressBook::new_with_addrs(
        "0.0.0.0:0".parse().unwrap(),
        &Mainnet,
        DEFAULT_MAX_CONNS_PER_IP,
        MAX_ADDRS_IN_ADDRESS_BOOK,
        Span::none(),
        initial_addrs,
    );

    address_book.update(MetaAddr::new_reconnect(addrs[0]));
    address_book.update(MetaAddr::new_responded(addrs[0], None));
    address_book.update(MetaAddr::new_reconnect(addrs[1]));
    address_book.update(MetaAddr::new_errored(
        addrs[1],
        Option::<PeerServices>::None,
    ));
    address_book.update(MetaAddr::new_reconnect(addrs[2]));

    assert_eq!(
        address_book.address_metrics(Utc::now()),
        AddressMetrics {
            responded: 1,
            never_attempted_gossiped: 1,
            failed: 1,
            attempt_pending: 1,
            recently_live: 1,
            recently_stopped_responding: 0,
            num_addresses: 4,
            address_limit: MAX_ADDRS_IN_ADDRESS_BOOK,
        }
    );
}
