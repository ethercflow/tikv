// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{iter::FromIterator, sync::Arc, time::Duration};

use futures::executor::block_on;
use kvproto::metapb;
use pd_client::PdClient;
use test_raftstore::*;
use tikv_util::config::ReadableDuration;

fn find_peer(region: &metapb::Region, store_id: u64) -> Option<&metapb::Peer> {
    region
        .get_peers()
        .iter()
        .find(|&p| p.get_store_id() == store_id)
}

#[test]
fn test_pull_peer_availablity_info() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.cfg.raft_store.pd_heartbeat_tick_interval = ReadableDuration::millis(100);
    cluster
        .cfg
        .raft_store
        .check_non_witnesses_availability_interval = ReadableDuration::millis(20);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();
    let peer_on_store1 = find_peer(&region, nodes[1]).unwrap();
    cluster.must_transfer_leader(region.get_id(), peer_on_store1.clone());

    // nonwitness -> witness
    let mut peer_on_store3 = find_peer(&region, nodes[2]).unwrap().clone();
    peer_on_store3.set_is_witness(true);
}
