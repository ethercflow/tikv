// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{iter::FromIterator, sync::Arc, time::Duration};

use futures::executor::block_on;
use kvproto::metapb;
use pd_client::PdClient;
use raftstore::store::util::find_peer;
use test_raftstore::*;
use tikv_util::config::ReadableDuration;

#[test]
fn test_request_snapshot_after_reboot() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.raft_store.pd_heartbeat_tick_interval = ReadableDuration::millis(100);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);

    cluster.must_put(b"k1", b"v1");

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();
    let peer_on_store1 = find_peer(&region, nodes[1]).unwrap();
    cluster.must_transfer_leader(region.get_id(), peer_on_store1.clone());

    // nonwitness -> witness
    let mut peer_on_store3 = find_peer(&region, nodes[2]).unwrap().clone();
    peer_on_store3.set_is_witness(true);
    cluster
        .pd_client
        .must_add_peer(region.get_id(), peer_on_store3.clone());

    std::thread::sleep(Duration::from_millis(100));
    must_get_none(&cluster.get_engine(3), b"k1");

    // witness -> nonwitness
    let fp = "ignore request snapshot";
    fail::cfg(fp, "return").unwrap();
    peer_on_store3.set_role(metapb::PeerRole::Learner);
    peer_on_store3.set_is_witness(false);
    cluster
        .pd_client
        .must_add_peer(region.get_id(), peer_on_store3.clone());
    std::thread::sleep(Duration::from_millis(20));
    // conf changed, but applying snapshot not yet completed
    assert_eq!(cluster.pd_client.get_pending_peers().len(), 1);
    must_get_none(&cluster.get_engine(3), b"k1");
    std::thread::sleep(Duration::from_millis(100));
    // as we ignore request snapshot, so snapshot should still not applied yet
    assert_eq!(cluster.pd_client.get_pending_peers().len(), 1);
    must_get_none(&cluster.get_engine(3), b"k1");

    cluster.stop_node(nodes[2]);
    fail::remove(fp);
    std::thread::sleep(Duration::from_millis(10));
    // the PeerState is Unavailable, so it will request snapshot immediately after
    // start.
    cluster.run_node(nodes[2]).unwrap();
    std::thread::sleep(Duration::from_millis(200));
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");
    assert_eq!(cluster.pd_client.get_pending_peers().len(), 0);
}

#[test]
fn test_delayed_request_snapshot() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.raft_store.pd_heartbeat_tick_interval = ReadableDuration::millis(100);
    cluster
        .cfg
        .raft_store
        .check_non_witnesses_availability_interval = ReadableDuration::millis(20);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);

    cluster.must_put(b"k1", b"v1");

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();
    let peer_on_store1 = find_peer(&region, nodes[1]).unwrap();
    cluster.must_transfer_leader(region.get_id(), peer_on_store1.clone());

    // nonwitness -> witness
    let mut peer_on_store3 = find_peer(&region, nodes[2]).unwrap().clone();
    peer_on_store3.set_is_witness(true);
    cluster
        .pd_client
        .must_add_peer(region.get_id(), peer_on_store3.clone());

    std::thread::sleep(Duration::from_millis(100));
    must_get_none(&cluster.get_engine(3), b"k1");

    // witness -> nonwitness
    let fp = "before request snapshot";
    fail::cfg(fp, "sleep(5000)").unwrap();
    peer_on_store3.set_role(metapb::PeerRole::Learner);
    peer_on_store3.set_is_witness(false);
    cluster
        .pd_client
        .must_add_peer(region.get_id(), peer_on_store3.clone());
    std::thread::sleep(Duration::from_millis(20));
    // conf changed, but applying snapshot not yet completed
    assert_eq!(cluster.pd_client.get_pending_peers().len(), 1);
    must_get_none(&cluster.get_engine(3), b"k1");
    for i in 0..10_u32 {
        cluster.must_put(i.to_be_bytes().as_slice(), i.to_be_bytes().as_slice());
    }
    std::thread::sleep(Duration::from_secs(10));
    // as we ignore request snapshot, so snapshot should still not applied yet
    assert_eq!(cluster.pd_client.get_pending_peers().len(), 0);
    must_get_none(&cluster.get_engine(3), b"k1");
}
