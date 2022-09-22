// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{iter::FromIterator, sync::Arc, time::Duration};

use futures::executor::block_on;
use kvproto::metapb;
use pd_client::PdClient;
use raftstore::store::util::find_peer;
use test_raftstore::*;
use tikv_util::config::ReadableDuration;

#[test]
fn test_witness() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.must_put(b"k1", b"v1");

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
    peer_on_store3.set_role(metapb::PeerRole::Learner);
    cluster
        .pd_client
        .must_add_peer(region.get_id(), peer_on_store3.clone());
    cluster
        .pd_client
        .must_remove_peer(region.get_id(), peer_on_store3.clone());
    peer_on_store3.set_is_witness(false);
    cluster
        .pd_client
        .must_add_peer(region.get_id(), peer_on_store3.clone());
    std::thread::sleep(Duration::from_millis(100));
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    // add a new witness peer
    cluster
        .pd_client
        .must_remove_peer(region.get_id(), peer_on_store3.clone());
    peer_on_store3.set_is_witness(true);
    cluster
        .pd_client
        .must_add_peer(region.get_id(), peer_on_store3.clone());
    std::thread::sleep(Duration::from_millis(100));
    must_get_none(&cluster.get_engine(3), b"k1");
}

#[test]
fn test_witness_leader() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.must_put(b"k1", b"v1");

    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();
    let mut peer_on_store1 = find_peer(&region, nodes[0]).unwrap().clone();
    cluster.must_transfer_leader(region.get_id(), peer_on_store1.clone());

    // nonwitness -> witness
    peer_on_store1.set_is_witness(true);
    cluster
        .pd_client
        .add_peer(region.get_id(), peer_on_store1.clone());

    // leader changes to witness failed
    std::thread::sleep(Duration::from_millis(100));
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");
}

#[test]
fn test_witness_leader_down() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.must_put(b"k0", b"v0");

    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();
    let mut peer_on_store1 = find_peer(&region, nodes[0]).unwrap().clone();
    cluster.must_transfer_leader(region.get_id(), peer_on_store1.clone());

    let mut peer_on_store2 = find_peer(&region, nodes[1]).unwrap().clone();
    // nonwitness -> witness
    peer_on_store2.set_is_witness(true);

    cluster
        .pd_client
        .add_peer(region.get_id(), peer_on_store2.clone());

    // the other follower is isolated
    cluster.add_send_filter(IsolationFilterFactory::new(3));
    for i in 1..100 {
        cluster.must_put(format!("k{}", i).as_bytes(), format!("v{}", i).as_bytes());
    }
    // the leader is down
    cluster.stop_node(1);

    cluster.clear_send_filters();
    std::thread::sleep(Duration::from_millis(1000));
    assert_eq!(
        cluster.leader_of_region(region.get_id()).unwrap().store_id,
        3
    );
    assert_eq!(cluster.must_get(b"k99"), Some(b"v99".to_vec()));
}

fn test_non_witness_availability(fp: &str) {
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
    fail::cfg(fp, "return").unwrap();
    peer_on_store3.set_role(metapb::PeerRole::Learner);
    peer_on_store3.set_is_witness(false);
    cluster
        .pd_client
        .must_add_peer(region.get_id(), peer_on_store3.clone());
    std::thread::sleep(Duration::from_millis(20));
    // Conf changed, but applying snapshot not yet completed
    must_get_none(&cluster.get_engine(3), b"k1");
    assert_eq!(cluster.pd_client.get_pending_peers().len(), 1);
    std::thread::sleep(Duration::from_millis(200));
    // snapshot applied
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");
    assert_eq!(cluster.pd_client.get_pending_peers().len(), 0);
    fail::remove(fp);
}

#[test]
fn test_pull_non_witness_availability() {
    test_non_witness_availability("ignore notify leader non-witness is available");
}

#[test]
fn test_push_non_witness_availability() {
    test_non_witness_availability("ignore schedule check non-witness availability tick");
}
