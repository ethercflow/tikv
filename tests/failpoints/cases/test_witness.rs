// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{iter::FromIterator, sync::Arc, time::Duration};

use futures::executor::block_on;
use pd_client::PdClient;
use test_raftstore::*;
use tikv_util::{config::ReadableDuration, store::find_peer};

// Test the case local reader works well with witness peer.
#[test]
fn test_witness_update_region_in_local_reader() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();
    let peer_on_store1 = find_peer(&region, nodes[0]).unwrap().clone();
    cluster.must_transfer_leader(region.get_id(), peer_on_store1);
    // nonwitness -> witness
    let peer_on_store3 = find_peer(&region, nodes[2]).unwrap().clone();
    cluster.pd_client.must_switch_witnesses(
        region.get_id(),
        vec![peer_on_store3.get_id()],
        vec![true],
    );

    cluster.must_put(b"k0", b"v0");

    // update region but the peer is not destroyed yet
    fail::cfg("change_peer_after_update_region_store_3", "pause").unwrap();

    cluster
        .pd_client
        .must_remove_peer(region.get_id(), peer_on_store3.clone());

    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();
    let mut request = new_request(
        region.get_id(),
        region.get_region_epoch().clone(),
        vec![new_get_cmd(b"k0")],
        false,
    );
    request.mut_header().set_peer(peer_on_store3);
    request.mut_header().set_replica_read(true);

    let resp = cluster
        .read(None, request.clone(), Duration::from_millis(100))
        .unwrap();
    assert_eq!(
        resp.get_header().get_error().get_recovery_in_progress(),
        &kvproto::errorpb::RecoveryInProgress {
            region_id: region.get_id(),
            ..Default::default()
        }
    );

    fail::remove("change_peer_after_update_region_store_3");
}

// Test the case request snapshot and apply successfully after non-witness
// restart.
#[test]
fn test_request_snapshot_after_reboot() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.cfg.raft_store.pd_heartbeat_tick_interval = ReadableDuration::millis(20);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();
    let peer_on_store1 = find_peer(&region, nodes[1]).unwrap();
    cluster.must_transfer_leader(region.get_id(), peer_on_store1.clone());
    // nonwitness -> witness
    let peer_on_store3 = find_peer(&region, nodes[2]).unwrap().clone();
    cluster.pd_client.must_switch_witnesses(
        region.get_id(),
        vec![peer_on_store3.get_id()],
        vec![true],
    );

    cluster.must_put(b"k1", b"v1");

    std::thread::sleep(Duration::from_millis(100));
    must_get_none(&cluster.get_engine(3), b"k1");

    // witness -> nonwitness
    let fp = "ignore request snapshot";
    fail::cfg(fp, "return").unwrap();
    cluster
        .pd_client
        .switch_witnesses(region.get_id(), vec![peer_on_store3.get_id()], vec![false]);
    std::thread::sleep(Duration::from_millis(500));
    // as we ignore request snapshot, so snapshot should still not applied yet
    assert_eq!(cluster.pd_client.get_pending_peers().len(), 1);
    must_get_none(&cluster.get_engine(3), b"k1");

    cluster.stop_node(nodes[2]);
    fail::remove(fp);
    std::thread::sleep(Duration::from_millis(100));
    // the PeerState is Unavailable, so it will request snapshot immediately after
    // start.
    cluster.run_node(nodes[2]).unwrap();
    must_get_none(&cluster.get_engine(3), b"k1");
    std::thread::sleep(Duration::from_millis(500));
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");
    assert_eq!(cluster.pd_client.get_pending_peers().len(), 0);
}

fn test_non_witness_availability(fp: &str) {
    let mut cluster = new_server_cluster(0, 3);
    cluster.cfg.raft_store.pd_heartbeat_tick_interval = ReadableDuration::millis(100);
    cluster.cfg.raft_store.check_peers_availability_interval = ReadableDuration::millis(20);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();
    let peer_on_store1 = find_peer(&region, nodes[1]).unwrap();
    cluster.must_transfer_leader(region.get_id(), peer_on_store1.clone());

    // non-witness -> witness
    let peer_on_store3 = find_peer(&region, nodes[2]).unwrap().clone();
    cluster.pd_client.must_switch_witnesses(
        region.get_id(),
        vec![peer_on_store3.get_id()],
        vec![true],
    );

    cluster.must_put(b"k1", b"v1");

    std::thread::sleep(Duration::from_millis(100));
    must_get_none(&cluster.get_engine(3), b"k1");

    fail::cfg(fp, "return").unwrap();

    // witness -> non-witness
    cluster
        .pd_client
        .switch_witnesses(region.get_id(), vec![peer_on_store3.get_id()], vec![false]);
    std::thread::sleep(Duration::from_millis(500));
    // snapshot applied
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");
    assert_eq!(cluster.pd_client.get_pending_peers().len(), 0);
    fail::remove(fp);
}

// Test the case leader pulls non-witness availability when non-witness failed
// to push the info.
#[test]
fn test_pull_non_witness_availability() {
    test_non_witness_availability("ignore notify leader the peer is available");
}

// Test the case non-witness pushes its availability without leader pulling.
#[test]
fn test_push_non_witness_availability() {
    test_non_witness_availability("ignore schedule check non-witness availability tick");
}

// Test the case non-witness hasn't finish applying snapshot when receives read
// request.
#[test]
fn test_non_witness_replica_read() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.must_put(b"k0", b"v0");

    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();
    let peer_on_store1 = find_peer(&region, nodes[0]).unwrap().clone();
    cluster.must_transfer_leader(region.get_id(), peer_on_store1);
    // nonwitness -> witness
    let peer_on_store3 = find_peer(&region, nodes[2]).unwrap().clone();
    cluster.pd_client.must_switch_witnesses(
        region.get_id(),
        vec![peer_on_store3.get_id()],
        vec![true],
    );

    // witness -> nonwitness
    fail::cfg("ignore request snapshot", "return").unwrap();
    cluster
        .pd_client
        .switch_witnesses(region.get_id(), vec![peer_on_store3.get_id()], vec![false]);
    std::thread::sleep(Duration::from_millis(100));
    // as we ignore request snapshot, so snapshot should still not applied yet

    let mut request = new_request(
        region.get_id(),
        region.get_region_epoch().clone(),
        vec![new_get_cmd(b"k0")],
        false,
    );
    request.mut_header().set_peer(peer_on_store3.clone());
    request.mut_header().set_replica_read(true);

    let resp = cluster
        .read(None, request, Duration::from_millis(100))
        .unwrap();
    assert_eq!(
        resp.get_header().get_error().get_recovery_in_progress(),
        &kvproto::errorpb::RecoveryInProgress {
            region_id: region.get_id(),
            ..Default::default()
        }
    );

    // start requesting snapshot and give enough time for applying snapshot to
    // complete
    fail::remove("ignore request snapshot");
    std::thread::sleep(Duration::from_millis(500));

    let mut request = new_request(
        region.get_id(),
        region.get_region_epoch().clone(),
        vec![new_get_cmd(b"k0")],
        false,
    );
    request.mut_header().set_peer(peer_on_store3);
    request.mut_header().set_replica_read(true);

    let resp = cluster
        .read(None, request, Duration::from_millis(100))
        .unwrap();
    assert_eq!(resp.get_header().has_error(), false);
}
