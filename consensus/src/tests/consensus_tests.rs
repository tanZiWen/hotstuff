use super::*;
use crate::common::{committee_with_base_port, keys};
use crate::config::Parameters;
use crypto::SecretKey;
use futures::future::try_join_all;
use std::fs;
use tokio::sync::mpsc::channel;
use tokio::task::JoinHandle;
use network::SimpleSender;
use std::net::SocketAddr;

fn spawn_nodes(
    keys: Vec<(PublicKey, SecretKey)>,
    committee: Committee,
    store_path: &str,
) -> Vec<JoinHandle<Vec<Block>>>{
    info!("Spawning nodes with {} key pairs", keys.len());

   keys.into_iter().enumerate().map(|(i, (name, secret))| {
        let committee = committee.clone();
        let parameters = Parameters {
            timeout_delay: 100,
            ..Parameters::default()
        };
        let store_path = format!("{}_{}", store_path, i);
        let store = Store::new(&store_path).unwrap();
        let signature_service = SignatureService::new(secret);
        let (tx_commit, mut rx_commit) = channel(1);
        
        tokio::spawn(async move {
            Consensus::spawn(
                name,
                committee,
                parameters,
                signature_service,
                store,
                tx_commit,
            );
            let mut blocks = Vec::new();
            while let Some(block) = rx_commit.recv().await {
                blocks.push(block);
            }
            info!("Node {} collected {} blocks", name, blocks.len());
            blocks
        })
    }).collect()
}

#[tokio::test]
async fn end_to_end() {
    let committee = committee_with_base_port(15_000);
    let keys = keys();
    env_logger::Builder::new()
         .filter_level(log::LevelFilter::Trace)
        .target(env_logger::Target::Stdout)
        .init();   
    // Run all nodes.
    let store_path = ".db_test_end_to_end";
    let handles = spawn_nodes(keys, committee.clone(), store_path);
    let mut network = SimpleSender::new();

   let addresses: Vec<SocketAddr> = committee
    .authorities
    .iter()
    .map(|(_, x)| x.address).collect();

    tokio::spawn(async move {
          loop {
                let serialized = bincode::serialize(&ConsensusMessage::Producer(Digest::random())).expect("Failed to serialize our payload message");
                let bytes = Bytes::from(serialized.clone());
                network.broadcast(addresses.clone(), bytes.clone()).await;
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
          }
    });

    let blocks_per_node = try_join_all(handles)
        .await
        .expect("One or more nodes failed to terminate correctly");
    info!("Collected blocks from {} nodes", blocks_per_node.len());

    for (i, blocks) in blocks_per_node.iter().enumerate() {
        info!("Node {} produced {} blocks: {:?}", i, blocks.len(), blocks);
        assert!(!blocks.is_empty(), "Node {} produced no blocks", i);
    }
    let last_blocks: Vec<&Block> = blocks_per_node
        .iter()
        .filter_map(|blocks| blocks.last())
        .collect();
    assert!(
        !last_blocks.is_empty(),
        "No blocks were produced by any node"
    );
    assert!(
        last_blocks.len() > 1,
        "At least two nodes are required to compare consistency"
    );
    assert!(
        last_blocks.windows(2).all(|w| w[0] == w[1]),
        "Last blocks are not consistent: {:?}", last_blocks
    );
    info!("All last blocks are consistent");
}


