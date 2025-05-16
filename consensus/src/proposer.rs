use crate::config::{Committee, Stake};
use crate::consensus::{ConsensusMessage, Round};
use crate::core::LASTEST_ROUND;
use crate::messages::{Block, QC, TC};
use bytes::Bytes;
use crypto::{Digest, PublicKey, SignatureService};
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, info};
use network::{CancelHandler, ReliableSender};
use tokio::sync::mpsc::{Receiver, Sender};
use std::collections::HashMap;
use rand::prelude::*;
use store::Store;
use std::convert::TryInto;

#[derive(Debug)]
pub enum ProposerMessage {
    Make(Round, QC, Option<TC>),
    Cleanup(Vec<u64>),
}

pub struct Proposer {
    name: PublicKey,
    committee: Committee,
    signature_service: SignatureService,
    rx_producer: Receiver<Digest>,
    rx_message: Receiver<ProposerMessage>,
    tx_loopback: Sender<Block>,
    buffer: HashMap<Round, Vec<Digest>>,
    network: ReliableSender,
    store: Store,
}

impl Proposer {
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        signature_service: SignatureService,
        rx_producer: Receiver<Digest>,
        rx_message: Receiver<ProposerMessage>,
        tx_loopback: Sender<Block>,
        store: Store,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                signature_service,
                rx_producer,
                rx_message,
                tx_loopback,
                buffer: HashMap::new(),
                network: ReliableSender::new(),
                store,
            }
            .run()
            .await;
        });
    }

    /// Helper function. It waits for a future to complete and then delivers a value.
    async fn waiter(wait_for: CancelHandler, deliver: Stake) -> Stake {
        let _ = wait_for.await;
        deliver
    }

    async fn make_block(&mut self, round: Round, qc: QC, tc: Option<TC>) {
        let lastest_round = self.get_lastest_round().await;

        let payload_round = lastest_round + 1;
        let payload = self.buffer.get(&payload_round);

        if payload.is_none() {
            // No payloads to propose.
            info!("Round: {:?}, No payloads to propose", round);
            return;
        }
        let mut rng: StdRng = StdRng::from_entropy();
        let payload = payload.and_then(|digests| digests.choose(&mut rng)).cloned().unwrap();
        // Generate a new block.
        let block = Block::new(
            qc,
            tc,
            self.name,
            round,
            payload,
            self.signature_service.clone(),
        )
        .await;

        debug!("Created {:?}", block);

        // Broadcast our new block.
        debug!("Broadcasting {:?}", block);
        let (names, addresses): (Vec<_>, _) = self
            .committee
            .broadcast_addresses(&self.name)
            .iter()
            .cloned()
            .unzip();
        let message = bincode::serialize(&ConsensusMessage::Propose(block.clone()))
            .expect("Failed to serialize block");
        let handles = self
            .network
            .broadcast(addresses, Bytes::from(message))
            .await;

        // Send our block to the core for processing.
        self.tx_loopback
            .send(block)
            .await
            .expect("Failed to send block");

        // Control system: Wait for 2f+1 nodes to acknowledge our block before continuing.
        let mut wait_for_quorum: FuturesUnordered<_> = names
            .into_iter()
            .zip(handles.into_iter())
            .map(|(name, handler)| {
                let stake = self.committee.stake(&name);
                Self::waiter(handler, stake)
            })
            .collect();

        let mut total_stake = self.committee.stake(&self.name);
        while let Some(stake) = wait_for_quorum.next().await {
            total_stake += stake;
            if total_stake >= self.committee.quorum_threshold() {
                break;
            }
        }
    }

    async fn get_lastest_payload(&mut self) -> Vec<Digest>{
        let latest_round = self
                    .store
                    .read(LASTEST_ROUND.as_bytes().to_vec())
                    .await
                    .unwrap_or_default();
        let last_round: u64 = u64::from_be_bytes(latest_round.unwrap().try_into().expect("Expected a Vec<u8> of length 8"));

        let store_payload = self.store.read(last_round.to_be_bytes().to_vec()).await.unwrap_or_default();
        bincode::deserialize::<Vec<Digest>>(&store_payload.unwrap_or_default()).unwrap_or_default()
    }

    async fn get_lastest_round(&mut self) -> u64 {
        let latest_round = self
            .store
            .read(LASTEST_ROUND.as_bytes().to_vec())
            .await
            .unwrap_or_default();
        if latest_round.is_none() {
            return 0;
        }
        u64::from_be_bytes(latest_round.unwrap().try_into().expect("Expected a Vec<u8> of length 8")) 
    }

    async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(digest) = self.rx_producer.recv() => {
                    info!("Received payload: {:?}", digest);
                    //TODO: Check if the digest is legal, zk-verify.
                    //TODO: Check if the sender is the producers
                    let lastest_round = self.get_lastest_round().await;
                    info!("Lastest round: {:?}", lastest_round);
                    self.buffer.entry(lastest_round+1)
                        .or_insert_with(Vec::new)
                        .push(digest.clone());
                },
                Some(message) = self.rx_message.recv() => match message {
                    ProposerMessage::Make(round, qc, tc) => self.make_block(round, qc, tc).await,
                    ProposerMessage::Cleanup(rounds) => {
                        for x in &rounds {
                            self.buffer.remove(x);
                        }
                    }
                }
            }
        }
    }
}

