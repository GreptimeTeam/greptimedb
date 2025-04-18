// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;

use api::v1::meta::mailbox_message::Payload;
use api::v1::meta::{HeartbeatResponse, MailboxMessage};
use common_meta::instruction::{
    DowngradeRegionReply, InstructionReply, SimpleReply, UpgradeRegionReply,
};
use common_meta::key::table_route::TableRouteValue;
use common_meta::key::test_utils::new_test_table_info;
use common_meta::key::topic_name::TopicNameKey;
use common_meta::key::TableMetadataManagerRef;
use common_meta::peer::Peer;
use common_meta::region_registry::{
    LeaderRegion, LeaderRegionManifestInfo, LeaderRegionRegistryRef,
};
use common_meta::rpc::router::{Region, RegionRoute};
use common_meta::sequence::Sequence;
use common_time::util::current_time_millis;
use common_wal::options::{KafkaWalOptions, WalOptions};
use store_api::logstore::EntryId;
use store_api::storage::RegionId;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::error::Result;
use crate::handler::{HeartbeatMailbox, Pusher, Pushers};
use crate::service::mailbox::{Channel, MailboxRef};

pub type MockHeartbeatReceiver = Receiver<std::result::Result<HeartbeatResponse, tonic::Status>>;

/// The context of mailbox.
pub struct MailboxContext {
    mailbox: MailboxRef,
    // The pusher is used in the mailbox.
    pushers: Pushers,
}

impl MailboxContext {
    pub fn new(sequence: Sequence) -> Self {
        let pushers = Pushers::default();
        let mailbox = HeartbeatMailbox::create(pushers.clone(), sequence);

        Self { mailbox, pushers }
    }

    /// Inserts a pusher for `datanode_id`
    pub async fn insert_heartbeat_response_receiver(
        &mut self,
        channel: Channel,
        tx: Sender<std::result::Result<HeartbeatResponse, tonic::Status>>,
    ) {
        let pusher_id = channel.pusher_id();
        let pusher = Pusher::new(tx);
        let _ = self.pushers.insert(pusher_id.string_key(), pusher).await;
    }

    pub fn mailbox(&self) -> &MailboxRef {
        &self.mailbox
    }
}

/// Sends a mock reply.
pub fn send_mock_reply(
    mailbox: MailboxRef,
    mut rx: MockHeartbeatReceiver,
    msg: impl Fn(u64) -> Result<MailboxMessage> + Send + 'static,
) {
    common_runtime::spawn_global(async move {
        while let Some(Ok(resp)) = rx.recv().await {
            let reply_id = resp.mailbox_message.unwrap().id;
            mailbox.on_recv(reply_id, msg(reply_id)).await.unwrap();
        }
    });
}

/// Generates a [InstructionReply::OpenRegion] reply.
pub fn new_open_region_reply(id: u64, result: bool, error: Option<String>) -> MailboxMessage {
    MailboxMessage {
        id,
        subject: "mock".to_string(),
        from: "datanode".to_string(),
        to: "meta".to_string(),
        timestamp_millis: current_time_millis(),
        payload: Some(Payload::Json(
            serde_json::to_string(&InstructionReply::OpenRegion(SimpleReply { result, error }))
                .unwrap(),
        )),
    }
}

/// Generates a [InstructionReply::CloseRegion] reply.
pub fn new_close_region_reply(id: u64) -> MailboxMessage {
    MailboxMessage {
        id,
        subject: "mock".to_string(),
        from: "datanode".to_string(),
        to: "meta".to_string(),
        timestamp_millis: current_time_millis(),
        payload: Some(Payload::Json(
            serde_json::to_string(&InstructionReply::CloseRegion(SimpleReply {
                result: false,
                error: None,
            }))
            .unwrap(),
        )),
    }
}

/// Generates a [InstructionReply::DowngradeRegion] reply.
pub fn new_downgrade_region_reply(
    id: u64,
    last_entry_id: Option<u64>,
    exist: bool,
    error: Option<String>,
) -> MailboxMessage {
    MailboxMessage {
        id,
        subject: "mock".to_string(),
        from: "datanode".to_string(),
        to: "meta".to_string(),
        timestamp_millis: current_time_millis(),
        payload: Some(Payload::Json(
            serde_json::to_string(&InstructionReply::DowngradeRegion(DowngradeRegionReply {
                last_entry_id,
                metadata_last_entry_id: None,
                exists: exist,
                error,
            }))
            .unwrap(),
        )),
    }
}

/// Generates a [InstructionReply::UpgradeRegion] reply.
pub fn new_upgrade_region_reply(
    id: u64,
    ready: bool,
    exists: bool,
    error: Option<String>,
) -> MailboxMessage {
    MailboxMessage {
        id,
        subject: "mock".to_string(),
        from: "datanode".to_string(),
        to: "meta".to_string(),
        timestamp_millis: current_time_millis(),
        payload: Some(Payload::Json(
            serde_json::to_string(&InstructionReply::UpgradeRegion(UpgradeRegionReply {
                ready,
                exists,
                error,
            }))
            .unwrap(),
        )),
    }
}

pub async fn new_wal_prune_metadata(
    table_metadata_manager: TableMetadataManagerRef,
    leader_region_registry: LeaderRegionRegistryRef,
    n_region: u32,
    n_table: u32,
    offsets: &[i64],
    threshold: u64,
    topic: String,
) -> (EntryId, Vec<RegionId>) {
    let datanode_id = 1;
    let from_peer = Peer::empty(datanode_id);
    let mut min_prunable_entry_id = u64::MAX;
    let mut max_prunable_entry_id = 0;
    let mut region_entry_ids = HashMap::with_capacity(n_table as usize * n_region as usize);
    for table_id in 0..n_table {
        let region_ids = (0..n_region)
            .map(|i| RegionId::new(table_id, i))
            .collect::<Vec<_>>();
        let table_info = new_test_table_info(table_id, 0..n_region).into();
        let region_routes = region_ids
            .iter()
            .map(|region_id| RegionRoute {
                region: Region::new_test(*region_id),
                leader_peer: Some(from_peer.clone()),
                ..Default::default()
            })
            .collect::<Vec<_>>();
        let wal_options = WalOptions::Kafka(KafkaWalOptions {
            topic: topic.clone(),
        });
        let wal_options = serde_json::to_string(&wal_options).unwrap();
        let region_wal_options: HashMap<u32, String> = (0..n_region)
            .map(|region_number| (region_number, wal_options.clone()))
            .collect();

        table_metadata_manager
            .create_table_metadata(
                table_info,
                TableRouteValue::physical(region_routes),
                region_wal_options,
            )
            .await
            .unwrap();
        table_metadata_manager
            .topic_name_manager()
            .batch_put(vec![TopicNameKey::new(&topic)])
            .await
            .unwrap();

        let current_region_entry_ids = region_ids
            .iter()
            .map(|region_id| {
                let rand_n = rand::random::<u64>() as usize;
                let current_prunable_entry_id = offsets[rand_n % offsets.len()] as u64;
                min_prunable_entry_id = min_prunable_entry_id.min(current_prunable_entry_id);
                max_prunable_entry_id = max_prunable_entry_id.max(current_prunable_entry_id);
                (*region_id, current_prunable_entry_id)
            })
            .collect::<HashMap<_, _>>();
        region_entry_ids.extend(current_region_entry_ids.clone());
        update_in_memory_region_flushed_entry_id(&leader_region_registry, current_region_entry_ids)
            .await
            .unwrap();
    }

    let regions_to_flush = region_entry_ids
        .iter()
        .filter_map(|(region_id, prunable_entry_id)| {
            if max_prunable_entry_id - prunable_entry_id > threshold {
                Some(*region_id)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    (min_prunable_entry_id, regions_to_flush)
}

pub async fn update_in_memory_region_flushed_entry_id(
    leader_region_registry: &LeaderRegionRegistryRef,
    region_entry_ids: HashMap<RegionId, u64>,
) -> Result<()> {
    let mut key_values = Vec::with_capacity(region_entry_ids.len());
    for (region_id, flushed_entry_id) in region_entry_ids {
        let value = LeaderRegion {
            datanode_id: 1,
            manifest: LeaderRegionManifestInfo::Mito {
                manifest_version: 0,
                flushed_entry_id,
                topic_latest_entry_id: 0,
            },
        };
        key_values.push((region_id, value));
    }
    leader_region_registry.batch_put(key_values);

    Ok(())
}
