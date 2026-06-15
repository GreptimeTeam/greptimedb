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

use common_meta::DatanodeId;
use common_meta::key::datanode_table::DatanodeTableManager;
use common_meta::key::topic_name::{TopicNameKey, TopicNameManager, TopicNameValue};
use common_meta::key::topic_region::{
    ReplayCheckpoint as MetadataReplayCheckpoint, TopicRegionKey, TopicRegionManager,
    TopicRegionValue,
};
use common_meta::kv_backend::KvBackendRef;
use common_meta::wal_provider::{
    RegionWalOptions, extract_topic_from_wal_options, serialize_wal_options,
};
use futures::TryStreamExt;
use snafu::ResultExt;
use store_api::metric_engine_consts::METRIC_ENGINE_NAME;
use store_api::path_utils::table_dir;
use store_api::region_request::{PathType, RegionOpenRequest, ReplayCheckpoint};
use store_api::storage::RegionId;
use tracing::info;

use crate::error::{GetMetadataSnafu, Result, SerializeWalOptionsSnafu};

/// The requests to open regions.
pub struct RegionOpenRequests {
    pub(crate) leader_regions: Vec<(RegionId, RegionOpenRequest)>,
    #[cfg(feature = "enterprise")]
    pub(crate) follower_regions: Vec<(RegionId, RegionOpenRequest)>,
}

impl RegionOpenRequests {
    /// Splits the request set into leader and follower regions.
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        Vec<(RegionId, RegionOpenRequest)>,
        Vec<(RegionId, RegionOpenRequest)>,
    ) {
        let leader_regions = self.leader_regions;
        #[cfg(feature = "enterprise")]
        let follower_regions = self.follower_regions;
        #[cfg(not(feature = "enterprise"))]
        let follower_regions = Vec::new();
        (leader_regions, follower_regions)
    }
}

fn group_region_by_topic(
    region_id: RegionId,
    region_options: &RegionWalOptions,
    topic_regions: &mut HashMap<String, Vec<RegionId>>,
) {
    if let Some(topic) = extract_topic_from_wal_options(region_id, region_options) {
        topic_regions.entry(topic).or_default().push(region_id);
    }
}

fn region_pruned_entry_ids(
    topic_regions: &HashMap<String, Vec<RegionId>>,
    topic_name_values: &HashMap<String, TopicNameValue>,
) -> HashMap<RegionId, u64> {
    topic_regions
        .iter()
        .flat_map(|(topic, region_ids)| {
            topic_name_values
                .get(topic)
                .into_iter()
                .flat_map(move |value| {
                    region_ids
                        .iter()
                        .map(move |region_id| (*region_id, value.pruned_entry_id))
                })
        })
        .collect()
}

fn get_replay_checkpoint(
    region_id: RegionId,
    topic_region_values: &Option<HashMap<RegionId, TopicRegionValue>>,
    pruned_entry_id: Option<u64>,
    is_metric_engine: bool,
) -> Option<ReplayCheckpoint> {
    let checkpoint = topic_region_values
        .as_ref()
        .and_then(|values| values.get(&region_id))
        .and_then(|value| value.checkpoint)
        .map(|checkpoint| {
            MetadataReplayCheckpoint::new(checkpoint.entry_id, checkpoint.metadata_entry_id)
        });

    MetadataReplayCheckpoint::merge_with_topic_pruned_entry_id(
        checkpoint,
        pruned_entry_id,
        is_metric_engine,
    )
    .map(|checkpoint| ReplayCheckpoint {
        entry_id: checkpoint.entry_id,
        metadata_entry_id: checkpoint.metadata_entry_id,
    })
}

/// Builds region-open requests from persisted metadata.
pub async fn build_region_open_requests(
    node_id: DatanodeId,
    kv_backend: KvBackendRef,
) -> Result<RegionOpenRequests> {
    let datanode_table_manager = DatanodeTableManager::new(kv_backend.clone());
    let table_values = datanode_table_manager
        .tables(node_id)
        .try_collect::<Vec<_>>()
        .await
        .context(GetMetadataSnafu)?;

    let topic_region_manager = TopicRegionManager::new(kv_backend.clone());
    let topic_name_manager = TopicNameManager::new(kv_backend);
    let mut topic_regions = HashMap::<String, Vec<RegionId>>::new();
    let mut regions = vec![];
    #[cfg(feature = "enterprise")]
    let mut follower_regions = vec![];

    for table_value in table_values {
        for region_number in table_value.regions {
            let region_id = RegionId::new(table_value.table_id, region_number);
            // Augments region options with wal options if a wal options is provided.
            let mut region_options = table_value.region_info.region_options.clone();
            serialize_wal_options(
                &mut region_options,
                region_id,
                &table_value.region_info.region_wal_options,
            )
            .context(SerializeWalOptionsSnafu { region_id })?;
            group_region_by_topic(
                region_id,
                &table_value.region_info.region_wal_options,
                &mut topic_regions,
            );

            regions.push((
                region_id,
                table_value.region_info.engine.clone(),
                table_value.region_info.region_storage_path.clone(),
                region_options,
            ));
        }

        #[cfg(feature = "enterprise")]
        for region_number in table_value.follower_regions {
            let region_id = RegionId::new(table_value.table_id, region_number);
            // Augments region options with wal options if a wal options is provided.
            let mut region_options = table_value.region_info.region_options.clone();
            serialize_wal_options(
                &mut region_options,
                RegionId::new(table_value.table_id, region_number),
                &table_value.region_info.region_wal_options,
            )
            .context(SerializeWalOptionsSnafu { region_id })?;
            group_region_by_topic(
                region_id,
                &table_value.region_info.region_wal_options,
                &mut topic_regions,
            );

            follower_regions.push((
                RegionId::new(table_value.table_id, region_number),
                table_value.region_info.engine.clone(),
                table_value.region_info.region_storage_path.clone(),
                region_options,
            ));
        }
    }

    let topic_region_values = if !topic_regions.is_empty() {
        let keys = topic_regions
            .iter()
            .flat_map(|(topic, regions)| {
                regions
                    .iter()
                    .map(|region_id| TopicRegionKey::new(*region_id, topic))
            })
            .collect::<Vec<_>>();
        let topic_region_manager = topic_region_manager
            .batch_get(keys)
            .await
            .context(GetMetadataSnafu)?;
        Some(topic_region_manager)
    } else {
        None
    };

    let topic_name_values = if !topic_regions.is_empty() {
        let topics = topic_regions
            .keys()
            .map(|topic| TopicNameKey::new(topic))
            .collect::<Vec<_>>();
        Some(
            topic_name_manager
                .batch_get(topics)
                .await
                .context(GetMetadataSnafu)?,
        )
    } else {
        None
    };
    let region_pruned_entry_ids = topic_name_values
        .as_ref()
        .map(|values| region_pruned_entry_ids(&topic_regions, values));

    let mut leader_region_requests = Vec::with_capacity(regions.len());
    for (region_id, engine, store_path, options) in regions {
        let table_dir = table_dir(&store_path, region_id.table_id());
        let pruned_entry_id = region_pruned_entry_ids
            .as_ref()
            .and_then(|values| values.get(&region_id).copied());
        let checkpoint = get_replay_checkpoint(
            region_id,
            &topic_region_values,
            pruned_entry_id,
            engine == METRIC_ENGINE_NAME,
        );
        info!("region_id: {}, checkpoint: {:?}", region_id, checkpoint);
        leader_region_requests.push((
            region_id,
            RegionOpenRequest {
                engine,
                table_dir,
                path_type: PathType::Bare,
                options,
                skip_wal_replay: false,
                checkpoint,
                requirements: Default::default(),
            },
        ));
    }

    #[cfg(feature = "enterprise")]
    let follower_region_requests = {
        let mut follower_region_requests = Vec::with_capacity(follower_regions.len());
        for (region_id, engine, store_path, options) in follower_regions {
            let table_dir = table_dir(&store_path, region_id.table_id());
            follower_region_requests.push((
                region_id,
                RegionOpenRequest {
                    engine,
                    table_dir,
                    path_type: PathType::Bare,
                    options,
                    skip_wal_replay: true,
                    checkpoint: None,
                    requirements: Default::default(),
                },
            ));
        }
        follower_region_requests
    };

    Ok(RegionOpenRequests {
        leader_regions: leader_region_requests,
        #[cfg(feature = "enterprise")]
        follower_regions: follower_region_requests,
    })
}
