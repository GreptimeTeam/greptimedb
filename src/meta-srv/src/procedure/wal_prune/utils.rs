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
use std::sync::Arc;
use std::time::Duration;

use common_meta::key::TableMetadataManagerRef;
use common_meta::region_registry::LeaderRegionRegistryRef;
use common_telemetry::warn;
use itertools::{Itertools, MinMaxResult};
use rskafka::client::partition::{OffsetAt, PartitionClient, UnknownTopicHandling};
use rskafka::client::Client;
use snafu::ResultExt;
use store_api::storage::RegionId;

use crate::error::{
    BuildPartitionClientSnafu, DeleteRecordsSnafu, GetOffsetSnafu, Result,
    TableMetadataManagerSnafu, UpdateTopicNameValueSnafu,
};

/// The default timeout for deleting records.
const DELETE_RECORDS_TIMEOUT: Duration = Duration::from_secs(5);
/// The default partition.
const DEFAULT_PARTITION: i32 = 0;

fn missing_region_ids(
    all_region_ids: &[RegionId],
    result_set: &HashMap<RegionId, u64>,
) -> Vec<RegionId> {
    let mut missing_region_ids = Vec::new();
    for region_id in all_region_ids {
        if !result_set.contains_key(region_id) {
            missing_region_ids.push(*region_id);
        }
    }
    missing_region_ids
}

/// Finds the prunable entry id for the topic.
///
/// Returns `None` if:
/// - The topic has no region.
/// - Some region info is missing from heartbeat.
pub(crate) async fn find_pruneable_entry_id_for_topic(
    table_metadata_manager: &TableMetadataManagerRef,
    leader_region_registry: &LeaderRegionRegistryRef,
    topic: &str,
) -> Result<Option<u64>> {
    let region_ids = table_metadata_manager
        .topic_region_manager()
        .regions(topic)
        .await
        .context(TableMetadataManagerSnafu)?
        .into_keys()
        .collect::<Vec<_>>();
    if region_ids.is_empty() {
        return Ok(None);
    }

    // Get the prunable entry id for each region.
    let prunable_entry_ids_map = leader_region_registry
        .batch_get(region_ids.iter().cloned())
        .into_iter()
        .map(|(region_id, region)| {
            let prunable_entry_id = region.manifest.prunable_entry_id();
            (region_id, prunable_entry_id)
        })
        .collect();
    let missing_region_ids = missing_region_ids(&region_ids, &prunable_entry_ids_map);
    if !missing_region_ids.is_empty() {
        warn!(
            "Cannot determine prunable entry id: missing region info from heartbeat. Topic: {}, missing region ids: {:?}",
            topic, missing_region_ids
        );
        return Ok(None);
    }

    let min_max_result = prunable_entry_ids_map.values().minmax();
    match min_max_result {
        MinMaxResult::NoElements => Ok(None),
        MinMaxResult::OneElement(prunable_entry_id) => Ok(Some(*prunable_entry_id)),
        MinMaxResult::MinMax(min_prunable_entry_id, _) => Ok(Some(*min_prunable_entry_id)),
    }
}

/// Determines whether pruning should be triggered based on the current pruned entry id and the prunable entry id.
/// Returns true if:
/// - There is no current pruned entry id (i.e., pruning has never occurred).
/// - The current pruned entry id is greater than the prunable entry id (i.e., there is something to prune).
pub(crate) fn should_trigger_prune(current: Option<u64>, prunable_entry_id: u64) -> bool {
    match current {
        None => true, // No pruning has occurred yet, should trigger immediately.
        Some(current) => prunable_entry_id > current,
    }
}

/// Returns a partition client for the given topic.
pub(crate) async fn get_partition_client(
    client: &Arc<Client>,
    topic: &str,
) -> Result<PartitionClient> {
    client
        .partition_client(topic, DEFAULT_PARTITION, UnknownTopicHandling::Retry)
        .await
        .context(BuildPartitionClientSnafu {
            topic,
            partition: DEFAULT_PARTITION,
        })
}

/// Returns the earliest and latest offsets for the given topic.
pub(crate) async fn get_offsets_for_topic(
    partition_client: &PartitionClient,
    topic: &str,
) -> Result<(u64, u64)> {
    let earliest_offset = partition_client
        .get_offset(OffsetAt::Earliest)
        .await
        .context(GetOffsetSnafu { topic })?;
    let latest_offset = partition_client
        .get_offset(OffsetAt::Latest)
        .await
        .context(GetOffsetSnafu { topic })?;

    Ok((earliest_offset as u64, latest_offset as u64))
}

/// Updates the pruned entry id for the given topic.
pub(crate) async fn update_pruned_entry_id(
    table_metadata_manager: &TableMetadataManagerRef,
    topic: &str,
    pruned_entry_id: u64,
) -> Result<()> {
    let prev = table_metadata_manager
        .topic_name_manager()
        .get(topic)
        .await
        .context(TableMetadataManagerSnafu)?;

    table_metadata_manager
        .topic_name_manager()
        .update(topic, pruned_entry_id, prev)
        .await
        .context(UpdateTopicNameValueSnafu { topic })?;

    Ok(())
}

/// Deletes the records for the given topic.
pub(crate) async fn delete_records(
    partition_client: &PartitionClient,
    topic: &str,
    pruned_entry_id: u64,
) -> Result<()> {
    partition_client
        .delete_records(
            // Note: here no "+1" is needed because the offset arg is exclusive,
            // and it's defensive programming just in case somewhere else have a off by one error,
            // see https://kafka.apache.org/36/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#endOffsets(java.util.Collection)
            // which we use to get the end offset from high watermark
            pruned_entry_id as i64,
            DELETE_RECORDS_TIMEOUT.as_millis() as i32,
        )
        .await
        .context(DeleteRecordsSnafu {
            topic,
            partition: DEFAULT_PARTITION,
            offset: pruned_entry_id,
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_trigger_prune_none_current() {
        // No pruning has occurred yet, should trigger
        assert!(should_trigger_prune(None, 10));
        assert!(should_trigger_prune(None, 0));
    }

    #[test]
    fn test_should_trigger_prune_prunable_greater_than_current() {
        // Prunable entry id is greater than current, should trigger
        assert!(should_trigger_prune(Some(5), 6));
        assert!(should_trigger_prune(Some(0), 1));
        assert!(should_trigger_prune(Some(99), 100));
    }

    #[test]
    fn test_should_not_trigger_prune_prunable_equal_to_current() {
        // Prunable entry id is equal to current, should not trigger
        assert!(!should_trigger_prune(Some(10), 10));
        assert!(!should_trigger_prune(Some(0), 0));
    }

    #[test]
    fn test_should_not_trigger_prune_prunable_less_than_current() {
        // Prunable entry id is less than current, should not trigger
        assert!(!should_trigger_prune(Some(10), 9));
        assert!(!should_trigger_prune(Some(100), 99));
    }

    #[test]
    fn test_missing_region_ids_none_missing() {
        let all_region_ids = vec![RegionId::new(1, 1), RegionId::new(2, 2)];
        let mut result_set = HashMap::new();
        result_set.insert(RegionId::new(1, 1), 10);
        result_set.insert(RegionId::new(2, 2), 20);
        let missing = missing_region_ids(&all_region_ids, &result_set);
        assert!(missing.is_empty());
    }

    #[test]
    fn test_missing_region_ids_some_missing() {
        let all_region_ids = vec![
            RegionId::new(1, 1),
            RegionId::new(2, 2),
            RegionId::new(3, 3),
        ];
        let mut result_set = HashMap::new();
        result_set.insert(RegionId::new(1, 1), 10);
        let missing = missing_region_ids(&all_region_ids, &result_set);
        assert_eq!(missing, vec![RegionId::new(2, 2), RegionId::new(3, 3)]);
    }

    #[test]
    fn test_missing_region_ids_all_missing() {
        let all_region_ids = vec![RegionId::new(1, 1), RegionId::new(2, 2)];
        let result_set = HashMap::new();
        let missing = missing_region_ids(&all_region_ids, &result_set);
        assert_eq!(missing, all_region_ids);
    }

    #[test]
    fn test_missing_region_ids_empty_all() {
        let all_region_ids: Vec<RegionId> = vec![];
        let mut result_set = HashMap::new();
        result_set.insert(RegionId::new(1, 1), 10);
        let missing = missing_region_ids(&all_region_ids, &result_set);
        assert!(missing.is_empty());
    }
}
