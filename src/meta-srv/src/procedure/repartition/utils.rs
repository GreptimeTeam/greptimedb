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

use std::collections::{HashMap, HashSet};

use common_error::ext::BoxedError;
use common_meta::key::TableMetadataManagerRef;
use common_meta::key::datanode_table::{DatanodeTableKey, DatanodeTableValue};
use common_meta::rpc::router::RegionRoute;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::storage::{RegionId, RegionNumber, TableId};

use crate::error::{self, Result};

/// Returns the `datanode_table_value`
///
/// Retry:
/// - Failed to retrieve the metadata of datanode table.
pub async fn get_datanode_table_value(
    table_metadata_manager: &TableMetadataManagerRef,
    table_id: TableId,
    datanode_id: u64,
) -> Result<DatanodeTableValue> {
    let datanode_table_value = table_metadata_manager
        .datanode_table_manager()
        .get(&DatanodeTableKey {
            datanode_id,
            table_id,
        })
        .await
        .context(error::TableMetadataManagerSnafu)
        .map_err(BoxedError::new)
        .with_context(|_| error::RetryLaterWithSourceSnafu {
            reason: format!("Failed to get DatanodeTable: {table_id}"),
        })?
        .context(error::DatanodeTableNotFoundSnafu {
            table_id,
            datanode_id,
        })?;
    Ok(datanode_table_value)
}

/// Merges and validates region WAL options for repartition.
///
/// This function:
/// 1. Validates that new WAL options don't overwrite existing ones
/// 2. Merges existing `region_wal_options` with new `new_region_wal_options`
/// 3. Filters out WAL options for regions that are not in `new_region_routes`
/// 4. Validates that every region in `new_region_routes` has a corresponding WAL option
///
/// # Arguments
/// * `region_wal_options` - Existing region WAL options from datanode table
/// * `new_region_wal_options` - New region WAL options to merge (should only contain newly allocated regions)
/// * `new_region_routes` - The new region routes after repartition
/// * `table_id` - Table ID for error reporting
///
/// # Returns
/// Returns the merged and filtered WAL options, ensuring all regions have options.
///
/// # Errors
/// Returns an error if:
/// - New WAL options try to overwrite existing ones for the same region
/// - Any region in `new_region_routes` is missing a WAL option
pub fn merge_and_validate_region_wal_options(
    region_wal_options: &HashMap<RegionNumber, String>,
    mut new_region_wal_options: HashMap<RegionNumber, String>,
    new_region_routes: &[RegionRoute],
    table_id: TableId,
) -> Result<HashMap<RegionNumber, String>> {
    // Doesn't allow overwriting existing WAL options.
    for (region_number, _) in new_region_wal_options.iter() {
        if region_wal_options.contains_key(region_number) {
            return error::UnexpectedSnafu {
                violated: format!(
                    "Overwriting existing WAL option for region: {}",
                    RegionId::new(table_id, *region_number)
                ),
            }
            .fail();
        }
    }

    new_region_wal_options.extend(region_wal_options.clone());

    // Extract region numbers from new routes
    let region_numbers: HashSet<RegionNumber> = new_region_routes
        .iter()
        .map(|r| r.region.id.region_number())
        .collect();

    // Filter out WAL options for regions that are not in new_region_routes
    new_region_wal_options.retain(|k, _| region_numbers.contains(k));

    // Validate that every region has a WAL option
    ensure!(
        region_numbers.len() == new_region_wal_options.len(),
        error::UnexpectedSnafu {
            violated: format!(
                "Mismatch between number of region_numbers ({}) and new_region_wal_options ({}) for table: {}",
                region_numbers.len(),
                new_region_wal_options.len(),
                table_id
            ),
        }
    );

    Ok(new_region_wal_options)
}

#[cfg(test)]
mod tests {
    use common_meta::peer::Peer;
    use common_meta::rpc::router::{Region, RegionRoute};
    use common_wal::options::{KafkaWalOptions, WalOptions};
    use store_api::storage::RegionId;

    use super::*;

    /// Helper function to create a Kafka WAL option string from a topic name.
    fn kafka_wal_option(topic: &str) -> String {
        serde_json::to_string(&WalOptions::Kafka(KafkaWalOptions {
            topic: topic.to_string(),
        }))
        .unwrap()
    }

    fn new_region_route(region_id: u64, datanode_id: u64) -> RegionRoute {
        RegionRoute {
            region: Region {
                id: RegionId::from_u64(region_id),
                ..Default::default()
            },
            leader_peer: Some(Peer::empty(datanode_id)),
            follower_peers: vec![],
            leader_state: None,
            leader_down_since: None,
            write_route_policy: None,
        }
    }

    #[test]
    fn test_merge_and_validate_region_wal_options_success() {
        let table_id = 1;
        let existing_wal_options: HashMap<RegionNumber, String> = vec![
            (1, kafka_wal_option("topic_1")),
            (2, kafka_wal_option("topic_2")),
        ]
        .into_iter()
        .collect();
        let new_wal_options: HashMap<RegionNumber, String> =
            vec![(3, kafka_wal_option("topic_3"))].into_iter().collect();
        let new_region_routes = vec![
            new_region_route(1, 1),
            new_region_route(2, 2),
            new_region_route(3, 3),
        ];
        let result = merge_and_validate_region_wal_options(
            &existing_wal_options,
            new_wal_options,
            &new_region_routes,
            table_id,
        )
        .unwrap();

        // Should have all three regions
        assert_eq!(result.len(), 3);
        assert!(result.contains_key(&1));
        assert!(result.contains_key(&2));
        assert!(result.contains_key(&3));
        // Existing options should be preserved
        assert_eq!(result.get(&1).unwrap(), &kafka_wal_option("topic_1"));
        assert_eq!(result.get(&2).unwrap(), &kafka_wal_option("topic_2"));
        // New option should be present
        assert_eq!(result.get(&3).unwrap(), &kafka_wal_option("topic_3"));
    }

    #[test]
    fn test_merge_and_validate_region_wal_options_new_overrides_existing() {
        let table_id = 1;
        let existing_wal_options: HashMap<RegionNumber, String> =
            vec![(1, kafka_wal_option("topic_1_old"))]
                .into_iter()
                .collect();
        let new_wal_options: HashMap<RegionNumber, String> =
            vec![(1, kafka_wal_option("topic_1_new"))]
                .into_iter()
                .collect();
        let new_region_routes = vec![new_region_route(1, 1)];
        merge_and_validate_region_wal_options(
            &existing_wal_options,
            new_wal_options,
            &new_region_routes,
            table_id,
        )
        .unwrap_err();
    }

    #[test]
    fn test_merge_and_validate_region_wal_options_filters_removed_regions() {
        let table_id = 1;
        let existing_wal_options: HashMap<RegionNumber, String> = vec![
            (1, kafka_wal_option("topic_1")),
            (2, kafka_wal_option("topic_2")),
            (3, kafka_wal_option("topic_3")),
        ]
        .into_iter()
        .collect();
        let new_wal_options = HashMap::new();
        // Only regions 1 and 2 are in new routes (region 3 removed)
        let new_region_routes = vec![new_region_route(1, 1), new_region_route(2, 2)];
        let result = merge_and_validate_region_wal_options(
            &existing_wal_options,
            new_wal_options,
            &new_region_routes,
            table_id,
        )
        .unwrap();

        // Should only have regions 1 and 2 (region 3 filtered out)
        assert_eq!(result.len(), 2);
        assert!(result.contains_key(&1));
        assert!(result.contains_key(&2));
        assert!(!result.contains_key(&3));
    }

    #[test]
    fn test_merge_and_validate_region_wal_options_missing_option() {
        let table_id = 1;
        let existing_wal_options: HashMap<RegionNumber, String> =
            vec![(1, kafka_wal_option("topic_1"))].into_iter().collect();
        let new_wal_options = HashMap::new();
        // Region 2 is in routes but has no WAL option
        let new_region_routes = vec![new_region_route(1, 1), new_region_route(2, 2)];
        let result = merge_and_validate_region_wal_options(
            &existing_wal_options,
            new_wal_options,
            &new_region_routes,
            table_id,
        );
        // Should fail validation
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Mismatch"));
        assert!(error_msg.contains(&table_id.to_string()));
    }
}
