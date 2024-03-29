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

use store_api::metric_engine_consts::{METRIC_DATA_REGION_GROUP, METRIC_METADATA_REGION_GROUP};
use store_api::storage::RegionId;

/// Change the given [RegionId]'s region group to [METRIC_METADATA_REGION_GROUP].
pub fn to_metadata_region_id(region_id: RegionId) -> RegionId {
    let table_id = region_id.table_id();
    let region_sequence = region_id.region_sequence();
    RegionId::with_group_and_seq(table_id, METRIC_METADATA_REGION_GROUP, region_sequence)
}

/// Change the given [RegionId]'s region group to [METRIC_DATA_REGION_GROUP].
pub fn to_data_region_id(region_id: RegionId) -> RegionId {
    let table_id = region_id.table_id();
    let region_sequence = region_id.region_sequence();
    RegionId::with_group_and_seq(table_id, METRIC_DATA_REGION_GROUP, region_sequence)
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_to_metadata_region_id() {
        let region_id = RegionId::new(1, 2);
        let expected_region_id = RegionId::with_group_and_seq(1, METRIC_METADATA_REGION_GROUP, 2);
        assert_eq!(to_metadata_region_id(region_id), expected_region_id);

        let region_id = RegionId::with_group_and_seq(1, 243, 2);
        let expected_region_id = RegionId::with_group_and_seq(1, METRIC_METADATA_REGION_GROUP, 2);
        assert_eq!(to_metadata_region_id(region_id), expected_region_id);
    }

    #[test]
    fn test_to_data_region_id() {
        let region_id = RegionId::new(1, 2);
        let expected_region_id = RegionId::with_group_and_seq(1, METRIC_DATA_REGION_GROUP, 2);
        assert_eq!(to_data_region_id(region_id), expected_region_id);

        let region_id = RegionId::with_group_and_seq(1, 243, 2);
        let expected_region_id = RegionId::with_group_and_seq(1, METRIC_DATA_REGION_GROUP, 2);
        assert_eq!(to_data_region_id(region_id), expected_region_id);
    }
}
