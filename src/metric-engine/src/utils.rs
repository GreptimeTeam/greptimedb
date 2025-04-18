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

use common_telemetry::{info, warn};
use mito2::engine::MitoEngine;
use snafu::ResultExt;
use store_api::metric_engine_consts::{
    MANIFEST_INFO_EXTENSION_KEY, METRIC_DATA_REGION_GROUP, METRIC_METADATA_REGION_GROUP,
};
use store_api::region_engine::{RegionEngine, RegionManifestInfo, RegionStatistic};
use store_api::storage::RegionId;

use crate::error::{Result, SerializeRegionManifestInfoSnafu};

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

/// Get the region statistic of the given [RegionId].
pub fn get_region_statistic(mito: &MitoEngine, region_id: RegionId) -> Option<RegionStatistic> {
    let metadata_region_id = to_metadata_region_id(region_id);
    let data_region_id = to_data_region_id(region_id);

    let metadata_stat = mito.region_statistic(metadata_region_id);
    let data_stat = mito.region_statistic(data_region_id);

    match (&metadata_stat, &data_stat) {
        (Some(metadata_stat), Some(data_stat)) => Some(RegionStatistic {
            num_rows: metadata_stat.num_rows + data_stat.num_rows,
            memtable_size: metadata_stat.memtable_size + data_stat.memtable_size,
            wal_size: metadata_stat.wal_size + data_stat.wal_size,
            manifest_size: metadata_stat.manifest_size + data_stat.manifest_size,
            sst_size: metadata_stat.sst_size + data_stat.sst_size,
            index_size: metadata_stat.index_size + data_stat.index_size,
            manifest: RegionManifestInfo::Metric {
                data_flushed_entry_id: data_stat.manifest.data_flushed_entry_id(),
                data_manifest_version: data_stat.manifest.data_manifest_version(),
                metadata_flushed_entry_id: metadata_stat.manifest.data_flushed_entry_id(),
                metadata_manifest_version: metadata_stat.manifest.data_manifest_version(),
            },
            data_topic_latest_entry_id: data_stat.data_topic_latest_entry_id,
            metadata_topic_latest_entry_id: metadata_stat.metadata_topic_latest_entry_id,
        }),
        _ => {
            warn!(
                "Failed to get region statistic for region {}, metadata_stat: {:?}, data_stat: {:?}",
                region_id, metadata_stat, data_stat
            );
            None
        }
    }
}

/// Appends the given [RegionId]'s manifest info to the given list.
pub(crate) fn append_manifest_info(
    mito: &MitoEngine,
    region_id: RegionId,
    manifest_infos: &mut Vec<(RegionId, RegionManifestInfo)>,
) {
    if let Some(statistic) = get_region_statistic(mito, region_id) {
        manifest_infos.push((region_id, statistic.manifest));
    }
}

/// Encodes the given list of ([RegionId], [RegionManifestInfo]) to extensions(key: MANIFEST_INFO_EXTENSION_KEY).
pub(crate) fn encode_manifest_info_to_extensions(
    manifest_infos: &[(RegionId, RegionManifestInfo)],
    extensions: &mut HashMap<String, Vec<u8>>,
) -> Result<()> {
    extensions.insert(
        MANIFEST_INFO_EXTENSION_KEY.to_string(),
        RegionManifestInfo::encode_list(manifest_infos)
            .context(SerializeRegionManifestInfoSnafu)?,
    );
    for (region_id, manifest_info) in manifest_infos {
        info!(
            "Added manifest info: {:?} to extensions, region_id: {:?}",
            manifest_info, region_id
        );
    }
    Ok(())
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
