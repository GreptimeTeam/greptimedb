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

use snafu::ResultExt;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{AffectedRows, RegionFlushRequest, RegionRequest};
use store_api::storage::RegionId;

use crate::engine::MetricEngineInner;
use crate::error::{MitoFlushOperationSnafu, Result, UnsupportedRegionRequestSnafu};
use crate::utils;

impl MetricEngineInner {
    pub async fn flush_region(
        &self,
        region_id: RegionId,
        req: RegionFlushRequest,
    ) -> Result<AffectedRows> {
        if !self.is_physical_region(region_id) {
            return UnsupportedRegionRequestSnafu {
                request: RegionRequest::Flush(req),
            }
            .fail();
        }

        let metadata_region_id = utils::to_metadata_region_id(region_id);
        // Flushes the metadata region as well
        self.mito
            .handle_request(metadata_region_id, RegionRequest::Flush(req.clone()))
            .await
            .context(MitoFlushOperationSnafu)
            .map(|response| response.affected_rows)?;

        let data_region_id = utils::to_data_region_id(region_id);
        self.mito
            .handle_request(data_region_id, RegionRequest::Flush(req.clone()))
            .await
            .context(MitoFlushOperationSnafu)
            .map(|response| response.affected_rows)
    }
}

#[cfg(test)]
mod tests {
    use api::v1::Rows;
    use futures_util::TryStreamExt;
    use itertools::Itertools as _;
    use store_api::region_request::RegionPutRequest;

    use super::*;
    use crate::test_util::{TestEnv, build_rows, row_schema_with_tags};

    #[tokio::test]
    async fn test_list_ssts_after_write_and_flush_metric() {
        let env = TestEnv::new().await;
        let engine = env.metric();

        let phy_to_logi = vec![
            (
                RegionId::new(11, 1),
                vec![RegionId::new(1111, 11), RegionId::new(1111, 12)],
            ),
            (RegionId::new(11, 2), vec![RegionId::new(1111, 2)]),
            (RegionId::new(22, 42), vec![RegionId::new(2222, 42)]),
        ];

        for (phy_region_id, logi_region_ids) in &phy_to_logi {
            env.create_physical_region(*phy_region_id, &TestEnv::default_table_dir(), vec![])
                .await;
            for logi_region_id in logi_region_ids {
                env.create_logical_region(*phy_region_id, *logi_region_id)
                    .await;
            }
        }

        // write to logical regions
        for (_, logi_region_ids) in &phy_to_logi {
            for logi_region_id in logi_region_ids {
                let schema = row_schema_with_tags(&["job"]);
                let rows = build_rows(1, 10);
                let request = RegionRequest::Put(RegionPutRequest {
                    rows: Rows { schema, rows },
                    hint: None,
                    partition_expr_version: None,
                });
                engine
                    .handle_request(*logi_region_id, request)
                    .await
                    .unwrap();
            }
        }

        for (phy_region_id, _) in &phy_to_logi {
            engine
                .handle_request(*phy_region_id, RegionRequest::Flush(Default::default()))
                .await
                .unwrap();
        }

        // list from manifest
        let mito = env.mito();
        let debug_format = mito
            .all_ssts_from_manifest()
            .await
            .into_iter()
            .map(|mut e| {
                e.file_path = e.file_path.replace(&e.file_id, "<file_id>");
                e.index_file_path = e
                    .index_file_path
                    .map(|path| path.replace(&e.file_id, "<file_id>"));
                e.file_id = "<file_id>".to_string();
                e.index_version = 0;
                format!("\n{:?}", e)
            })
            .sorted()
            .collect::<Vec<_>>()
            .join("");
        assert_eq!(
            debug_format,
            r#"
ManifestSstEntry { table_dir: "test_metric_region/", region_id: 47244640257(11, 1), table_id: 11, region_number: 1, region_group: 0, region_sequence: 1, file_id: "<file_id>", index_version: 0, level: 0, file_path: "test_metric_region/11_0000000001/data/<file_id>.parquet", file_size: 3217, index_file_path: Some("test_metric_region/11_0000000001/data/index/<file_id>.puffin"), index_file_size: Some(235), num_rows: 10, num_row_groups: 1, num_series: Some(1), min_ts: 0::Millisecond, max_ts: 9::Millisecond, sequence: Some(20), origin_region_id: 47244640257(11, 1), node_id: None, visible: true }
ManifestSstEntry { table_dir: "test_metric_region/", region_id: 47244640258(11, 2), table_id: 11, region_number: 2, region_group: 0, region_sequence: 2, file_id: "<file_id>", index_version: 0, level: 0, file_path: "test_metric_region/11_0000000002/data/<file_id>.parquet", file_size: 3217, index_file_path: Some("test_metric_region/11_0000000002/data/index/<file_id>.puffin"), index_file_size: Some(235), num_rows: 10, num_row_groups: 1, num_series: Some(1), min_ts: 0::Millisecond, max_ts: 9::Millisecond, sequence: Some(10), origin_region_id: 47244640258(11, 2), node_id: None, visible: true }
ManifestSstEntry { table_dir: "test_metric_region/", region_id: 47261417473(11, 16777217), table_id: 11, region_number: 16777217, region_group: 1, region_sequence: 1, file_id: "<file_id>", index_version: 0, level: 0, file_path: "test_metric_region/11_0000000001/metadata/<file_id>.parquet", file_size: 3487, index_file_path: None, index_file_size: None, num_rows: 8, num_row_groups: 1, num_series: Some(8), min_ts: 0::Millisecond, max_ts: 0::Millisecond, sequence: Some(8), origin_region_id: 47261417473(11, 16777217), node_id: None, visible: true }
ManifestSstEntry { table_dir: "test_metric_region/", region_id: 47261417474(11, 16777218), table_id: 11, region_number: 16777218, region_group: 1, region_sequence: 2, file_id: "<file_id>", index_version: 0, level: 0, file_path: "test_metric_region/11_0000000002/metadata/<file_id>.parquet", file_size: 3471, index_file_path: None, index_file_size: None, num_rows: 4, num_row_groups: 1, num_series: Some(4), min_ts: 0::Millisecond, max_ts: 0::Millisecond, sequence: Some(4), origin_region_id: 47261417474(11, 16777218), node_id: None, visible: true }
ManifestSstEntry { table_dir: "test_metric_region/", region_id: 94489280554(22, 42), table_id: 22, region_number: 42, region_group: 0, region_sequence: 42, file_id: "<file_id>", index_version: 0, level: 0, file_path: "test_metric_region/22_0000000042/data/<file_id>.parquet", file_size: 3217, index_file_path: Some("test_metric_region/22_0000000042/data/index/<file_id>.puffin"), index_file_size: Some(235), num_rows: 10, num_row_groups: 1, num_series: Some(1), min_ts: 0::Millisecond, max_ts: 9::Millisecond, sequence: Some(10), origin_region_id: 94489280554(22, 42), node_id: None, visible: true }
ManifestSstEntry { table_dir: "test_metric_region/", region_id: 94506057770(22, 16777258), table_id: 22, region_number: 16777258, region_group: 1, region_sequence: 42, file_id: "<file_id>", index_version: 0, level: 0, file_path: "test_metric_region/22_0000000042/metadata/<file_id>.parquet", file_size: 3471, index_file_path: None, index_file_size: None, num_rows: 4, num_row_groups: 1, num_series: Some(4), min_ts: 0::Millisecond, max_ts: 0::Millisecond, sequence: Some(4), origin_region_id: 94506057770(22, 16777258), node_id: None, visible: true }"#,
        );
        // list from storage
        let storage_entries = mito
            .all_ssts_from_storage()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let debug_format = storage_entries
            .into_iter()
            .map(|mut e| {
                let i = e.file_path.rfind('/').unwrap();
                e.file_path.replace_range(i..(i + 37), "/<file_id>");
                format!("\n{:?}", e)
            })
            .sorted()
            .collect::<Vec<_>>()
            .join("");
        assert_eq!(
            debug_format,
            r#"
StorageSstEntry { file_path: "test_metric_region/11_0000000001/data/<file_id>.parquet", file_size: None, last_modified_ms: None, node_id: None }
StorageSstEntry { file_path: "test_metric_region/11_0000000001/data/index/<file_id>.puffin", file_size: None, last_modified_ms: None, node_id: None }
StorageSstEntry { file_path: "test_metric_region/11_0000000001/metadata/<file_id>.parquet", file_size: None, last_modified_ms: None, node_id: None }
StorageSstEntry { file_path: "test_metric_region/11_0000000002/data/<file_id>.parquet", file_size: None, last_modified_ms: None, node_id: None }
StorageSstEntry { file_path: "test_metric_region/11_0000000002/data/index/<file_id>.puffin", file_size: None, last_modified_ms: None, node_id: None }
StorageSstEntry { file_path: "test_metric_region/11_0000000002/metadata/<file_id>.parquet", file_size: None, last_modified_ms: None, node_id: None }
StorageSstEntry { file_path: "test_metric_region/22_0000000042/data/<file_id>.parquet", file_size: None, last_modified_ms: None, node_id: None }
StorageSstEntry { file_path: "test_metric_region/22_0000000042/data/index/<file_id>.puffin", file_size: None, last_modified_ms: None, node_id: None }
StorageSstEntry { file_path: "test_metric_region/22_0000000042/metadata/<file_id>.parquet", file_size: None, last_modified_ms: None, node_id: None }"#
        );
    }
}
