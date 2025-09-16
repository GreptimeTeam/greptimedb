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

use std::time::Instant;

use common_telemetry::info;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::region_engine::{RegionEngine, RegionManifestInfo, SyncManifestResponse};
use store_api::storage::RegionId;

use crate::engine::MetricEngineInner;
use crate::error::{
    MetricManifestInfoSnafu, MitoSyncOperationSnafu, PhysicalRegionNotFoundSnafu, Result,
};
use crate::utils;

impl MetricEngineInner {
    pub async fn sync_region(
        &self,
        region_id: RegionId,
        manifest_info: RegionManifestInfo,
    ) -> Result<SyncManifestResponse> {
        ensure!(
            manifest_info.is_metric(),
            MetricManifestInfoSnafu { region_id }
        );

        let metadata_region_id = utils::to_metadata_region_id(region_id);
        // checked by ensure above
        let metadata_manifest_version = manifest_info
            .metadata_manifest_version()
            .unwrap_or_default();
        let metadata_flushed_entry_id = manifest_info
            .metadata_flushed_entry_id()
            .unwrap_or_default();
        let metadata_region_manifest =
            RegionManifestInfo::mito(metadata_manifest_version, metadata_flushed_entry_id, 0);
        let metadata_synced = self
            .mito
            .sync_region(metadata_region_id, metadata_region_manifest)
            .await
            .context(MitoSyncOperationSnafu)?
            .is_data_synced();

        let data_region_id = utils::to_data_region_id(region_id);
        let data_manifest_version = manifest_info.data_manifest_version();
        let data_flushed_entry_id = manifest_info.data_flushed_entry_id();
        let data_region_manifest =
            RegionManifestInfo::mito(data_manifest_version, data_flushed_entry_id, 0);

        let data_synced = self
            .mito
            .sync_region(data_region_id, data_region_manifest)
            .await
            .context(MitoSyncOperationSnafu)?
            .is_data_synced();

        if !metadata_synced {
            return Ok(SyncManifestResponse::Metric {
                metadata_synced,
                data_synced,
                new_opened_logical_region_ids: vec![],
            });
        }

        let now = Instant::now();
        // Recovers the states from the metadata region
        // if the metadata manifest version is updated.
        let physical_region_options = *self
            .state
            .read()
            .unwrap()
            .physical_region_states()
            .get(&data_region_id)
            .context(PhysicalRegionNotFoundSnafu {
                region_id: data_region_id,
            })?
            .options();
        let new_opened_logical_region_ids = self
            .recover_states(data_region_id, physical_region_options)
            .await?;
        info!(
            "Sync metadata region for physical region {}, cost: {:?}, new opened logical region ids: {:?}",
            data_region_id,
            now.elapsed(),
            new_opened_logical_region_ids
        );

        Ok(SyncManifestResponse::Metric {
            metadata_synced,
            data_synced,
            new_opened_logical_region_ids,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use api::v1::SemanticType;
    use common_query::prelude::greptime_timestamp;
    use common_telemetry::info;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use store_api::metadata::ColumnMetadata;
    use store_api::region_engine::{RegionEngine, RegionManifestInfo};
    use store_api::region_request::{
        AddColumn, AlterKind, RegionAlterRequest, RegionFlushRequest, RegionRequest,
    };
    use store_api::storage::RegionId;

    use crate::metadata_region::MetadataRegion;
    use crate::test_util::TestEnv;

    #[tokio::test]
    async fn test_sync_region_with_new_created_logical_regions() {
        common_telemetry::init_default_ut_logging();
        let mut env = TestEnv::with_prefix("sync_with_new_created_logical_regions").await;
        env.init_metric_region().await;

        info!("creating follower engine");
        // Create a follower engine.
        let (_follower_mito, follower_metric) = env.create_follower_engine().await;

        let physical_region_id = env.default_physical_region_id();

        // Flushes the physical region
        let metric_engine = env.metric();
        metric_engine
            .handle_request(
                env.default_physical_region_id(),
                RegionRequest::Flush(RegionFlushRequest::default()),
            )
            .await
            .unwrap();

        let response = follower_metric
            .sync_region(physical_region_id, RegionManifestInfo::metric(1, 0, 1, 0))
            .await
            .unwrap();
        assert!(response.is_metric());
        let new_opened_logical_region_ids = response.new_opened_logical_region_ids().unwrap();
        assert_eq!(new_opened_logical_region_ids, vec![RegionId::new(3, 2)]);

        // Sync again, no new logical region should be opened
        let response = follower_metric
            .sync_region(physical_region_id, RegionManifestInfo::metric(1, 0, 1, 0))
            .await
            .unwrap();
        assert!(response.is_metric());
        let new_opened_logical_region_ids = response.new_opened_logical_region_ids().unwrap();
        assert!(new_opened_logical_region_ids.is_empty());
    }

    fn test_alter_logical_region_request() -> RegionAlterRequest {
        RegionAlterRequest {
            kind: AlterKind::AddColumns {
                columns: vec![AddColumn {
                    column_metadata: ColumnMetadata {
                        column_id: 0,
                        semantic_type: SemanticType::Tag,
                        column_schema: ColumnSchema::new(
                            "tag1",
                            ConcreteDataType::string_datatype(),
                            false,
                        ),
                    },
                    location: None,
                }],
            },
        }
    }

    #[tokio::test]
    async fn test_sync_region_alter_alter_logical_region() {
        common_telemetry::init_default_ut_logging();
        let mut env = TestEnv::with_prefix("sync_region_alter_alter_logical_region").await;
        env.init_metric_region().await;

        info!("creating follower engine");
        let physical_region_id = env.default_physical_region_id();
        // Flushes the physical region
        let metric_engine = env.metric();
        metric_engine
            .handle_request(
                env.default_physical_region_id(),
                RegionRequest::Flush(RegionFlushRequest::default()),
            )
            .await
            .unwrap();

        // Create a follower engine.
        let (follower_mito, follower_metric) = env.create_follower_engine().await;
        let metric_engine = env.metric();
        let engine_inner = env.metric().inner;
        let region_id = env.default_logical_region_id();
        let request = test_alter_logical_region_request();

        engine_inner
            .alter_logical_regions(
                physical_region_id,
                vec![(region_id, request)],
                &mut HashMap::new(),
            )
            .await
            .unwrap();

        // Flushes the physical region
        metric_engine
            .handle_request(
                env.default_physical_region_id(),
                RegionRequest::Flush(RegionFlushRequest::default()),
            )
            .await
            .unwrap();

        // Sync the follower engine
        let response = follower_metric
            .sync_region(physical_region_id, RegionManifestInfo::metric(2, 0, 2, 0))
            .await
            .unwrap();
        assert!(response.is_metric());
        let new_opened_logical_region_ids = response.new_opened_logical_region_ids().unwrap();
        assert!(new_opened_logical_region_ids.is_empty());

        let logical_region_id = env.default_logical_region_id();
        let metadata_region = MetadataRegion::new(follower_mito.clone());
        let semantic_type = metadata_region
            .column_semantic_type(physical_region_id, logical_region_id, "tag1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(semantic_type, SemanticType::Tag);
        let timestamp_index = metadata_region
            .column_semantic_type(physical_region_id, logical_region_id, greptime_timestamp())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(timestamp_index, SemanticType::Timestamp);
    }
}
