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

use common_error::ext::BoxedError;
use common_telemetry::info;
use mito2::manifest::action::RegionEdit;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::region_engine::{MitoCopyRegionFromRequest, SyncRegionFromResponse};
use store_api::storage::RegionId;

use crate::engine::MetricEngineInner;
use crate::error::{
    MissingFilesSnafu, MitoCopyRegionFromOperationSnafu, MitoEditRegionSnafu,
    PhysicalRegionNotFoundSnafu, Result,
};
use crate::utils;

impl MetricEngineInner {
    /// Syncs the logical regions from the source region to the target region in the metric engine.
    ///
    /// This operation:
    /// 1. Copies SST files from source metadata region to target metadata region
    /// 2. Transforms logical region metadata (updates region numbers to match target)
    /// 3. Edits target manifest to remove old file entries (copied files)
    /// 4. Recovers states and returns newly opened logical region IDs
    ///
    /// **Note**: Only the metadata region is synced. The data region is not affected.
    pub(crate) async fn sync_region_from_region(
        &self,
        region_id: RegionId,
        source_region_id: RegionId,
        parallelism: usize,
    ) -> Result<SyncRegionFromResponse> {
        let source_metadata_region_id = utils::to_metadata_region_id(source_region_id);
        let target_metadata_region_id = utils::to_metadata_region_id(region_id);
        let target_data_region_id = utils::to_data_region_id(region_id);
        let source_data_region_id = utils::to_data_region_id(source_region_id);
        info!(
            "Syncing region from region {} to region {}",
            source_region_id, region_id
        );

        let res = self
            .mito
            .copy_region_from(
                target_metadata_region_id,
                MitoCopyRegionFromRequest {
                    source_region_id: source_metadata_region_id,
                    parallelism,
                },
            )
            .await
            .map_err(BoxedError::new)
            .context(MitoCopyRegionFromOperationSnafu {
                source_region_id: source_metadata_region_id,
                target_region_id: target_metadata_region_id,
            })?;

        if res.copied_file_ids.is_empty() {
            info!(
                "No files were copied from source region {} to target region {}, copied file ids are empty",
                source_metadata_region_id, target_metadata_region_id
            );
            return Ok(SyncRegionFromResponse::Metric {
                metadata_synced: false,
                data_synced: false,
                new_opened_logical_region_ids: vec![],
            });
        }

        let target_region = self.mito.find_region(target_metadata_region_id).context(
            PhysicalRegionNotFoundSnafu {
                region_id: target_metadata_region_id,
            },
        )?;
        let files_to_remove = target_region.file_metas(&res.copied_file_ids).await;
        let missing_file_ids = res
            .copied_file_ids
            .iter()
            .zip(&files_to_remove)
            .filter_map(|(file_id, maybe_meta)| {
                if maybe_meta.is_none() {
                    Some(*file_id)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        ensure!(
            missing_file_ids.is_empty(),
            MissingFilesSnafu {
                region_id: target_metadata_region_id,
                file_ids: missing_file_ids,
            }
        );
        let files_to_remove = files_to_remove.into_iter().flatten().collect::<Vec<_>>();
        // Transform the logical region metadata of the target data region.
        self.metadata_region
            .transform_logical_region_metadata(target_data_region_id, source_data_region_id)
            .await?;

        let edit = RegionEdit {
            files_to_add: vec![],
            files_to_remove: files_to_remove.clone(),
            timestamp_ms: Some(chrono::Utc::now().timestamp_millis()),
            compaction_time_window: None,
            flushed_entry_id: None,
            flushed_sequence: None,
            committed_sequence: None,
        };
        self.mito
            .edit_region(target_metadata_region_id, edit)
            .await
            .map_err(BoxedError::new)
            .context(MitoEditRegionSnafu {
                region_id: target_metadata_region_id,
            })?;
        info!(
            "Successfully edit metadata region: {} after syncing from source metadata region: {}, files to remove: {:?}",
            target_metadata_region_id,
            source_metadata_region_id,
            files_to_remove
                .iter()
                .map(|meta| meta.file_id)
                .collect::<Vec<_>>(),
        );

        let now = Instant::now();
        // Always recover states from the target metadata region after syncing
        // from the source metadata region.
        let physical_region_options = *self
            .state
            .read()
            .unwrap()
            .physical_region_states()
            .get(&target_data_region_id)
            .context(PhysicalRegionNotFoundSnafu {
                region_id: target_data_region_id,
            })?
            .options();
        let new_opened_logical_region_ids = self
            .recover_states(target_data_region_id, physical_region_options)
            .await?;
        info!(
            "Sync metadata region from source region {} to target region {}, recover states cost: {:?}, new opened logical region ids: {:?}",
            source_metadata_region_id,
            target_metadata_region_id,
            now.elapsed(),
            new_opened_logical_region_ids
        );

        Ok(SyncRegionFromResponse::Metric {
            metadata_synced: true,
            data_synced: false,
            new_opened_logical_region_ids,
        })
    }
}

#[cfg(test)]
mod tests {

    use common_error::ext::ErrorExt;
    use common_error::status_code::StatusCode;
    use common_telemetry::debug;
    use store_api::metric_engine_consts::{METRIC_ENGINE_NAME, PHYSICAL_TABLE_METADATA_KEY};
    use store_api::region_engine::{RegionEngine, SyncRegionFromRequest};
    use store_api::region_request::{
        BatchRegionDdlRequest, PathType, RegionCloseRequest, RegionFlushRequest, RegionOpenRequest,
        RegionRequest,
    };
    use store_api::storage::RegionId;

    use crate::metadata_region::MetadataRegion;
    use crate::test_util::{TestEnv, create_logical_region_request};

    async fn assert_logical_table_columns(
        metadata_region: &MetadataRegion,
        physical_region_id: RegionId,
        logical_region_id: RegionId,
        expected_columns: &[&str],
    ) {
        let mut columns = metadata_region
            .logical_columns(physical_region_id, logical_region_id)
            .await
            .unwrap()
            .into_iter()
            .map(|(n, _)| n)
            .collect::<Vec<_>>();
        columns.sort_unstable();
        assert_eq!(columns, expected_columns);
    }

    #[tokio::test]
    async fn test_sync_region_from_region() {
        common_telemetry::init_default_ut_logging();
        let env = TestEnv::new().await;
        let metric_engine = env.metric();
        let source_physical_region_id = RegionId::new(1024, 0);
        let logical_region_id1 = RegionId::new(1025, 0);
        let logical_region_id2 = RegionId::new(1026, 0);
        env.create_physical_region(source_physical_region_id, "/test_dir1", vec![])
            .await;
        let region_create_request1 =
            create_logical_region_request(&["job"], source_physical_region_id, "logical1");
        let region_create_request2 =
            create_logical_region_request(&["host"], source_physical_region_id, "logical2");
        metric_engine
            .handle_batch_ddl_requests(BatchRegionDdlRequest::Create(vec![
                (logical_region_id1, region_create_request1),
                (logical_region_id2, region_create_request2),
            ]))
            .await
            .unwrap();
        debug!("Flushing source physical region");
        metric_engine
            .handle_request(
                source_physical_region_id,
                RegionRequest::Flush(RegionFlushRequest {
                    row_group_size: None,
                }),
            )
            .await
            .unwrap();
        let logical_regions = metric_engine
            .logical_regions(source_physical_region_id)
            .await
            .unwrap();
        assert!(logical_regions.contains(&logical_region_id1));
        assert!(logical_regions.contains(&logical_region_id2));

        let target_physical_region_id = RegionId::new(1024, 1);
        let target_logical_region_id1 = RegionId::new(1025, 1);
        let target_logical_region_id2 = RegionId::new(1026, 1);
        // Prepare target physical region
        env.create_physical_region(target_physical_region_id, "/test_dir1", vec![])
            .await;
        let r = metric_engine
            .sync_region(
                target_physical_region_id,
                SyncRegionFromRequest::FromRegion {
                    source_region_id: source_physical_region_id,
                    parallelism: 1,
                },
            )
            .await
            .unwrap();
        let new_opened_logical_region_ids = r.new_opened_logical_region_ids().unwrap();
        assert_eq!(new_opened_logical_region_ids.len(), 2);
        assert!(new_opened_logical_region_ids.contains(&target_logical_region_id1));
        assert!(new_opened_logical_region_ids.contains(&target_logical_region_id2));
        debug!("Sync region from again");
        assert_logical_table_columns(
            &env.metadata_region(),
            target_physical_region_id,
            target_logical_region_id1,
            &["greptime_timestamp", "greptime_value", "job"],
        )
        .await;
        assert_logical_table_columns(
            &env.metadata_region(),
            target_physical_region_id,
            target_logical_region_id2,
            &["greptime_timestamp", "greptime_value", "host"],
        )
        .await;
        let logical_regions = env
            .metadata_region()
            .logical_regions(target_physical_region_id)
            .await
            .unwrap();
        assert_eq!(logical_regions.len(), 2);
        assert!(logical_regions.contains(&target_logical_region_id1));
        assert!(logical_regions.contains(&target_logical_region_id2));

        // Should be ok to sync region from again.
        let r = metric_engine
            .sync_region(
                target_physical_region_id,
                SyncRegionFromRequest::FromRegion {
                    source_region_id: source_physical_region_id,
                    parallelism: 1,
                },
            )
            .await
            .unwrap();
        let new_opened_logical_region_ids = r.new_opened_logical_region_ids().unwrap();
        assert!(new_opened_logical_region_ids.is_empty());

        // Try to close region and reopen it, should be ok.
        metric_engine
            .handle_request(
                target_physical_region_id,
                RegionRequest::Close(RegionCloseRequest {}),
            )
            .await
            .unwrap();
        let physical_region_option = [(PHYSICAL_TABLE_METADATA_KEY.to_string(), String::new())]
            .into_iter()
            .collect();
        metric_engine
            .handle_request(
                target_physical_region_id,
                RegionRequest::Open(RegionOpenRequest {
                    engine: METRIC_ENGINE_NAME.to_string(),
                    table_dir: "/test_dir1".to_string(),
                    path_type: PathType::Bare,
                    options: physical_region_option,
                    skip_wal_replay: false,
                    checkpoint: None,
                }),
            )
            .await
            .unwrap();
        let logical_regions = env
            .metadata_region()
            .logical_regions(target_physical_region_id)
            .await
            .unwrap();
        assert_eq!(logical_regions.len(), 2);
        assert!(logical_regions.contains(&target_logical_region_id1));
        assert!(logical_regions.contains(&target_logical_region_id2));
    }

    #[tokio::test]
    async fn test_sync_region_from_region_with_no_files() {
        common_telemetry::init_default_ut_logging();
        let env = TestEnv::new().await;
        let metric_engine = env.metric();
        let source_physical_region_id = RegionId::new(1024, 0);
        env.create_physical_region(source_physical_region_id, "/test_dir1", vec![])
            .await;
        let target_physical_region_id = RegionId::new(1024, 1);
        env.create_physical_region(target_physical_region_id, "/test_dir1", vec![])
            .await;
        let r = metric_engine
            .sync_region(
                target_physical_region_id,
                SyncRegionFromRequest::FromRegion {
                    source_region_id: source_physical_region_id,
                    parallelism: 1,
                },
            )
            .await
            .unwrap();
        let new_opened_logical_region_ids = r.new_opened_logical_region_ids().unwrap();
        assert!(new_opened_logical_region_ids.is_empty());
    }

    #[tokio::test]
    async fn test_sync_region_from_region_source_not_exist() {
        common_telemetry::init_default_ut_logging();
        let env = TestEnv::new().await;
        let metric_engine = env.metric();
        let source_physical_region_id = RegionId::new(1024, 0);
        let target_physical_region_id = RegionId::new(1024, 1);
        env.create_physical_region(target_physical_region_id, "/test_dir1", vec![])
            .await;
        let err = metric_engine
            .sync_region(
                target_physical_region_id,
                SyncRegionFromRequest::FromRegion {
                    source_region_id: source_physical_region_id,
                    parallelism: 1,
                },
            )
            .await
            .unwrap_err();
        assert_eq!(err.status_code(), StatusCode::InvalidArguments);
    }
}
