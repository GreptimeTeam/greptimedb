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
    MitoPauseCompactionOperationSnafu, PhysicalRegionNotFoundSnafu, Result,
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
            "Syncing region from region {} to region {}, parallelism: {}",
            source_region_id, region_id, parallelism
        );

        // Cleanup removes the original copied file IDs, so compaction must not replace them.
        let guard = self
            .mito
            .pause_compaction(target_metadata_region_id)
            .await
            .map_err(BoxedError::new)
            .context(MitoPauseCompactionOperationSnafu {
                region_id: target_metadata_region_id,
            })?;
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
            self.metadata_region
                .remove_copied_source_keys(target_data_region_id, source_data_region_id)
                .await?;
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
        // `copy_region_from` does not trigger compaction,
        // so there should be no files removed and thus no missing files.
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
            ..Default::default()
        };
        self.mito
            .edit_region(target_metadata_region_id, edit)
            .await
            .map_err(BoxedError::new)
            .context(MitoEditRegionSnafu {
                region_id: target_metadata_region_id,
            })?;
        self.metadata_region
            .remove_copied_source_keys(target_data_region_id, source_data_region_id)
            .await?;
        drop(guard);
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
    async fn test_metadata_sync_pause_on_missing_target() {
        let env = TestEnv::new().await;
        let source = env.default_physical_region_id();
        let target = RegionId::new(source.table_id(), source.region_number() + 1);
        let target_metadata = crate::utils::to_metadata_region_id(target);
        let err = env
            .metric()
            .inner
            .sync_region_from_region(target, source, 1)
            .await
            .unwrap_err();
        assert_eq!(StatusCode::RegionNotFound, err.status_code());
        let crate::error::Error::MitoPauseCompactionOperation {
            region_id, source, ..
        } = err
        else {
            panic!("unexpected sync error: {err:?}");
        };
        assert_eq!(target_metadata, region_id);
        assert!(matches!(
            source.into_inner().as_any().downcast_ref::<mito2::error::Error>(),
            Some(mito2::error::Error::RegionNotFound { region_id, .. }) if *region_id == target_metadata
        ));
    }

    #[tokio::test]
    async fn test_metadata_sync_blocks_compaction_until_cleanup() {
        let env = TestEnv::new().await;
        env.init_metric_region().await;
        let source = env.default_physical_region_id();
        let logical = env.default_logical_region_id();
        let target = RegionId::new(source.table_id(), 7);
        env.create_physical_region(target, &TestEnv::default_table_dir(), vec![])
            .await;
        // Leave target rows in the memtable so the intervening flush creates an SST.
        let existing = RegionId::new(4, target.region_number());
        env.create_logical_region(target, existing).await;
        let metric = env.metric();
        metric
            .handle_request(source, RegionRequest::Flush(Default::default()))
            .await
            .unwrap();
        let target_metadata = crate::utils::to_metadata_region_id(target);
        let metadata = &metric.inner.metadata_region;
        let mito = env.mito();

        // Hold the transform at its metadata write. The copied source marker is
        // observable through a key-only scan while the sync waits for the lock.
        let observe = metadata.with_writes_paused(target_metadata, async {
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                while !metadata
                    .logical_regions(target)
                    .await
                    .unwrap()
                    .contains(&logical)
                {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            mito.handle_request(target_metadata, RegionRequest::Flush(Default::default()))
                .await
                .unwrap();
            let err = mito
                .handle_request(target_metadata, RegionRequest::Compact(Default::default()))
                .await
                .unwrap_err();
            assert!(matches!(
                err.into_inner()
                    .as_any()
                    .downcast_ref::<mito2::error::Error>(),
                Some(mito2::error::Error::CompactionCancelled { .. })
            ));
        });
        let sync = metric.sync_region(
            target,
            SyncRegionFromRequest::FromRegion {
                source_region_id: source,
                parallelism: 1,
            },
        );
        let ((), result) = tokio::join!(observe, sync);
        result.unwrap();
        let remapped = RegionId::new(logical.table_id(), target.region_number());
        let mut recovered = metadata.logical_regions(target).await.unwrap();
        recovered.sort_unstable();
        assert_eq!(recovered, [remapped, existing]);
        mito.handle_request(target_metadata, RegionRequest::Compact(Default::default()))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_retry_after_cleanup_failure_without_compaction() {
        assert_retry_after_cleanup_failure(None).await;
    }

    #[tokio::test]
    async fn test_retry_after_cleanup_failure_with_target_compaction() {
        assert_retry_after_cleanup_failure(Some(CompactionRegion::Target)).await;
    }

    #[tokio::test]
    async fn test_retry_after_cleanup_failure_with_source_compaction() {
        assert_retry_after_cleanup_failure(Some(CompactionRegion::Source)).await;
    }

    enum CompactionRegion {
        Source,
        Target,
    }

    async fn assert_retry_after_cleanup_failure(compaction: Option<CompactionRegion>) {
        use std::time::Duration;

        use mito2::test_util::{DeltaFaults, TestEnv as MitoTestEnv};

        let faults = DeltaFaults::default();
        let mito_env = MitoTestEnv::with_prefix("sync-cleanup-retry")
            .await
            .with_mock_layer(faults.layer());
        let env = TestEnv::with_mito_env(mito_env).await;
        let source = env.default_physical_region_id();
        let target = RegionId::new(source.table_id(), 7);
        env.create_physical_region(source, &TestEnv::default_table_dir(), vec![])
            .await;
        env.create_physical_region(target, &TestEnv::default_table_dir(), vec![])
            .await;
        let source_meta = crate::utils::to_metadata_region_id(source);
        let target_meta = crate::utils::to_metadata_region_id(target);
        let mito = env.mito();
        let metric = env.metric();
        let metadata = &metric.inner.metadata_region;
        // Keep the source file set stable until the first attempt has failed.
        let source_guard = mito.pause_compaction(source_meta).await.unwrap();
        let mut expected = Vec::new();
        for table_id in 10..15 {
            let logical = RegionId::new(table_id, source.region_number());
            let request = create_logical_region_request(&["job"], source, "test");
            metric
                .handle_request(logical, RegionRequest::Create(request))
                .await
                .unwrap();
            mito.handle_request(source_meta, RegionRequest::Flush(Default::default()))
                .await
                .unwrap();
            expected.push(RegionId::new(table_id, target.region_number()));
        }

        let sync = || metric.inner.sync_region_from_region(target, source, 1);
        assert!(metadata.logical_regions(target).await.unwrap().is_empty());
        let fail_cleanup = metadata.with_writes_paused(target_meta, async {
            tokio::time::timeout(Duration::from_secs(10), async {
                while metadata.logical_regions(target).await.unwrap().is_empty() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            faults.fail_next_write();
        });
        let ((), first) = tokio::join!(fail_cleanup, sync());
        assert!(
            matches!(first, Err(crate::error::Error::MitoEditRegion { .. })),
            "{first:?}"
        );
        // An uncertain manifest edit closes the region. Replay it before retrying.
        mito.handle_request(
            target_meta,
            RegionRequest::Open(RegionOpenRequest {
                engine: mito2::engine::MITO_ENGINE_NAME.into(),
                table_dir: TestEnv::default_table_dir(),
                path_type: PathType::Metadata,
                options: Default::default(),
                skip_wal_replay: false,
                checkpoint: None,
                requirements: Default::default(),
            }),
        )
        .await
        .unwrap();
        mito.set_region_role(target_meta, store_api::region_engine::RegionRole::Leader)
            .unwrap();
        let copied = mito.find_memtable_and_sst_stats(target_meta).unwrap().1;
        assert!(!copied.is_empty());
        let changed_region = match compaction {
            Some(CompactionRegion::Target) => {
                // Flushing the transformed rows triggers normal automatic compaction.
                mito.handle_request(target_meta, RegionRequest::Flush(Default::default()))
                    .await
                    .unwrap();
                Some(target_meta)
            }
            Some(CompactionRegion::Source) => {
                drop(source_guard);
                mito.handle_request(source_meta, RegionRequest::Compact(Default::default()))
                    .await
                    .unwrap();
                Some(source_meta)
            }
            None => None,
        };
        if let Some(changed_region) = changed_region {
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    let files = mito.find_memtable_and_sst_stats(changed_region).unwrap().1;
                    if copied
                        .iter()
                        .any(|old| files.iter().all(|f| f.file_id != old.file_id))
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("compaction must replace a copied SST before retry");
        }
        sync().await.unwrap();
        let mut actual = metadata.logical_regions(target).await.unwrap();
        actual.sort_unstable();
        assert_eq!(actual, expected);
        for id in expected {
            let source_id = RegionId::new(id.table_id(), source.region_number());
            assert!(
                metadata
                    .logical_columns(target, source_id)
                    .await
                    .unwrap()
                    .is_empty()
            );
            assert_eq!(3, metadata.logical_columns(target, id).await.unwrap().len());
        }
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
                RegionRequest::Flush(RegionFlushRequest::default()),
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
                RegionRequest::Close(RegionCloseRequest::default()),
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
                    requirements: Default::default(),
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
    async fn test_sync_replays_partial_column_batch_after_restart() {
        use store_api::region_engine::{MitoCopyRegionFromRequest, RegionRole};

        let mut env = TestEnv::new().await;
        let source = env.default_physical_region_id();
        let logical = env.default_logical_region_id();
        let target = RegionId::new(source.table_id(), source.region_number() + 1);
        let target_logical = RegionId::new(logical.table_id(), target.region_number());
        let dir = TestEnv::default_table_dir();
        env.init_metric_region().await;
        env.create_physical_region(target, &dir, vec![]).await;
        let engine = env.metric();
        engine.inner.flush_task.stop().await.unwrap();
        engine
            .handle_request(source, RegionRequest::Flush(Default::default()))
            .await
            .unwrap();

        // Model a crash after the input SSTs are installed and one column put
        // has reached the WAL, before the new region marker has been written.
        let target_metadata = crate::utils::to_metadata_region_id(target);
        env.mito()
            .copy_region_from(
                target_metadata,
                MitoCopyRegionFromRequest {
                    source_region_id: crate::utils::to_metadata_region_id(source),
                    parallelism: 1,
                },
            )
            .await
            .unwrap();
        let (name, column) = env
            .metadata_region()
            .logical_columns(source, logical)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let put = MetadataRegion::build_put_request_from_iter(std::iter::once((
            MetadataRegion::concat_column_key(target_logical, &name),
            MetadataRegion::serialize_column_metadata(&column),
        )));
        env.mito()
            .handle_request(target_metadata, RegionRequest::Put(put))
            .await
            .unwrap();
        env.reopen_engine(Default::default()).await;
        let engine = env.metric();
        engine
            .handle_request(
                target,
                RegionRequest::Open(RegionOpenRequest {
                    engine: METRIC_ENGINE_NAME.to_string(),
                    table_dir: dir,
                    path_type: PathType::Bare,
                    options: [(PHYSICAL_TABLE_METADATA_KEY.to_string(), String::new())]
                        .into_iter()
                        .collect(),
                    skip_wal_replay: false,
                    checkpoint: None,
                    requirements: Default::default(),
                }),
            )
            .await
            .unwrap();
        // The heartbeat sync handler only sends FromRegion to a writable leader.
        engine.set_region_role(target, RegionRole::Leader).unwrap();
        let result = engine
            .sync_region(
                target,
                SyncRegionFromRequest::FromRegion {
                    source_region_id: source,
                    parallelism: 1,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            result.new_opened_logical_region_ids().unwrap(),
            vec![target_logical]
        );
        assert_eq!(
            engine.logical_regions(target).await.unwrap(),
            vec![target_logical]
        );
        assert_logical_table_columns(
            &env.metadata_region(),
            target,
            target_logical,
            &["greptime_timestamp", "greptime_value", "job"],
        )
        .await;
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
