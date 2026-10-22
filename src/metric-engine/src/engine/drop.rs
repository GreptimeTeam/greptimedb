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

//! Drop a metric region

use common_telemetry::{debug, info};
use snafu::ResultExt;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{AffectedRows, RegionDropRequest, RegionRequest};
use store_api::storage::RegionId;

use crate::engine::MetricEngineInner;
use crate::error::{
    CloseMitoRegionSnafu, LogicalRegionNotFoundSnafu, PhysicalRegionBusySnafu, Result,
};
use crate::metrics::PHYSICAL_REGION_COUNT;
use crate::utils;

impl MetricEngineInner {
    pub async fn drop_region(
        &self,
        region_id: RegionId,
        req: RegionDropRequest,
    ) -> Result<AffectedRows> {
        let data_region_id = utils::to_data_region_id(region_id);
        let fast_path = req.fast_path;
        let force = req.force;

        // enclose the guard in a block to prevent the guard from polluting the async context
        let (is_physical_region, is_physical_region_busy) = {
            if let Some(state) = self
                .state
                .read()
                .unwrap()
                .physical_region_states()
                .get(&data_region_id)
            {
                debug!(
                    "Physical region {} is busy, there are still some logical regions: {:?}",
                    data_region_id,
                    state
                        .logical_regions()
                        .iter()
                        .map(|id| id.to_string())
                        .collect::<Vec<_>>()
                );
                (true, !state.logical_regions().is_empty())
            } else {
                // the second argument is not used, just pass in a dummy value
                (false, true)
            }
        };

        if is_physical_region {
            // check if there is no logical region relates to this physical region
            if is_physical_region_busy && !force {
                // reject if there is any present logical region
                return Err(PhysicalRegionBusySnafu {
                    region_id: data_region_id,
                }
                .build());
            }
            if is_physical_region_busy && force {
                info!("Dropping physical region {} with force", data_region_id);
            }
            return self
                .drop_physical_region(data_region_id, req.partial_drop)
                .await;
        }

        if fast_path {
            // for fast path, we don't delete the metadata in the metadata region.
            // it only remove the logical region from the engine state.
            //
            // The drop database procedure will ensure the metadata region and data region are dropped eventually.
            self.state
                .write()
                .unwrap()
                .remove_logical_region(region_id)?;

            Ok(0)
        } else {
            let metadata_region_id = self
                .state
                .read()
                .unwrap()
                .physical_region_to_drop(region_id);
            if let Some(metadata_region_id) = metadata_region_id {
                self.drop_logical_region(region_id, metadata_region_id)
                    .await
            } else {
                Err(LogicalRegionNotFoundSnafu { region_id }.build())
            }
        }
    }

    async fn drop_physical_region(
        &self,
        region_id: RegionId,
        partial_drop: bool,
    ) -> Result<AffectedRows> {
        let data_region_id = utils::to_data_region_id(region_id);
        let metadata_region_id = utils::to_metadata_region_id(region_id);

        // Drop mito regions.
        // Since the physical regions are going to be dropped, we don't need to
        // update the contents in metadata region.
        self.mito
            .handle_request(
                data_region_id,
                RegionRequest::Drop(RegionDropRequest {
                    fast_path: false,
                    force: false,
                    partial_drop,
                }),
            )
            .await
            .with_context(|_| CloseMitoRegionSnafu { region_id })?;
        self.mito
            .handle_request(
                metadata_region_id,
                RegionRequest::Drop(RegionDropRequest {
                    fast_path: false,
                    force: false,
                    partial_drop,
                }),
            )
            .await
            .with_context(|_| CloseMitoRegionSnafu { region_id })?;

        PHYSICAL_REGION_COUNT.dec();

        // Update engine state
        self.state
            .write()
            .unwrap()
            .remove_physical_region(data_region_id)?;

        Ok(0)
    }

    async fn drop_logical_region(
        &self,
        logical_region_id: RegionId,
        physical_region_id: RegionId,
    ) -> Result<AffectedRows> {
        // Update metadata
        if let Err(err) = self
            .metadata_region
            .remove_logical_region(physical_region_id, logical_region_id)
            .await
        {
            // The delete may have been applied although it failed. The region takes no more
            // writes, and a retried drop deletes its metadata again and removes it.
            self.state
                .write()
                .unwrap()
                .mark_logical_region_dropping(logical_region_id);
            return Err(err);
        }

        // Update engine state
        self.state
            .write()
            .unwrap()
            .finish_logical_region_drop(logical_region_id)?;

        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use api::v1::Rows;
    use common_wal::config::object_store::{ObjectStoreWalConfig, STANDALONE_GENERATION};
    use common_wal::options::{ObjectStoreWalOptions, WAL_OPTIONS_KEY, WalOptions};
    use log_store::ObjectStoreLogStore;
    use mito2::config::MitoConfig;
    use mito2::test_util::TestEnv as MitoTestEnv;
    use object_store::ObjectStore;
    use object_store::services::Memory;
    use store_api::logstore::LogStore;
    use store_api::region_engine::RegionEngine;
    use store_api::region_request::{RegionDropRequest, RegionPutRequest, RegionRequest};
    use store_api::storage::RegionId;

    use crate::test_util::{TestEnv, build_rows, row_schema_with_tags};
    use crate::utils::{to_data_region_id, to_metadata_region_id};

    fn drop_request() -> RegionRequest {
        RegionRequest::Drop(RegionDropRequest {
            fast_path: false,
            force: false,
            partial_drop: false,
        })
    }

    /// How the WAL releases of the dropped regions fail.
    #[derive(Clone, Copy, PartialEq)]
    enum ReleaseFailure {
        /// The release of the data region fails transiently.
        Data,
        /// The releases of the data and the metadata region fail transiently.
        Both,
        /// The release of the data region meets a conflicting object, which
        /// poisons the WAL, so the cleanup of the metadata region fails too.
        Conflict,
    }

    #[tokio::test]
    async fn test_drop_physical_region_whose_wal_release_fails() {
        for failure in [
            ReleaseFailure::Data,
            ReleaseFailure::Both,
            ReleaseFailure::Conflict,
        ] {
            let object_store = ObjectStore::new(Memory::default()).unwrap();
            let config = ObjectStoreWalConfig::default();
            let open_wal = || {
                ObjectStoreLogStore::try_new(
                    object_store.clone(),
                    &config,
                    0,
                    STANDALONE_GENERATION,
                )
            };
            let wal = open_wal().await.unwrap();
            let mut mito_env = MitoTestEnv::with_prefix("metric-drop-failed-release").await;
            let mito = mito_env
                .create_engine_with_log_store(MitoConfig::default(), wal.clone())
                .await;
            let env = TestEnv::with_mito_engine(mito_env, mito);
            let region_id = RegionId::new(1, 1);
            let wal_options = WalOptions::ObjectStore(ObjectStoreWalOptions::new(
                "wal/datanodes/0/epochs/0".to_string(),
            ));
            env.create_physical_region(
                region_id,
                "test_metric_table",
                vec![(
                    WAL_OPTIONS_KEY.to_string(),
                    serde_json::to_string(&wal_options).unwrap(),
                )],
            )
            .await;
            // Both mito regions write to the WAL: the data region the rows of a
            // logical region, the metadata region its creation and its drop.
            let logical_region_id = RegionId::new(1024, 1);
            env.create_logical_region(region_id, logical_region_id)
                .await;
            let metric = env.metric();
            metric
                .handle_request(
                    logical_region_id,
                    RegionRequest::Put(RegionPutRequest {
                        skip_wal: false,
                        rows: Rows {
                            schema: row_schema_with_tags(&["job"]),
                            rows: build_rows(1, 10),
                        },
                        hint: None,
                        partition_expr_version: None,
                    }),
                )
                .await
                .unwrap();
            metric
                .handle_request(logical_region_id, drop_request())
                .await
                .unwrap();

            // The drop still removes both regions and the physical region state.
            match failure {
                ReleaseFailure::Data => wal.fail_next_create_after_write(),
                ReleaseFailure::Both => wal.fail_creates(),
                ReleaseFailure::Conflict => {
                    // A copy of the last object, of the same epoch, takes the
                    // sequence of the next one.
                    let objects = "wal/datanodes/0/epochs/0/objects/";
                    let last = object_store
                        .list(objects)
                        .await
                        .unwrap()
                        .iter()
                        .filter_map(|entry| entry.name().strip_suffix(".wal")?.parse::<u64>().ok())
                        .max()
                        .unwrap();
                    let path = |object_seq: u64| format!("{objects}{object_seq:020}.wal");
                    let copy = object_store.read(&path(last)).await.unwrap();
                    object_store.write(&path(last + 1), copy).await.unwrap();
                }
            }
            metric
                .handle_request(region_id, drop_request())
                .await
                .unwrap();
            assert!(!env.mito().is_region_exists(to_data_region_id(region_id)));
            assert!(
                !env.mito()
                    .is_region_exists(to_metadata_region_id(region_id))
            );
            assert!(
                !metric
                    .inner
                    .state
                    .read()
                    .unwrap()
                    .exist_physical_region(to_data_region_id(region_id))
            );

            // The stop writes the release records of failed releases. A store
            // opened later finds both regions released and keeps no segment of
            // either. A poisoned store writes nothing more.
            if failure == ReleaseFailure::Conflict {
                assert!(wal.list_namespaces().await.is_err());
                continue;
            }
            wal.allow_creates();
            wal.stop().await.unwrap();
            let wal = open_wal().await.unwrap();
            wal.wait_for_garbage_collection().await;
            assert!(wal.list_namespaces().await.unwrap().is_empty());
            wal.stop().await.unwrap();
        }
    }
}
