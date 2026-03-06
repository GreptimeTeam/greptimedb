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
                .logical_regions()
                .get(&region_id)
                .copied();
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
        self.metadata_region
            .remove_logical_region(physical_region_id, logical_region_id)
            .await?;

        // Update engine state
        self.state
            .write()
            .unwrap()
            .remove_logical_region(logical_region_id)?;

        Ok(0)
    }
}
