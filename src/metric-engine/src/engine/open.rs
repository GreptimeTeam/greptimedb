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

//! Open a metric region.

use common_telemetry::info;
use mito2::engine::MITO_ENGINE_NAME;
use object_store::util::join_dir;
use snafu::{OptionExt, ResultExt};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metric_engine_consts::{DATA_REGION_SUBDIR, METADATA_REGION_SUBDIR};
use store_api::region_engine::RegionEngine;
use store_api::region_request::{AffectedRows, RegionOpenRequest, RegionRequest};
use store_api::storage::RegionId;

use crate::engine::create::region_options_for_metadata_region;
use crate::engine::options::{set_data_region_options, PhysicalRegionOptions};
use crate::engine::MetricEngineInner;
use crate::error::{OpenMitoRegionSnafu, PhysicalRegionNotFoundSnafu, Result};
use crate::metrics::{LOGICAL_REGION_COUNT, PHYSICAL_REGION_COUNT};
use crate::utils;

impl MetricEngineInner {
    /// Open a metric region.
    ///
    /// Only open requests to a physical region matter. Those to logical regions are
    /// actually an empty operation -- it only check if the request is valid. Since
    /// logical regions are multiplexed over physical regions, they are always "open".
    ///
    /// If trying to open a logical region whose physical region is not open, metric
    /// engine will throw a [RegionNotFound](common_error::status_code::StatusCode::RegionNotFound)
    /// error.
    pub async fn open_region(
        &self,
        region_id: RegionId,
        request: RegionOpenRequest,
    ) -> Result<AffectedRows> {
        if request.is_physical_table() {
            // open physical region and recover states
            let physical_region_options = PhysicalRegionOptions::try_from(&request.options)?;
            self.open_physical_region(region_id, request).await?;
            let data_region_id = utils::to_data_region_id(region_id);
            let primary_key_encoding = self.mito.get_primary_key_encoding(data_region_id).context(
                PhysicalRegionNotFoundSnafu {
                    region_id: data_region_id,
                },
            )?;
            self.recover_states(region_id, primary_key_encoding, physical_region_options)
                .await?;

            Ok(0)
        } else {
            // Don't check if the logical region exist. Because a logical region cannot be opened
            // individually, it is always "open" if its physical region is open. But the engine
            // can't tell if the logical region is not exist or the physical region is not opened
            // yet. Thus simply return `Ok` here to ignore all those errors.
            Ok(0)
        }
    }

    /// Invokes mito engine to open physical regions (data and metadata).
    async fn open_physical_region(
        &self,
        region_id: RegionId,
        request: RegionOpenRequest,
    ) -> Result<AffectedRows> {
        let metadata_region_dir = join_dir(&request.region_dir, METADATA_REGION_SUBDIR);
        let data_region_dir = join_dir(&request.region_dir, DATA_REGION_SUBDIR);

        let metadata_region_options = region_options_for_metadata_region(request.options.clone());
        let open_metadata_region_request = RegionOpenRequest {
            region_dir: metadata_region_dir,
            options: metadata_region_options,
            engine: MITO_ENGINE_NAME.to_string(),
            skip_wal_replay: request.skip_wal_replay,
        };

        let mut data_region_options = request.options;
        set_data_region_options(
            &mut data_region_options,
            self.config.experimental_sparse_primary_key_encoding,
        );
        let open_data_region_request = RegionOpenRequest {
            region_dir: data_region_dir,
            options: data_region_options,
            engine: MITO_ENGINE_NAME.to_string(),
            skip_wal_replay: request.skip_wal_replay,
        };

        let metadata_region_id = utils::to_metadata_region_id(region_id);
        let data_region_id = utils::to_data_region_id(region_id);

        self.mito
            .handle_request(
                metadata_region_id,
                RegionRequest::Open(open_metadata_region_request),
            )
            .await
            .with_context(|_| OpenMitoRegionSnafu {
                region_type: "metadata",
            })?;
        self.mito
            .handle_request(
                data_region_id,
                RegionRequest::Open(open_data_region_request),
            )
            .await
            .with_context(|_| OpenMitoRegionSnafu {
                region_type: "data",
            })?;

        info!("Opened physical metric region {region_id}");
        PHYSICAL_REGION_COUNT.inc();

        Ok(0)
    }

    /// Recovers [MetricEngineState](crate::engine::state::MetricEngineState) from
    /// physical region (idnefied by the given region id).
    ///
    /// Includes:
    /// - Record physical region's column names
    /// - Record the mapping between logical region id and physical region id
    pub(crate) async fn recover_states(
        &self,
        physical_region_id: RegionId,
        primary_key_encoding: PrimaryKeyEncoding,
        physical_region_options: PhysicalRegionOptions,
    ) -> Result<()> {
        // load logical regions and physical column names
        let logical_regions = self
            .metadata_region
            .logical_regions(physical_region_id)
            .await?;
        let physical_columns = self
            .data_region
            .physical_columns(physical_region_id)
            .await?;
        let logical_region_num = logical_regions.len();

        {
            let mut state = self.state.write().unwrap();
            // recover physical column names
            let physical_columns = physical_columns
                .into_iter()
                .map(|col| (col.column_schema.name, col.column_id))
                .collect();
            state.add_physical_region(
                physical_region_id,
                physical_columns,
                primary_key_encoding,
                physical_region_options,
            );
            // recover logical regions
            for logical_region_id in &logical_regions {
                state.add_logical_region(physical_region_id, *logical_region_id);
            }
        }

        for logical_region_id in logical_regions {
            self.metadata_region
                .open_logical_region(logical_region_id)
                .await;
        }

        LOGICAL_REGION_COUNT.add(logical_region_num as i64);

        Ok(())
    }
}

// Unit tests in engine.rs
