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

use std::collections::HashSet;

use api::v1::SemanticType;
use common_telemetry::info;
use mito2::engine::MITO_ENGINE_NAME;
use object_store::util::join_dir;
use snafu::{OptionExt, ResultExt};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metric_engine_consts::{DATA_REGION_SUBDIR, METADATA_REGION_SUBDIR};
use store_api::region_engine::{BatchResponses, RegionEngine};
use store_api::region_request::{AffectedRows, RegionOpenRequest, RegionRequest};
use store_api::storage::RegionId;

use crate::engine::create::region_options_for_metadata_region;
use crate::engine::options::{set_data_region_options, PhysicalRegionOptions};
use crate::engine::MetricEngineInner;
use crate::error::{
    BatchOpenMitoRegionSnafu, OpenMitoRegionSnafu, PhysicalRegionNotFoundSnafu, Result,
};
use crate::metrics::{LOGICAL_REGION_COUNT, PHYSICAL_REGION_COUNT};
use crate::utils;

impl MetricEngineInner {
    pub async fn handle_batch_open_requests(
        &self,
        parallelism: usize,
        requests: Vec<(RegionId, RegionOpenRequest)>,
    ) -> Result<BatchResponses> {
        // We need to open metadata region and data region for each request.
        let mut all_requests = Vec::with_capacity(requests.len() * 2);
        let mut physical_region_ids = Vec::with_capacity(requests.len());
        let mut data_region_ids = HashSet::with_capacity(requests.len());

        for (region_id, request) in requests {
            if !request.is_physical_table() {
                continue;
            }
            let physical_region_options = PhysicalRegionOptions::try_from(&request.options)?;
            let metadata_region_id = utils::to_metadata_region_id(region_id);
            let data_region_id = utils::to_data_region_id(region_id);
            let (open_metadata_region_request, open_data_region_request) =
                self.transform_open_physical_region_request(request);
            all_requests.push((metadata_region_id, open_metadata_region_request));
            all_requests.push((data_region_id, open_data_region_request));
            physical_region_ids.push((region_id, physical_region_options));
            data_region_ids.insert(data_region_id);
        }

        let results = self
            .mito
            .handle_batch_open_requests(parallelism, all_requests)
            .await
            .context(BatchOpenMitoRegionSnafu {})?
            .into_iter()
            .filter(|(region_id, _)| data_region_ids.contains(region_id))
            .collect::<Vec<_>>();

        for (physical_region_id, physical_region_options) in physical_region_ids {
            let primary_key_encoding = self
                .mito
                .get_primary_key_encoding(physical_region_id)
                .context(PhysicalRegionNotFoundSnafu {
                    region_id: physical_region_id,
                })?;
            self.recover_states(
                physical_region_id,
                primary_key_encoding,
                physical_region_options,
            )
            .await?;
        }

        Ok(results)
    }

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

    /// Transform the open request to open metadata region and data region.
    ///
    /// Returns:
    /// - The open request for metadata region.
    /// - The open request for data region.
    fn transform_open_physical_region_request(
        &self,
        request: RegionOpenRequest,
    ) -> (RegionOpenRequest, RegionOpenRequest) {
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

        (open_metadata_region_request, open_data_region_request)
    }

    /// Invokes mito engine to open physical regions (data and metadata).
    async fn open_physical_region(
        &self,
        region_id: RegionId,
        request: RegionOpenRequest,
    ) -> Result<AffectedRows> {
        let metadata_region_id = utils::to_metadata_region_id(region_id);
        let data_region_id = utils::to_data_region_id(region_id);
        let (open_metadata_region_request, open_data_region_request) =
            self.transform_open_physical_region_request(request);

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
    ///
    /// Returns new opened logical region ids.
    pub(crate) async fn recover_states(
        &self,
        physical_region_id: RegionId,
        primary_key_encoding: PrimaryKeyEncoding,
        physical_region_options: PhysicalRegionOptions,
    ) -> Result<Vec<RegionId>> {
        // load logical regions and physical column names
        let logical_regions = self
            .metadata_region
            .logical_regions(physical_region_id)
            .await?;
        let physical_columns = self
            .data_region
            .physical_columns(physical_region_id)
            .await?;

        {
            let mut state = self.state.write().unwrap();
            // recover physical column names
            // Safety: The physical columns are loaded from the data region, which always
            // has a time index.
            let time_index_unit = physical_columns
                .iter()
                .find_map(|col| {
                    if col.semantic_type == SemanticType::Timestamp {
                        col.column_schema
                            .data_type
                            .as_timestamp()
                            .map(|data_type| data_type.unit())
                    } else {
                        None
                    }
                })
                .unwrap();
            let physical_columns = physical_columns
                .into_iter()
                .map(|col| (col.column_schema.name, col.column_id))
                .collect();
            state.add_physical_region(
                physical_region_id,
                physical_columns,
                primary_key_encoding,
                physical_region_options,
                time_index_unit,
            );
            // recover logical regions
            for logical_region_id in &logical_regions {
                state.add_logical_region(physical_region_id, *logical_region_id);
            }
        }

        let mut opened_logical_region_ids = Vec::new();
        // The `recover_states` may be called multiple times, we only count the logical regions
        // that are opened for the first time.
        for logical_region_id in logical_regions {
            if self
                .metadata_region
                .open_logical_region(logical_region_id)
                .await
            {
                opened_logical_region_ids.push(logical_region_id);
            }
        }

        LOGICAL_REGION_COUNT.add(opened_logical_region_ids.len() as i64);

        Ok(opened_logical_region_ids)
    }
}

// Unit tests in engine.rs
