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

use api::region::RegionResponse;
use api::v1::SemanticType;
use common_error::ext::BoxedError;
use common_telemetry::{error, info, warn};
use datafusion::common::HashMap;
use mito2::engine::MITO_ENGINE_NAME;
use snafu::{OptionExt, ResultExt};
use store_api::region_engine::{BatchResponses, RegionEngine};
use store_api::region_request::{AffectedRows, PathType, RegionOpenRequest, ReplayCheckpoint};
use store_api::storage::RegionId;

use crate::engine::MetricEngineInner;
use crate::engine::create::region_options_for_metadata_region;
use crate::engine::options::{PhysicalRegionOptions, set_data_region_options};
use crate::error::{
    BatchOpenMitoRegionSnafu, NoOpenRegionResultSnafu, OpenMitoRegionSnafu,
    PhysicalRegionNotFoundSnafu, Result,
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
        let mut physical_region_ids = HashMap::with_capacity(requests.len());

        for (region_id, request) in requests {
            if !request.is_physical_table() {
                warn!("Skipping non-physical table open request: {region_id}");
                continue;
            }
            let physical_region_options = PhysicalRegionOptions::try_from(&request.options)?;
            let metadata_region_id = utils::to_metadata_region_id(region_id);
            let data_region_id = utils::to_data_region_id(region_id);
            let (open_metadata_region_request, open_data_region_request) =
                self.transform_open_physical_region_request(request);
            all_requests.push((metadata_region_id, open_metadata_region_request));
            all_requests.push((data_region_id, open_data_region_request));
            physical_region_ids.insert(region_id, physical_region_options);
        }

        let mut results = self
            .mito
            .handle_batch_open_requests(parallelism, all_requests)
            .await
            .context(BatchOpenMitoRegionSnafu {})?
            .into_iter()
            .collect::<HashMap<_, _>>();

        let mut responses = Vec::with_capacity(physical_region_ids.len());
        for (physical_region_id, physical_region_options) in physical_region_ids {
            let metadata_region_id = utils::to_metadata_region_id(physical_region_id);
            let data_region_id = utils::to_data_region_id(physical_region_id);
            let metadata_region_result = results.remove(&metadata_region_id);
            let data_region_result: Option<std::result::Result<RegionResponse, BoxedError>> =
                results.remove(&data_region_id);
            // Pass the optional `metadata_region_result` and `data_region_result` to
            // `recover_physical_region_with_results`. This function handles errors for each
            // open physical region request, allowing the process to continue with the
            // remaining regions even if some requests fail.
            let response = self
                .recover_physical_region_with_results(
                    metadata_region_result,
                    data_region_result,
                    physical_region_id,
                    physical_region_options,
                    true,
                )
                .await
                .map_err(BoxedError::new);
            responses.push((physical_region_id, response));
        }

        Ok(responses)
    }

    // If the metadata region is opened with a stale manifest,
    // the metric engine may fail to recover logical tables from the metadata region,
    // as the manifest could reference files that have already been deleted
    // due to compaction operations performed by the region leader.
    async fn close_physical_region_on_recovery_failure(&self, physical_region_id: RegionId) {
        info!(
            "Closing metadata region {} and data region {} on metadata recovery failure",
            utils::to_metadata_region_id(physical_region_id),
            utils::to_data_region_id(physical_region_id)
        );
        if let Err(err) = self.close_physical_region(physical_region_id).await {
            error!(err; "Failed to close physical region {}", physical_region_id);
        }
    }

    pub(crate) async fn recover_physical_region_with_results(
        &self,
        metadata_region_result: Option<std::result::Result<RegionResponse, BoxedError>>,
        data_region_result: Option<std::result::Result<RegionResponse, BoxedError>>,
        physical_region_id: RegionId,
        physical_region_options: PhysicalRegionOptions,
        close_region_on_failure: bool,
    ) -> Result<RegionResponse> {
        let metadata_region_id = utils::to_metadata_region_id(physical_region_id);
        let data_region_id = utils::to_data_region_id(physical_region_id);
        let _ = metadata_region_result
            .context(NoOpenRegionResultSnafu {
                region_id: metadata_region_id,
            })?
            .context(OpenMitoRegionSnafu {
                region_type: "metadata",
            })?;

        let data_region_response = data_region_result
            .context(NoOpenRegionResultSnafu {
                region_id: data_region_id,
            })?
            .context(OpenMitoRegionSnafu {
                region_type: "data",
            })?;

        if let Err(err) = self
            .recover_states(physical_region_id, physical_region_options)
            .await
        {
            if close_region_on_failure {
                self.close_physical_region_on_recovery_failure(physical_region_id)
                    .await;
            }
            return Err(err);
        }
        Ok(data_region_response)
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
            if self
                .state
                .read()
                .unwrap()
                .physical_region_states()
                .get(&region_id)
                .is_some()
            {
                warn!(
                    "The physical region {} is already open, ignore the open request",
                    region_id
                );
                return Ok(0);
            }
            // open physical region and recover states
            let physical_region_options = PhysicalRegionOptions::try_from(&request.options)?;
            self.open_physical_region(region_id, request).await?;
            if let Err(err) = self
                .recover_states(region_id, physical_region_options)
                .await
            {
                self.close_physical_region_on_recovery_failure(region_id)
                    .await;
                return Err(err);
            }

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
        let metadata_region_options = region_options_for_metadata_region(&request.options);
        let checkpoint = request.checkpoint;

        let open_metadata_region_request = RegionOpenRequest {
            table_dir: request.table_dir.clone(),
            path_type: PathType::Metadata,
            options: metadata_region_options,
            engine: MITO_ENGINE_NAME.to_string(),
            skip_wal_replay: request.skip_wal_replay,
            checkpoint: checkpoint.map(|checkpoint| ReplayCheckpoint {
                entry_id: checkpoint.metadata_entry_id.unwrap_or_default(),
                metadata_entry_id: None,
            }),
        };

        let mut data_region_options = request.options;
        set_data_region_options(
            &mut data_region_options,
            self.config.sparse_primary_key_encoding,
        );
        let open_data_region_request = RegionOpenRequest {
            table_dir: request.table_dir.clone(),
            path_type: PathType::Data,
            options: data_region_options,
            engine: MITO_ENGINE_NAME.to_string(),
            skip_wal_replay: request.skip_wal_replay,
            checkpoint: checkpoint.map(|checkpoint| ReplayCheckpoint {
                entry_id: checkpoint.entry_id,
                metadata_entry_id: None,
            }),
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
        let _ = self
            .mito
            .handle_batch_open_requests(
                2,
                vec![
                    (metadata_region_id, open_metadata_region_request),
                    (data_region_id, open_data_region_request),
                ],
            )
            .await
            .context(BatchOpenMitoRegionSnafu {})?;

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
        physical_region_options: PhysicalRegionOptions,
    ) -> Result<Vec<RegionId>> {
        // load logical regions and physical column names
        let logical_regions = self
            .metadata_region
            .logical_regions(physical_region_id)
            .await?;
        common_telemetry::debug!(
            "Recover states for physical region {}, logical regions: {:?}",
            physical_region_id,
            logical_regions
        );
        let physical_columns = self
            .data_region
            .physical_columns(physical_region_id)
            .await?;
        let primary_key_encoding = self
            .mito
            .get_primary_key_encoding(physical_region_id)
            .context(PhysicalRegionNotFoundSnafu {
                region_id: physical_region_id,
            })?;

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
