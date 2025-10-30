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

use common_error::ext::BoxedError;
use common_telemetry::debug;
use snafu::{OptionExt, ResultExt};
use store_api::region_engine::{BatchResponses, RegionEngine};
use store_api::region_request::{
    AffectedRows, RegionCatchupRequest, RegionRequest, ReplayCheckpoint,
};
use store_api::storage::RegionId;

use crate::engine::MetricEngineInner;
use crate::error::{
    BatchCatchupMitoRegionSnafu, MitoCatchupOperationSnafu, PhysicalRegionNotFoundSnafu, Result,
    UnsupportedRegionRequestSnafu,
};
use crate::utils;

impl MetricEngineInner {
    pub async fn handle_batch_catchup_requests(
        &self,
        parallelism: usize,
        requests: Vec<(RegionId, RegionCatchupRequest)>,
    ) -> Result<BatchResponses> {
        let mut all_requests = Vec::with_capacity(requests.len() * 2);
        let mut physical_region_options_list = Vec::with_capacity(requests.len());

        for (region_id, req) in requests {
            let metadata_region_id = utils::to_metadata_region_id(region_id);
            let data_region_id = utils::to_data_region_id(region_id);

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
            physical_region_options_list.push((data_region_id, physical_region_options));
            all_requests.push((
                metadata_region_id,
                RegionCatchupRequest {
                    set_writable: req.set_writable,
                    entry_id: req.metadata_entry_id,
                    metadata_entry_id: None,
                    location_id: req.location_id,
                    checkpoint: req.checkpoint.map(|c| ReplayCheckpoint {
                        entry_id: c.metadata_entry_id.unwrap_or_default(),
                        metadata_entry_id: None,
                    }),
                },
            ));
            all_requests.push((
                data_region_id,
                RegionCatchupRequest {
                    set_writable: req.set_writable,
                    entry_id: req.entry_id,
                    metadata_entry_id: None,
                    location_id: req.location_id,
                    checkpoint: req.checkpoint.map(|c| ReplayCheckpoint {
                        entry_id: c.entry_id,
                        metadata_entry_id: None,
                    }),
                },
            ));
        }

        let mut results = self
            .mito
            .handle_batch_catchup_requests(parallelism, all_requests)
            .await
            .context(BatchCatchupMitoRegionSnafu {})?
            .into_iter()
            .collect::<HashMap<_, _>>();

        let mut responses = Vec::with_capacity(physical_region_options_list.len());
        for (physical_region_id, physical_region_options) in physical_region_options_list {
            let metadata_region_id = utils::to_metadata_region_id(physical_region_id);
            let data_region_id = utils::to_data_region_id(physical_region_id);
            let metadata_region_result = results.remove(&metadata_region_id);
            let data_region_result = results.remove(&data_region_id);

            // Pass the optional `metadata_region_result` and `data_region_result` to
            // `recover_physical_region_with_results`. This function handles errors for each
            // catchup physical region request, allowing the process to continue with the
            // remaining regions even if some requests fail.
            let response = self
                .recover_physical_region_with_results(
                    metadata_region_result,
                    data_region_result,
                    physical_region_id,
                    physical_region_options,
                    // Note: The region is not closed on failure here to prevent the region server from incorrectly regarding it as open.
                    false,
                )
                .await
                .map_err(BoxedError::new);
            responses.push((physical_region_id, response));
        }

        Ok(responses)
    }

    pub async fn catchup_region(
        &self,
        region_id: RegionId,
        req: RegionCatchupRequest,
    ) -> Result<AffectedRows> {
        if !self.is_physical_region(region_id) {
            return UnsupportedRegionRequestSnafu {
                request: RegionRequest::Catchup(req),
            }
            .fail();
        }
        let data_region_id = utils::to_data_region_id(region_id);
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

        let metadata_region_id = utils::to_metadata_region_id(region_id);
        // TODO(weny): improve the catchup, we can read the wal entries only once.
        debug!("Catchup metadata region {metadata_region_id}");
        self.mito
            .handle_request(
                metadata_region_id,
                RegionRequest::Catchup(RegionCatchupRequest {
                    set_writable: req.set_writable,
                    entry_id: req.metadata_entry_id,
                    metadata_entry_id: None,
                    location_id: req.location_id,
                    checkpoint: req.checkpoint.map(|c| ReplayCheckpoint {
                        entry_id: c.metadata_entry_id.unwrap_or_default(),
                        metadata_entry_id: None,
                    }),
                }),
            )
            .await
            .context(MitoCatchupOperationSnafu)?;

        debug!("Catchup data region {data_region_id}");
        self.mito
            .handle_request(
                data_region_id,
                RegionRequest::Catchup(RegionCatchupRequest {
                    set_writable: req.set_writable,
                    entry_id: req.entry_id,
                    metadata_entry_id: None,
                    location_id: req.location_id,
                    checkpoint: req.checkpoint.map(|c| ReplayCheckpoint {
                        entry_id: c.entry_id,
                        metadata_entry_id: None,
                    }),
                }),
            )
            .await
            .context(MitoCatchupOperationSnafu)
            .map(|response| response.affected_rows)?;

        self.recover_states(region_id, physical_region_options)
            .await?;
        Ok(0)
    }
}
