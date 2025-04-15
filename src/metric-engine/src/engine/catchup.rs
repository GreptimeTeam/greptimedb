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

use common_telemetry::debug;
use snafu::{OptionExt, ResultExt};
use store_api::region_engine::RegionEngine;
use store_api::region_request::{AffectedRows, RegionCatchupRequest, RegionRequest};
use store_api::storage::RegionId;

use crate::engine::MetricEngineInner;
use crate::error::{
    MitoCatchupOperationSnafu, PhysicalRegionNotFoundSnafu, Result, UnsupportedRegionRequestSnafu,
};
use crate::utils;

impl MetricEngineInner {
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
                }),
            )
            .await
            .context(MitoCatchupOperationSnafu)
            .map(|response| response.affected_rows)?;

        let primary_key_encoding = self.mito.get_primary_key_encoding(data_region_id).context(
            PhysicalRegionNotFoundSnafu {
                region_id: data_region_id,
            },
        )?;
        self.recover_states(region_id, primary_key_encoding, physical_region_options)
            .await?;
        Ok(0)
    }
}
