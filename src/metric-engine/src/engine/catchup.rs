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

use snafu::ResultExt;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{AffectedRows, RegionCatchupRequest, RegionRequest};
use store_api::storage::RegionId;

use crate::engine::MetricEngineInner;
use crate::error::{MitoCatchupOperationSnafu, Result, UnsupportedRegionRequestSnafu};
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
        let metadata_region_id = utils::to_metadata_region_id(region_id);
        // TODO(weny): improve the catchup, we can read the wal entries only once.
        self.mito
            .handle_request(
                metadata_region_id,
                RegionRequest::Catchup(RegionCatchupRequest {
                    set_writable: req.set_writable,
                    entry_id: None,
                }),
            )
            .await
            .context(MitoCatchupOperationSnafu)?;

        self.mito
            .handle_request(
                region_id,
                RegionRequest::Catchup(RegionCatchupRequest {
                    set_writable: req.set_writable,
                    entry_id: req.entry_id,
                }),
            )
            .await
            .context(MitoCatchupOperationSnafu)
            .map(|response| response.affected_rows)?;

        self.recover_states(region_id).await?;
        Ok(0)
    }
}
