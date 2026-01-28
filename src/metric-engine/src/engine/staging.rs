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

use common_base::AffectedRows;
use snafu::ResultExt;
use store_api::region_engine::RegionEngine;
use store_api::region_request::RegionRequest;
use store_api::storage::RegionId;

use crate::engine::MetricEngine;
use crate::error::{MitoEnterStagingOperationSnafu, Result};

impl MetricEngine {
    /// Handles the enter staging request for the given region.
    pub(crate) async fn handle_enter_staging_request(
        &self,
        region_id: RegionId,
        request: RegionRequest,
    ) -> Result<AffectedRows> {
        // We don't need to enter staging for metadata region.
        // Callers should pass the data region id here; metadata regions stay unchanged.
        self.inner
            .mito
            .handle_request(region_id, request)
            .await
            .context(MitoEnterStagingOperationSnafu)
            .map(|response| response.affected_rows)
    }
}
