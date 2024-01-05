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

//! Close a metric region

use mito2::engine::MITO_ENGINE_NAME;
use object_store::util::join_dir;
use snafu::{OptionExt, ResultExt};
use store_api::metric_engine_consts::{
    DATA_REGION_SUBDIR, METADATA_REGION_SUBDIR, PHYSICAL_TABLE_METADATA_KEY,
};
use store_api::region_engine::RegionEngine;
use store_api::region_request::{
    AffectedRows, RegionCloseRequest, RegionOpenRequest, RegionRequest,
};
use store_api::storage::RegionId;

use super::MetricEngineInner;
use crate::error::{
    CloseMitoRegionSnafu, Error, LogicalRegionNotFoundSnafu, OpenMitoRegionSnafu,
    PhysicalRegionNotFoundSnafu, Result,
};
use crate::metrics::PHYSICAL_REGION_COUNT;
use crate::{metadata_region, utils};

impl MetricEngineInner {
    pub async fn close_region(
        &self,
        region_id: RegionId,
        _req: RegionCloseRequest,
    ) -> Result<AffectedRows> {
        let data_region_id = utils::to_data_region_id(region_id);
        if self
            .state
            .read()
            .unwrap()
            .physical_regions()
            .contains_key(&data_region_id)
        {
            self.close_physical_region(data_region_id).await?;
            self.state
                .write()
                .unwrap()
                .remove_physical_region(data_region_id)?;

            Ok(0)
        } else if self
            .state
            .read()
            .unwrap()
            .logical_regions()
            .contains_key(&region_id)
        {
            Ok(0)
        } else {
            Err(LogicalRegionNotFoundSnafu { region_id }.build())
        }
    }

    async fn close_physical_region(&self, region_id: RegionId) -> Result<AffectedRows> {
        let data_region_id = utils::to_data_region_id(region_id);
        let metadata_region_id = utils::to_metadata_region_id(region_id);

        self.mito
            .handle_request(data_region_id, RegionRequest::Close(RegionCloseRequest {}))
            .await
            .with_context(|_| CloseMitoRegionSnafu { region_id })?;
        self.mito
            .handle_request(
                metadata_region_id,
                RegionRequest::Close(RegionCloseRequest {}),
            )
            .await
            .with_context(|_| CloseMitoRegionSnafu { region_id })?;

        PHYSICAL_REGION_COUNT.dec();

        Ok(0)
    }
}

// Unit tests in engine.rs
