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

use common_telemetry::{debug, info};
use snafu::ResultExt;
use store_api::region_engine::{RegionEngine, RegionRole};
use store_api::region_request::{AffectedRows, RegionCloseRequest, RegionRequest};
use store_api::storage::RegionId;

use crate::engine::MetricEngineInner;
use crate::error::{CloseMitoRegionSnafu, Result};
use crate::metrics::PHYSICAL_REGION_COUNT;
use crate::utils;

impl MetricEngineInner {
    pub async fn close_region(
        &self,
        region_id: RegionId,
        req: RegionCloseRequest,
    ) -> Result<AffectedRows> {
        let data_region_id = utils::to_data_region_id(region_id);
        if self
            .state
            .read()
            .unwrap()
            .exist_physical_region(data_region_id)
        {
            self.close_physical_region(data_region_id, req.flush_on_close)
                .await?;
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
            debug!("Closing a non-existent logical region {}", region_id);
            Ok(0)
        }
    }

    pub(crate) async fn close_physical_region(
        &self,
        region_id: RegionId,
        flush_on_close: bool,
    ) -> Result<AffectedRows> {
        let data_region_id = utils::to_data_region_id(region_id);
        let metadata_region_id = utils::to_metadata_region_id(region_id);

        // The engine reports a physical region by its data region, so once that is closed the
        // metadata region keeps no lease. A retry that finds the data region closed and the
        // metadata region a follower waives the requested flush of the metadata region. The role
        // is read before that close, so the waiver also holds for a metadata region promoted in
        // between. The close then releases the region's WAL only up to its flushed entry id, so
        // unflushed rows that are in a WAL are kept; a region without a WAL loses them, as on any
        // close of a region that cannot flush.
        let data_region_closed = !self.mito.is_region_exists(data_region_id);
        self.mito
            .handle_request(
                data_region_id,
                RegionRequest::Close(RegionCloseRequest { flush_on_close }),
            )
            .await
            .context(CloseMitoRegionSnafu { region_id })?;
        let skip_metadata_flush = flush_on_close
            && data_region_closed
            && self.mito.role(metadata_region_id) == Some(RegionRole::Follower);
        if skip_metadata_flush {
            info!(
                "Waiving the requested flush of follower metadata region {}, its data region is closed",
                metadata_region_id
            );
        }
        self.mito
            .handle_request(
                metadata_region_id,
                RegionRequest::Close(RegionCloseRequest {
                    flush_on_close: flush_on_close && !skip_metadata_flush,
                }),
            )
            .await
            .context(CloseMitoRegionSnafu { region_id })?;

        PHYSICAL_REGION_COUNT.dec();

        Ok(0)
    }
}

// Unit tests in engine.rs
