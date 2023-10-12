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

//! Handling create request.

use std::sync::Arc;

use common_query::Output;
use common_telemetry::info;
use metrics::increment_gauge;
use snafu::ResultExt;
use store_api::logstore::LogStore;
use store_api::metadata::RegionMetadataBuilder;
use store_api::region_request::RegionCreateRequest;
use store_api::storage::RegionId;

use crate::error::{InvalidMetadataSnafu, Result};
use crate::metrics::REGION_COUNT;
use crate::region::opener::{check_recovered_region, RegionOpener};
use crate::worker::RegionWorkerLoop;

impl<S: LogStore> RegionWorkerLoop<S> {
    pub(crate) async fn handle_create_request(
        &mut self,
        region_id: RegionId,
        request: RegionCreateRequest,
    ) -> Result<Output> {
        // Checks whether the table exists.
        if let Some(region) = self.regions.get_region(region_id) {
            // Region already exists.
            check_recovered_region(
                &region.metadata(),
                region_id,
                &request.column_metadatas,
                &request.primary_key,
            )?;

            return Ok(Output::AffectedRows(0));
        }

        // Convert the request into a RegionMetadata and validate it.
        let mut builder = RegionMetadataBuilder::new(region_id);
        for column in request.column_metadatas {
            builder.push_column_metadata(column);
        }
        builder.primary_key(request.primary_key);
        let metadata = builder.build().context(InvalidMetadataSnafu)?;

        // Create a MitoRegion from the RegionMetadata.
        let region = RegionOpener::new(
            region_id,
            &request.region_dir,
            self.memtable_builder.clone(),
            self.object_store.clone(),
            self.scheduler.clone(),
        )
        .metadata(metadata)
        .options(request.options)
        .cache(Some(self.cache_manager.clone()))
        .create_or_open(&self.config, &self.wal)
        .await?;

        info!("A new region created, region: {:?}", region.metadata());

        increment_gauge!(REGION_COUNT, 1.0);

        // Insert the MitoRegion into the RegionMap.
        self.regions.insert_region(Arc::new(region));

        Ok(Output::AffectedRows(0))
    }
}
