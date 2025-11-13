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

use common_telemetry::info;
use store_api::logstore::LogStore;
use store_api::metadata::RegionMetadataBuilder;
use store_api::region_request::{AffectedRows, RegionCreateRequest};
use store_api::storage::RegionId;

use crate::error::Result;
use crate::region::opener::{RegionOpener, check_recovered_region};
use crate::worker::RegionWorkerLoop;

impl<S: LogStore> RegionWorkerLoop<S> {
    pub(crate) async fn handle_create_request(
        &mut self,
        region_id: RegionId,
        request: RegionCreateRequest,
    ) -> Result<AffectedRows> {
        // Checks whether the table exists.
        if let Some(region) = self.regions.get_region(region_id) {
            // Region already exists.
            check_recovered_region(
                &region.metadata(),
                region_id,
                &request.column_metadatas,
                &request.primary_key,
            )?;

            return Ok(0);
        }

        // Convert the request into a RegionMetadata and validate it.
        let mut builder = RegionMetadataBuilder::new(region_id);
        for column in request.column_metadatas {
            builder.push_column_metadata(column);
        }
        builder.primary_key(request.primary_key);
        if let Some(expr_json) = request.partition_expr_json.as_ref() {
            builder.partition_expr_json(Some(expr_json.clone()));
        }

        // Create a MitoRegion from the RegionMetadata.
        let region = RegionOpener::new(
            region_id,
            &request.table_dir,
            request.path_type,
            self.memtable_builder_provider.clone(),
            self.object_store_manager.clone(),
            self.purge_scheduler.clone(),
            self.puffin_manager_factory.clone(),
            self.intermediate_manager.clone(),
            self.time_provider.clone(),
            self.file_ref_manager.clone(),
            self.partition_expr_fetcher.clone(),
        )
        .metadata_builder(builder)
        .parse_options(request.options)?
        .cache(Some(self.cache_manager.clone()))
        .create_or_open(&self.config, &self.wal)
        .await?;

        info!(
            "A new region created, worker: {}, region: {:?}",
            self.id,
            region.metadata()
        );

        self.region_count.inc();

        // Insert the MitoRegion into the RegionMap.
        self.regions.insert_region(region);

        Ok(0)
    }
}
