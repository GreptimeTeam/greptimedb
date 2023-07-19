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

use snafu::ensure;

use crate::error::{RegionExistsSnafu, Result};
use crate::metadata::{RegionMetadataBuilder, INIT_REGION_VERSION};
use crate::region::opener::RegionOpener;
use crate::worker::request::CreateRequest;
use crate::worker::RegionWorkerLoop;

impl<S> RegionWorkerLoop<S> {
    pub(crate) async fn handle_create_request(&mut self, request: CreateRequest) -> Result<()> {
        // Checks whether the table exists.
        if self.regions.is_region_exists(request.region_id) {
            ensure!(
                request.create_if_not_exists,
                RegionExistsSnafu {
                    region_id: request.region_id,
                }
            );

            // Table already exists.
            return Ok(());
        }

        // Convert the request into a RegionMetadata and validate it.
        let mut builder = RegionMetadataBuilder::new(request.region_id, INIT_REGION_VERSION);
        for column in request.column_metadatas {
            builder.push_column_metadata(column);
        }
        builder.primary_key(request.primary_key);
        let metadata = builder.build()?;

        // Create a MitoRegion from the RegionMetadata.
        let region = RegionOpener::new(
            metadata,
            self.memtable_builder.clone(),
            self.object_store.clone(),
        )
        .region_dir(&request.region_dir);

        // Write manifest

        // Insert the MitoRegion into the RegionMap.
        unimplemented!()
    }
}
