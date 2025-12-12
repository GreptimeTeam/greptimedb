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
use std::time::Instant;

use common_error::ext::BoxedError;
use common_telemetry::info;
use futures::future::try_join_all;
use partition::expr::PartitionExpr;
use snafu::{OptionExt, ResultExt};
use store_api::storage::RegionId;

use crate::error::{FetchManifestsSnafu, InvalidRequestSnafu, MissingManifestSnafu, Result};
use crate::manifest::action::RegionManifest;
use crate::region::{MitoRegionRef, RegionMetadataLoader};
use crate::remap_manifest::RemapManifest;
use crate::request::RemapManifestsRequest;
use crate::sst::location::region_dir_from_table_dir;
use crate::worker::RegionWorkerLoop;

impl<S> RegionWorkerLoop<S> {
    pub(crate) fn handle_remap_manifests_request(&mut self, request: RemapManifestsRequest) {
        let region_id = request.region_id;
        let sender = request.sender;
        let region = match self.regions.staging_region(region_id) {
            Ok(region) => region,
            Err(e) => {
                let _ = sender.send(Err(e));
                return;
            }
        };

        let same_table = request
            .input_regions
            .iter()
            .map(|r| r.table_id())
            .all(|t| t == region_id.table_id());

        if !same_table {
            let _ = sender.send(
                InvalidRequestSnafu {
                    region_id,
                    reason: "Input regions must be from the same table",
                }
                .fail(),
            );
            return;
        }

        let region_metadata_loader =
            RegionMetadataLoader::new(self.config.clone(), self.object_store_manager.clone());
        common_runtime::spawn_global(async move {
            let result = Self::fetch_and_remap_manifests(
                region,
                region_metadata_loader,
                request.input_regions,
                request.new_partition_exprs,
                request.region_mapping,
            )
            .await;

            let _ = sender.send(result);
        });
    }

    async fn fetch_and_remap_manifests(
        region: MitoRegionRef,
        region_metadata_loader: RegionMetadataLoader,
        input_regions: Vec<RegionId>,
        new_partition_exprs: HashMap<RegionId, PartitionExpr>,
        region_mapping: HashMap<RegionId, Vec<RegionId>>,
    ) -> Result<HashMap<RegionId, RegionManifest>> {
        let mut tasks = Vec::with_capacity(input_regions.len());
        let region_options = region.version().options.clone();
        let table_dir = region.table_dir();
        let path_type = region.path_type();
        let now = Instant::now();
        for input_region in &input_regions {
            let region_dir = region_dir_from_table_dir(table_dir, *input_region, path_type);
            let storage = region_options.storage.clone();
            let moved_region_metadata_loader = region_metadata_loader.clone();
            tasks.push(async move {
                moved_region_metadata_loader
                    .load_manifest(&region_dir, &storage)
                    .await
            });
        }

        let results = try_join_all(tasks)
            .await
            .map_err(BoxedError::new)
            .context(FetchManifestsSnafu)?;
        let manifests = results
            .into_iter()
            .zip(input_regions)
            .map(|(manifest_res, region_id)| {
                let manifest = manifest_res.context(MissingManifestSnafu { region_id })?;
                Ok((region_id, (*manifest).clone()))
            })
            .collect::<Result<HashMap<_, _>>>()?;
        let mut mapper = RemapManifest::new(manifests, new_partition_exprs, region_mapping);
        let remap_result = mapper.remap_manifests()?;
        info!(
            "Remap manifests cost: {:?}, region: {}",
            now.elapsed(),
            region.region_id
        );

        Ok(remap_result.new_manifests)
    }
}
