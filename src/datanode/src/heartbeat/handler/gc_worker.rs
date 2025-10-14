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

use common_meta::instruction::{GcRegions, GcRegionsReply, InstructionReply};
use common_telemetry::{info, warn};
use mito2::gc::LocalGcWorker;
use snafu::{OptionExt, ResultExt};
use store_api::storage::{FileRefsManifest, RegionId};

use crate::error::{GcMitoEngineSnafu, RegionNotFoundSnafu, Result, UnexpectedSnafu};
use crate::heartbeat::handler::HandlerContext;

impl HandlerContext {
    pub(crate) async fn handle_gc_regions_instruction(
        self,
        gc_regions: GcRegions,
    ) -> Option<InstructionReply> {
        let region_ids = gc_regions.regions.clone();
        info!("Received gc regions instruction: {:?}", region_ids);

        let mut table_id = None;
        let is_same_table = region_ids.windows(2).all(|w| {
            let t1 = w[0].table_id();
            let t2 = w[1].table_id();
            if table_id.is_none() {
                table_id = Some(t1);
            }
            t1 == t2
        });
        if !is_same_table {
            return Some(InstructionReply::GcRegions(GcRegionsReply {
                result: Err(format!(
                    "Regions to GC should belong to the same table, found: {:?}",
                    region_ids
                )),
            }));
        }

        let (region_id, gc_worker) = match self
            .create_gc_worker(region_ids, &gc_regions.file_refs_manifest)
            .await
        {
            Ok(worker) => worker,
            Err(e) => {
                return Some(InstructionReply::GcRegions(GcRegionsReply {
                    result: Err(format!("Failed to create GC worker: {}", e)),
                }));
            }
        };

        let register_result = self
            .gc_tasks
            .try_register(
                region_id,
                Box::pin(async move {
                    info!("Starting gc worker for region {}", region_id);
                    let report = gc_worker
                        .run()
                        .await
                        .context(GcMitoEngineSnafu { region_id })?;
                    info!("Gc worker for region {} finished", region_id);
                    Ok(report)
                }),
            )
            .await;
        if register_result.is_busy() {
            warn!("Another gc task is running for the region: {region_id}");
        }
        let mut watcher = register_result.into_watcher();
        let result = self.gc_tasks.wait_until_finish(&mut watcher).await;
        match result {
            Ok(report) => Some(InstructionReply::GcRegions(GcRegionsReply {
                result: Ok(report),
            })),
            Err(err) => Some(InstructionReply::GcRegions(GcRegionsReply {
                result: Err(format!("{err:?}")),
            })),
        }
    }

    async fn create_gc_worker(
        &self,
        mut region_ids: Vec<RegionId>,
        file_ref_manifest: &FileRefsManifest,
    ) -> Result<(RegionId, LocalGcWorker)> {
        // always use the smallest region id on datanode as the target region id
        region_ids.sort_by_key(|r| r.region_number());
        let mito_engine = self.region_server.mito_engine().context(UnexpectedSnafu {
            violated: "MitoEngine not found".to_string(),
        })?;
        let region_id = *region_ids.first().context(UnexpectedSnafu {
            violated: "No region ids provided".to_string(),
        })?;

        let mito_config = mito_engine.mito_config();
        let region = mito_engine
            .find_region(region_id)
            .context(RegionNotFoundSnafu { region_id })?;
        let access_layer = region.access_layer();

        let cache_manager = mito_engine.cache_manager();

        let gc_worker = LocalGcWorker::try_new(
            access_layer.clone(),
            Some(cache_manager),
            region_ids.into_iter().collect(),
            Default::default(),
            mito_config.clone().into(),
            file_ref_manifest.clone(),
            &mito_engine.gc_limiter(),
        )
        .await
        .context(GcMitoEngineSnafu { region_id })?;

        Ok((region_id, gc_worker))
    }
}
