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
use common_telemetry::{debug, warn};
use mito2::gc::LocalGcWorker;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::storage::{FileRefsManifest, RegionId};

use crate::error::{GcMitoEngineSnafu, InvalidGcArgsSnafu, Result, UnexpectedSnafu};
use crate::heartbeat::handler::{HandlerContext, InstructionHandler};

pub struct GcRegionsHandler;

#[async_trait::async_trait]
impl InstructionHandler for GcRegionsHandler {
    type Instruction = GcRegions;

    async fn handle(
        &self,
        ctx: &HandlerContext,
        gc_regions: Self::Instruction,
    ) -> Option<InstructionReply> {
        let region_ids = gc_regions.regions.clone();
        debug!("Received gc regions instruction: {:?}", region_ids);

        let (region_id, gc_worker) = match self
            .create_gc_worker(
                ctx,
                region_ids,
                &gc_regions.file_refs_manifest,
                gc_regions.full_file_listing,
            )
            .await
        {
            Ok(worker) => worker,
            Err(e) => {
                return Some(InstructionReply::GcRegions(GcRegionsReply {
                    result: Err(format!("Failed to create GC worker: {}", e)),
                }));
            }
        };

        let register_result = ctx
            .gc_tasks
            .try_register(
                region_id,
                Box::pin(async move {
                    debug!("Starting gc worker for region {}", region_id);
                    let report = gc_worker
                        .run()
                        .await
                        .context(GcMitoEngineSnafu { region_id })?;
                    debug!("Gc worker for region {} finished", region_id);
                    Ok(report)
                }),
            )
            .await;
        if register_result.is_busy() {
            warn!("Another gc task is running for the region: {region_id}");
            return Some(InstructionReply::GcRegions(GcRegionsReply {
                result: Err(format!(
                    "Another gc task is running for the region: {region_id}"
                )),
            }));
        }
        let mut watcher = register_result.into_watcher();
        let result = ctx.gc_tasks.wait_until_finish(&mut watcher).await;
        match result {
            Ok(report) => Some(InstructionReply::GcRegions(GcRegionsReply {
                result: Ok(report),
            })),
            Err(err) => Some(InstructionReply::GcRegions(GcRegionsReply {
                result: Err(format!("{err:?}")),
            })),
        }
    }
}

impl GcRegionsHandler {
    /// Create a GC worker for the given region IDs.
    /// Return the first region ID(after sort by given region id) and the GC worker.
    async fn create_gc_worker(
        &self,
        ctx: &HandlerContext,
        mut region_ids: Vec<RegionId>,
        file_ref_manifest: &FileRefsManifest,
        full_file_listing: bool,
    ) -> Result<(RegionId, LocalGcWorker)> {
        // always use the smallest region id on datanode as the target region id
        region_ids.sort_by_key(|r| r.region_number());

        let mito_engine = ctx
            .region_server
            .mito_engine()
            .with_context(|| UnexpectedSnafu {
                violated: "MitoEngine not found".to_string(),
            })?;

        let region_id = *region_ids.first().with_context(|| InvalidGcArgsSnafu {
            msg: "No region ids provided".to_string(),
        })?;

        // also need to ensure all regions are on this datanode
        ensure!(
            region_ids
                .iter()
                .all(|rid| mito_engine.find_region(*rid).is_some()),
            InvalidGcArgsSnafu {
                msg: format!(
                    "Some regions are not on current datanode:{:?}",
                    region_ids
                        .iter()
                        .filter(|rid| mito_engine.find_region(**rid).is_none())
                        .collect::<Vec<_>>()
                ),
            }
        );

        // Find the access layer from one of the regions that exists on this datanode
        let access_layer = mito_engine
            .find_region(region_id)
            .with_context(|| InvalidGcArgsSnafu {
                msg: format!(
                    "None of the regions is on current datanode:{:?}",
                    region_ids
                ),
            })?
            .access_layer();

        // if region happen to be dropped before this but after gc scheduler send gc instr,
        // need to deal with it properly(it is ok for region to be dropped after GC worker started)
        // region not found here can only be drop table/database case, since region migration is prevented by lock in gc procedure
        // TODO(discord9): add integration test for this drop case
        let mito_regions = region_ids
            .iter()
            .filter_map(|rid| mito_engine.find_region(*rid).map(|r| (*rid, r)))
            .collect();

        let cache_manager = mito_engine.cache_manager();

        let gc_worker = LocalGcWorker::try_new(
            access_layer.clone(),
            Some(cache_manager),
            mito_regions,
            mito_engine.mito_config().gc.clone(),
            file_ref_manifest.clone(),
            &mito_engine.gc_limiter(),
            full_file_listing,
        )
        .await
        .context(GcMitoEngineSnafu { region_id })?;

        Ok((region_id, gc_worker))
    }
}
