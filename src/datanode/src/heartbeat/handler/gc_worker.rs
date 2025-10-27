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
use snafu::{OptionExt, ResultExt};
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

        let is_same_table = region_ids.windows(2).all(|w| {
            let t1 = w[0].table_id();
            let t2 = w[1].table_id();
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
        let region_id = *region_ids.first().with_context(|| UnexpectedSnafu {
            violated: "No region ids provided".to_string(),
        })?;

        let mito_config = mito_engine.mito_config();

        // Find the access layer from one of the regions that exists on this datanode
        let access_layer = region_ids
            .iter()
            .find_map(|rid| mito_engine.find_region(*rid))
            .with_context(|| InvalidGcArgsSnafu {
                msg: format!(
                    "None of the regions is on current datanode:{:?}",
                    region_ids
                ),
            })?
            .access_layer();

        let cache_manager = mito_engine.cache_manager();

        let gc_worker = LocalGcWorker::try_new(
            access_layer.clone(),
            Some(cache_manager),
            region_ids.into_iter().collect(),
            Default::default(),
            mito_config.clone().into(),
            file_ref_manifest.clone(),
            &mito_engine.gc_limiter(),
            full_file_listing,
        )
        .await
        .context(GcMitoEngineSnafu { region_id })?;

        Ok((region_id, gc_worker))
    }
}
