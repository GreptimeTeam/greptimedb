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

use common_meta::instruction::{InstructionReply, SimpleReply};
use common_telemetry::warn;
use futures::future::BoxFuture;
use mito2::engine::MitoEngine;
use mito2::gc::LocalGcWorker;
use snafu::{OptionExt, ResultExt};
use store_api::storage::RegionId;

use crate::error::{GcMitoEngineSnafu, RegionNotFoundSnafu, Result, UnexpectedSnafu};
use crate::heartbeat::handler::HandlerContext;

impl HandlerContext {
    pub(crate) fn handle_gc_regions_instruction(
        self,
        region_ids: Vec<RegionId>,
    ) -> BoxFuture<'static, Option<InstructionReply>> {
        Box::pin(async move {
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
                return Some(InstructionReply::GcRegions(SimpleReply {
                    result: false,
                    error: Some(format!(
                        "Regions to GC should belong to the same table, found: {:?}",
                        region_ids
                    )),
                }));
            }

            let (region_id, gc_worker) = match self.create_gc_worker(region_ids).await {
                Ok(worker) => worker,
                Err(e) => {
                    return Some(InstructionReply::GcRegions(SimpleReply {
                        result: false,
                        error: Some(format!("Failed to create GC worker: {}", e)),
                    }));
                }
            };

            let register_result = self
                .gc_tasks
                .try_register(
                    region_id,
                    Box::pin(async move {
                        gc_worker
                            .run()
                            .await
                            .context(GcMitoEngineSnafu { region_id })?;
                        Ok(())
                    }),
                )
                .await;
            if register_result.is_busy() {
                warn!("Another gc task is running for the region: {region_id}");
            }
            let mut watcher = register_result.into_watcher();
            let result = self.flush_tasks.wait_until_finish(&mut watcher).await;
            match result {
                Ok(()) => Some(InstructionReply::GcRegions(SimpleReply {
                    result: true,
                    error: None,
                })),
                Err(err) => Some(InstructionReply::GcRegions(SimpleReply {
                    result: false,
                    error: Some(format!("{err:?}")),
                })),
            }
        })
    }

    async fn create_gc_worker(
        &self,
        mut region_ids: Vec<RegionId>,
    ) -> Result<(RegionId, LocalGcWorker)> {
        // always use the smallest region id on datanode as the target region id
        region_ids.sort_by_key(|r| r.region_number());
        let (region_id, mito_engine) = self.find_engine_for_regions(&region_ids)?;

        let mito_config = mito_engine.mito_config();
        let region = mito_engine
            .find_region(region_id)
            .context(RegionNotFoundSnafu { region_id })?;
        let access_layer = region.access_layer();

        let gc_worker = LocalGcWorker::try_new(
            access_layer.clone(),
            region_ids,
            Default::default(),
            mito_config.clone(),
        )
        .await
        .context(GcMitoEngineSnafu { region_id })?;

        Ok((region_id, gc_worker))
    }

    fn find_engine_for_regions(&self, region_ids: &[RegionId]) -> Result<(RegionId, MitoEngine)> {
        for region_id in region_ids {
            let engine = self.region_server.find_engine(*region_id)?;
            let Some(engine) = engine else {
                continue;
            };
            let engine = engine
                .as_any()
                .downcast_ref::<MitoEngine>()
                .context(UnexpectedSnafu {
                    violated: format!(
                        "Expected MitoEngine, found: {:?}",
                        std::any::type_name_of_val(engine.as_ref())
                    ),
                })?
                .clone();
            return Ok((*region_id, engine));
        }
        UnexpectedSnafu {
            violated: format!(
                "No MitoEngine found for regions on current datanode: {:?}",
                region_ids
            ),
        }
        .fail()
    }
}
