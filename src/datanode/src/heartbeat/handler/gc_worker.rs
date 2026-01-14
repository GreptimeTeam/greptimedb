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

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use common_meta::instruction::{GcRegions, GcRegionsReply, InstructionReply};
use common_meta::key::table_info::TableInfoManager;
use common_telemetry::{debug, warn};
use mito2::access_layer::{AccessLayer, AccessLayerRef};
use mito2::engine::MitoEngine;
use mito2::gc::LocalGcWorker;
use mito2::region::MitoRegionRef;
use snafu::{OptionExt, ResultExt};
use store_api::path_utils::table_dir;
use store_api::region_request::PathType;
use store_api::storage::{FileRefsManifest, GcReport, RegionId};
use table::requests::STORAGE_KEY;

use crate::error::{GcMitoEngineSnafu, GetMetadataSnafu, Result, UnexpectedSnafu};
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

        if region_ids.is_empty() {
            return Some(InstructionReply::GcRegions(GcRegionsReply {
                result: Ok(GcReport::default()),
            }));
        }

        // Always use the smallest region id on datanode as the target region id for task tracker
        let mut sorted_region_ids = gc_regions.regions.clone();
        sorted_region_ids.sort_by_key(|r| r.region_number());
        let target_region_id = sorted_region_ids[0];

        // Group regions by table_id
        let mut table_to_regions: HashMap<u32, Vec<RegionId>> = HashMap::new();
        for rid in region_ids {
            table_to_regions
                .entry(rid.table_id())
                .or_default()
                .push(rid);
        }

        let file_refs_manifest = gc_regions.file_refs_manifest.clone();
        let full_file_listing = gc_regions.full_file_listing;

        let ctx_clone = ctx.clone();
        let register_result = ctx
            .gc_tasks
            .try_register(
                target_region_id,
                Box::pin(async move {
                    let mut reports = Vec::with_capacity(table_to_regions.len());
                    for (table_id, regions) in table_to_regions {
                        debug!(
                            "Starting gc worker for table {}, regions: {:?}",
                            table_id, regions
                        );
                        let gc_worker = GcRegionsHandler::create_gc_worker(
                            &ctx_clone,
                            table_id,
                            regions,
                            &file_refs_manifest,
                            full_file_listing,
                        )
                        .await?;

                        let report = gc_worker.run().await.context(GcMitoEngineSnafu {
                            region_id: target_region_id,
                        })?;
                        debug!(
                            "Gc worker for table {} finished, report: {:?}",
                            table_id, report
                        );
                        reports.push(report);
                    }

                    // Merge reports
                    let mut merged_report = GcReport::default();
                    for report in reports {
                        merged_report
                            .deleted_files
                            .extend(report.deleted_files.into_iter());
                    }
                    Ok(merged_report)
                }),
            )
            .await;

        if register_result.is_busy() {
            warn!("Another gc task is running for the region: {target_region_id}");
            return Some(InstructionReply::GcRegions(GcRegionsReply {
                result: Err(format!(
                    "Another gc task is running for the region: {target_region_id}"
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
    /// Create a GC worker for the given table and region IDs.
    async fn create_gc_worker(
        ctx: &HandlerContext,
        table_id: u32,
        region_ids: Vec<RegionId>,
        file_ref_manifest: &FileRefsManifest,
        full_file_listing: bool,
    ) -> Result<LocalGcWorker> {
        let mito_engine = ctx
            .region_server
            .mito_engine()
            .with_context(|| UnexpectedSnafu {
                violated: "MitoEngine not found".to_string(),
            })?;

        let (access_layer, mito_regions) =
            Self::get_access_layer(ctx, &mito_engine, table_id, &region_ids).await?;

        let cache_manager = mito_engine.cache_manager();

        let gc_worker = LocalGcWorker::try_new(
            access_layer,
            Some(cache_manager),
            mito_regions,
            mito_engine.mito_config().gc.clone(),
            file_ref_manifest.clone(),
            &mito_engine.gc_limiter(),
            full_file_listing,
        )
        .await
        .context(GcMitoEngineSnafu {
            region_id: region_ids[0],
        })?;

        Ok(gc_worker)
    }

    /// Get the access layer for the given table and region IDs.
    /// It also returns the mito regions if they are found in the engine.
    async fn get_access_layer(
        ctx: &HandlerContext,
        mito_engine: &MitoEngine,
        table_id: u32,
        region_ids: &[RegionId],
    ) -> Result<(AccessLayerRef, BTreeMap<RegionId, Option<MitoRegionRef>>)> {
        // 1. Try to find an active region for this table to reuse AccessLayer
        let mut access_layer = None;
        let mut mito_regions = BTreeMap::new();

        for rid in region_ids {
            let region = mito_engine.find_region(*rid);
            if access_layer.is_none()
                && let Some(r) = &region
            {
                access_layer = Some(r.access_layer());
            }
            mito_regions.insert(*rid, region);
        }

        // 2. If no active region in the batch, try to find ANY active region of this table
        if access_layer.is_none() {
            for region in mito_engine.regions() {
                if region.region_id().table_id() == table_id {
                    access_layer = Some(region.access_layer());
                    break;
                }
            }
        }

        // 3. Fallback to manual construction
        let access_layer = if let Some(al) = access_layer {
            al
        } else {
            Self::construct_access_layer(ctx, mito_engine, table_id).await?
        };

        Ok((access_layer, mito_regions))
    }

    /// Manually construct an access layer from table metadata.
    async fn construct_access_layer(
        ctx: &HandlerContext,
        mito_engine: &MitoEngine,
        table_id: u32,
    ) -> Result<AccessLayerRef> {
        let table_info_manager = TableInfoManager::new(ctx.kv_backend.clone());
        let table_info_value = table_info_manager
            .get(table_id)
            .await
            .context(GetMetadataSnafu)?
            .with_context(|| UnexpectedSnafu {
                violated: format!("Table metadata not found for table {}", table_id),
            })?;

        let table_dir = table_dir(&table_info_value.region_storage_path(), table_id);
        let storage_name = table_info_value
            .table_info
            .meta
            .options
            .extra_options
            .get(STORAGE_KEY);

        let object_store = if let Some(name) = storage_name {
            mito_engine
                .object_store_manager()
                .find(name)
                .cloned()
                .with_context(|| UnexpectedSnafu {
                    violated: format!("Object store {} not found", name),
                })?
        } else {
            mito_engine
                .object_store_manager()
                .default_object_store()
                .clone()
        };

        Ok(Arc::new(AccessLayer::new(
            table_dir,
            PathType::Bare,
            object_store,
            mito_engine.puffin_manager_factory().clone(),
            mito_engine.intermediate_manager().clone(),
        )))
    }
}
