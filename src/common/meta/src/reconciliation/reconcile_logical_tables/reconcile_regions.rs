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

use std::any::Any;
use std::collections::HashMap;

use api::v1::region::{CreateRequests, RegionRequest, RegionRequestHeader, region_request};
use common_procedure::{Context as ProcedureContext, Status};
use common_telemetry::info;
use common_telemetry::tracing_context::TracingContext;
use futures::future;
use serde::{Deserialize, Serialize};
use store_api::storage::{RegionId, RegionNumber, TableId};
use table::metadata::RawTableInfo;

use crate::ddl::utils::{add_peer_context_if_needed, region_storage_path};
use crate::ddl::{CreateRequestBuilder, build_template_from_raw_table_info};
use crate::error::Result;
use crate::reconciliation::reconcile_logical_tables::update_table_infos::UpdateTableInfos;
use crate::reconciliation::reconcile_logical_tables::{ReconcileLogicalTablesContext, State};
use crate::rpc::router::{find_leaders, region_distribution};

#[derive(Debug, Serialize, Deserialize)]
pub struct ReconcileRegions;

#[async_trait::async_trait]
#[typetag::serde]
impl State for ReconcileRegions {
    async fn next(
        &mut self,
        ctx: &mut ReconcileLogicalTablesContext,
        _procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        if ctx.persistent_ctx.create_tables.is_empty() {
            return Ok((Box::new(UpdateTableInfos), Status::executing(false)));
        }

        // Safety: previous steps ensure the physical table route is set.
        let region_routes = &ctx
            .persistent_ctx
            .physical_table_route
            .as_ref()
            .unwrap()
            .region_routes;

        let region_distribution = region_distribution(region_routes);
        let leaders = find_leaders(region_routes)
            .into_iter()
            .map(|p| (p.id, p))
            .collect::<HashMap<_, _>>();
        let region_rpc = ctx.region_rpc.clone();
        let mut create_table_tasks = Vec::with_capacity(leaders.len());
        for (datanode_id, region_role_set) in region_distribution {
            if region_role_set.leader_regions.is_empty() {
                continue;
            }
            // Safety: It contains all leaders in the region routes.
            let peer = leaders.get(&datanode_id).unwrap().clone();
            let request = self.make_request(&region_role_set.leader_regions, ctx)?;
            let region_rpc = region_rpc.clone();
            create_table_tasks.push(async move {
                region_rpc
                    .handle_region(&peer, request)
                    .await
                    .map_err(add_peer_context_if_needed(peer))
            });
        }

        future::join_all(create_table_tasks)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        let table_id = ctx.table_id();
        let table_name = ctx.table_name();
        info!(
            "Reconciled regions for logical tables: {:?}, physical table: {}, table_id: {}",
            ctx.persistent_ctx
                .create_tables
                .iter()
                .map(|(table_id, _)| table_id)
                .collect::<Vec<_>>(),
            table_id,
            table_name
        );
        ctx.persistent_ctx.create_tables.clear();
        return Ok((Box::new(UpdateTableInfos), Status::executing(true)));
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl ReconcileRegions {
    fn make_request(
        &self,
        region_numbers: &[u32],
        ctx: &ReconcileLogicalTablesContext,
    ) -> Result<RegionRequest> {
        let physical_table_id = ctx.table_id();
        let table_name = ctx.table_name();
        let create_tables = &ctx.persistent_ctx.create_tables;

        let mut requests = Vec::with_capacity(region_numbers.len() * create_tables.len());

        for (table_id, table_info) in create_tables {
            let request_builder =
                create_region_request_from_raw_table_info(table_info, physical_table_id)?;
            let storage_path =
                region_storage_path(&table_name.catalog_name, &table_name.schema_name);
            let partition_exprs = prepare_partition_exprs(ctx, *table_id);

            for region_number in region_numbers {
                let region_id = RegionId::new(*table_id, *region_number);

                let one_region_request = request_builder.build_one(
                    region_id,
                    storage_path.clone(),
                    &HashMap::new(),
                    &partition_exprs,
                );
                requests.push(one_region_request);
            }
        }

        Ok(RegionRequest {
            header: Some(RegionRequestHeader {
                tracing_context: TracingContext::from_current_span().to_w3c(),
                ..Default::default()
            }),
            body: Some(region_request::Body::Creates(CreateRequests { requests })),
        })
    }
}

/// Creates a region request builder from a raw table info.
///
/// Note: This function is primarily intended for creating logical tables or allocating placeholder regions.
fn create_region_request_from_raw_table_info(
    raw_table_info: &RawTableInfo,
    physical_table_id: TableId,
) -> Result<CreateRequestBuilder> {
    let template = build_template_from_raw_table_info(raw_table_info)?;
    Ok(CreateRequestBuilder::new(template, Some(physical_table_id)))
}

fn prepare_partition_exprs(
    ctx: &ReconcileLogicalTablesContext,
    table_id: TableId,
) -> HashMap<RegionNumber, String> {
    ctx.persistent_ctx
        .physical_table_route
        .as_ref()
        .map(|r| {
            r.region_routes
                .iter()
                .filter(|r| r.region.id.table_id() == table_id)
                .map(|r| (r.region.id.region_number(), r.region.partition_expr()))
                .collect::<HashMap<_, _>>()
        })
        .unwrap_or_default()
}
