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
use table::metadata::TableInfo;

use crate::ddl::utils::region_metadata_lister::RegionMetadataLister;
use crate::ddl::utils::{add_peer_context_if_needed, region_storage_path};
use crate::ddl::{CreateRequestBuilder, build_template_from_raw_table_info};
use crate::error::{self, Result};
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
        ctx.volatile_ctx
            .result_summary
            .begin_region_reconciliation();
        if ctx.persistent_ctx.create_tables.is_empty() {
            ctx.volatile_ctx
                .result_summary
                .mark_regions_reconciled(0, 0);
            return Ok((Box::new(UpdateTableInfos), Status::executing(false)));
        }

        // Safety: previous steps ensure the physical table route is set.
        let region_routes = ctx
            .persistent_ctx
            .physical_table_route
            .as_ref()
            .unwrap()
            .region_routes
            .clone();

        let missing_regions_by_table = self.missing_regions_by_table(ctx, &region_routes).await?;
        let mut missing_tables_by_region = HashMap::<RegionNumber, Vec<TableId>>::new();
        for (table_id, region_numbers) in &missing_regions_by_table {
            for region_number in region_numbers {
                missing_tables_by_region
                    .entry(*region_number)
                    .or_default()
                    .push(*table_id);
            }
        }

        let region_distribution = region_distribution(&region_routes);
        let leaders = find_leaders(&region_routes)
            .into_iter()
            .map(|p| (p.id, p))
            .collect::<HashMap<_, _>>();
        let mut create_table_tasks = Vec::with_capacity(leaders.len());
        for (datanode_id, region_role_set) in region_distribution {
            if region_role_set.leader_regions.is_empty() {
                continue;
            }
            // Safety: It contains all leaders in the region routes.
            let peer = leaders.get(&datanode_id).unwrap().clone();
            let mut requests = Vec::new();
            for region_number in region_role_set.leader_regions {
                let Some(table_ids) = missing_tables_by_region.get(&region_number) else {
                    continue;
                };
                for table_id in table_ids {
                    requests.push((
                        self.make_request(region_number, *table_id, ctx)?,
                        *table_id,
                        region_number,
                    ));
                }
            }
            if requests.is_empty() {
                continue;
            }
            let requester = ctx.node_manager.datanode(&peer).await;
            create_table_tasks.push(async move {
                let mut created_regions = Vec::new();
                for (request, table_id, region_number) in requests {
                    if let Err(error) = requester
                        .handle(request)
                        .await
                        .map_err(add_peer_context_if_needed(peer.clone()))
                    {
                        return (created_regions, Err(error));
                    }
                    created_regions.push((table_id, region_number));
                }
                (created_regions, Ok(()))
            });
        }

        let results = future::join_all(create_table_tasks).await;
        let mut successful_regions_by_table = HashMap::<TableId, usize>::new();
        let mut first_error = None;
        let mut created_regions = Vec::new();
        for (task_created_regions, result) in results {
            for (table_id, _) in &task_created_regions {
                *successful_regions_by_table.entry(*table_id).or_default() += 1;
            }
            created_regions.extend(task_created_regions);
            if first_error.is_none() {
                first_error = result.err();
            }
        }
        let created_region_count = successful_regions_by_table.values().sum();
        let created_region_table_count = missing_regions_by_table
            .iter()
            .filter(|(table_id, region_numbers)| {
                successful_regions_by_table
                    .get(table_id)
                    .is_some_and(|count| *count == region_numbers.len())
            })
            .count();
        ctx.volatile_ctx
            .result_summary
            .record_created_regions(created_region_table_count, created_region_count);
        for (table_id, region_number) in created_regions {
            if let Some(region_numbers) =
                ctx.volatile_ctx.missing_regions_by_table.get_mut(&table_id)
            {
                region_numbers.retain(|number| *number != region_number);
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
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
        ctx.volatile_ctx
            .result_summary
            .mark_regions_reconciled(created_region_table_count, created_region_count);
        ctx.volatile_ctx.missing_regions_by_table.clear();
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
        region_number: RegionNumber,
        table_id: TableId,
        ctx: &ReconcileLogicalTablesContext,
    ) -> Result<RegionRequest> {
        let physical_table_id = ctx.table_id();
        let table_name = ctx.table_name();
        let Some((_, table_info)) = ctx
            .persistent_ctx
            .create_tables
            .iter()
            .find(|(candidate, _)| *candidate == table_id)
        else {
            return error::UnexpectedSnafu {
                err_msg: format!("Missing table info for logical table {table_id}"),
            }
            .fail();
        };
        let request_builder =
            create_region_request_from_raw_table_info(table_info, physical_table_id)?;
        let storage_path = region_storage_path(&table_name.catalog_name, &table_name.schema_name);
        let partition_exprs = prepare_partition_exprs(ctx, table_id);
        let region_id = RegionId::new(table_id, region_number);
        let request = request_builder.build_one(
            region_id,
            storage_path,
            &HashMap::new(),
            &partition_exprs,
        )?;

        Ok(RegionRequest {
            header: Some(RegionRequestHeader {
                tracing_context: TracingContext::from_current_span().to_w3c(),
                ..Default::default()
            }),
            body: Some(region_request::Body::Creates(CreateRequests {
                requests: vec![request],
            })),
        })
    }

    async fn missing_regions_by_table(
        &self,
        ctx: &mut ReconcileLogicalTablesContext,
        region_routes: &[crate::rpc::router::RegionRoute],
    ) -> Result<HashMap<TableId, Vec<RegionNumber>>> {
        let table_ids = ctx
            .persistent_ctx
            .create_tables
            .iter()
            .map(|(table_id, _)| *table_id)
            .collect::<Vec<_>>();
        if table_ids.iter().all(|table_id| {
            ctx.volatile_ctx
                .missing_regions_by_table
                .contains_key(table_id)
        }) {
            return Ok(ctx.volatile_ctx.missing_regions_by_table.clone());
        }

        let lister = RegionMetadataLister::new(ctx.node_manager.clone());
        ctx.volatile_ctx.missing_regions_by_table.clear();
        for table_id in table_ids {
            let region_metadatas = lister.list_with_ids(table_id, region_routes).await?;
            ctx.volatile_ctx
                .result_summary
                .record_scanned_regions(region_metadatas.len());
            let missing_regions = region_metadatas
                .into_iter()
                .filter_map(|(region_id, metadata)| {
                    metadata.is_none().then_some(region_id.region_number())
                })
                .collect::<Vec<_>>();
            ctx.volatile_ctx
                .missing_regions_by_table
                .insert(table_id, missing_regions);
        }
        Ok(ctx.volatile_ctx.missing_regions_by_table.clone())
    }
}

/// Creates a region request builder from a raw table info.
///
/// Note: This function is primarily intended for creating logical tables or allocating placeholder regions.
fn create_region_request_from_raw_table_info(
    table_info: &TableInfo,
    physical_table_id: TableId,
) -> Result<CreateRequestBuilder> {
    let template = build_template_from_raw_table_info(table_info)?;
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
