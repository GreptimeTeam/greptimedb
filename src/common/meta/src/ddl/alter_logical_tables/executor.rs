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

use api::region::RegionResponse;
use api::v1::alter_table_expr::Kind;
use api::v1::region::{
    AddColumn, AddColumns, AlterRequest, AlterRequests, RegionColumnDef, RegionRequest,
    RegionRequestHeader, alter_request, region_request,
};
use api::v1::{self, AlterTableExpr};
use common_telemetry::tracing_context::TracingContext;
use common_telemetry::{debug, warn};
use futures::future;
use store_api::metadata::ColumnMetadata;
use store_api::storage::{RegionId, RegionNumber, TableId};

use crate::ddl::utils::{add_peer_context_if_needed, raw_table_info};
use crate::error::Result;
use crate::instruction::CacheIdent;
use crate::key::table_info::TableInfoValue;
use crate::key::{DeserializedValueWithBytes, RegionDistribution, TableMetadataManagerRef};
use crate::region_rpc::RegionRpcRef;
use crate::rpc::router::{RegionRoute, find_leaders, region_distribution};

/// [AlterLogicalTablesExecutor] performs:
/// - Alters logical regions on the datanodes.
/// - Updates table metadata for alter table operation.
pub struct AlterLogicalTablesExecutor<'a> {
    /// The alter table expressions.
    ///
    /// The first element is the logical table id, the second element is the alter table expression.
    alters: Vec<(TableId, &'a AlterTableExpr)>,
}

impl<'a> AlterLogicalTablesExecutor<'a> {
    pub fn new(alters: Vec<(TableId, &'a AlterTableExpr)>) -> Self {
        Self { alters }
    }

    /// Alters logical regions on the datanodes.
    pub(crate) async fn on_alter_regions(
        &self,
        region_rpc: &RegionRpcRef,
        region_routes: &[RegionRoute],
    ) -> Result<Vec<RegionResponse>> {
        let region_distribution = region_distribution(region_routes);
        let leaders = find_leaders(region_routes)
            .into_iter()
            .map(|p| (p.id, p))
            .collect::<HashMap<_, _>>();
        let mut alter_region_tasks = Vec::with_capacity(leaders.len());
        for (datanode_id, region_role_set) in region_distribution {
            if region_role_set.leader_regions.is_empty() {
                continue;
            }
            // Safety: must exists.
            let peer = leaders.get(&datanode_id).unwrap();
            let requests = self.make_alter_region_request(&region_role_set.leader_regions);
            let peer = peer.clone();
            let region_rpc = region_rpc.clone();

            debug!("Sending alter region requests to datanode {}", peer);
            alter_region_tasks.push(async move {
                region_rpc
                    .handle_region(&peer, make_request(requests))
                    .await
                    .map_err(add_peer_context_if_needed(peer))
            });
        }

        future::join_all(alter_region_tasks)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()
    }

    fn make_alter_region_request(&self, region_numbers: &[RegionNumber]) -> AlterRequests {
        let mut requests = Vec::with_capacity(region_numbers.len() * self.alters.len());
        for (table_id, alter) in self.alters.iter() {
            for region_number in region_numbers {
                let region_id = RegionId::new(*table_id, *region_number);
                let request = make_alter_region_request(region_id, alter);
                requests.push(request);
            }
        }

        AlterRequests { requests }
    }

    /// Updates table metadata for alter table operation.
    ///
    /// ## Panic:
    /// - If the region distribution is not set when updating table metadata.
    pub(crate) async fn on_alter_metadata(
        physical_table_id: TableId,
        table_metadata_manager: &TableMetadataManagerRef,
        current_table_info_value: &DeserializedValueWithBytes<TableInfoValue>,
        region_distribution: RegionDistribution,
        physical_columns: &[ColumnMetadata],
    ) -> Result<()> {
        if physical_columns.is_empty() {
            warn!(
                "No physical columns found, leaving the physical table's schema unchanged when altering logical tables"
            );
            return Ok(());
        }

        let table_ref = current_table_info_value.table_ref();
        let table_id = physical_table_id;

        // Generates new table info
        let old_raw_table_info = current_table_info_value.table_info.clone();
        let new_raw_table_info =
            raw_table_info::build_new_physical_table_info(old_raw_table_info, physical_columns);

        debug!(
            "Starting update table: {} metadata, table_id: {}, new table info: {:?}",
            table_ref, table_id, new_raw_table_info
        );

        table_metadata_manager
            .update_table_info(
                current_table_info_value,
                Some(region_distribution),
                new_raw_table_info,
            )
            .await?;

        Ok(())
    }

    /// Builds the cache ident keys for the alter logical tables.
    ///
    /// The cache ident keys are:
    /// - The table id of the logical tables.
    /// - The table name of the logical tables.
    /// - The table id of the physical table.
    pub(crate) fn build_cache_ident_keys(
        physical_table_info: &TableInfoValue,
        logical_table_info_values: &[&TableInfoValue],
    ) -> Vec<CacheIdent> {
        let mut cache_keys = Vec::with_capacity(logical_table_info_values.len() * 2 + 2);
        cache_keys.extend(logical_table_info_values.iter().flat_map(|table| {
            vec![
                CacheIdent::TableId(table.table_info.ident.table_id),
                CacheIdent::TableName(table.table_name()),
            ]
        }));
        cache_keys.push(CacheIdent::TableId(
            physical_table_info.table_info.ident.table_id,
        ));
        cache_keys.push(CacheIdent::TableName(physical_table_info.table_name()));

        cache_keys
    }
}

fn make_request(alter_requests: AlterRequests) -> RegionRequest {
    RegionRequest {
        header: Some(RegionRequestHeader {
            tracing_context: TracingContext::from_current_span().to_w3c(),
            ..Default::default()
        }),
        body: Some(region_request::Body::Alters(alter_requests)),
    }
}

/// Makes an alter region request.
pub fn make_alter_region_request(
    region_id: RegionId,
    alter_table_expr: &AlterTableExpr,
) -> AlterRequest {
    let region_id = region_id.as_u64();
    let kind = match &alter_table_expr.kind {
        Some(Kind::AddColumns(add_columns)) => Some(alter_request::Kind::AddColumns(
            to_region_add_columns(add_columns),
        )),
        _ => unreachable!(), // Safety: we have checked the kind in check_input_tasks
    };

    AlterRequest {
        region_id,
        schema_version: 0,
        kind,
    }
}

fn to_region_add_columns(add_columns: &v1::AddColumns) -> AddColumns {
    let add_columns = add_columns
        .add_columns
        .iter()
        .map(|add_column| {
            let region_column_def = RegionColumnDef {
                column_def: add_column.column_def.clone(),
                ..Default::default() // other fields are not used in alter logical table
            };
            AddColumn {
                column_def: Some(region_column_def),
                ..Default::default() // other fields are not used in alter logical table
            }
        })
        .collect();
    AddColumns { add_columns }
}
