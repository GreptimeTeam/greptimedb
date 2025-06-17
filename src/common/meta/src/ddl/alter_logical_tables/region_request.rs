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

use api::v1::alter_table_expr::Kind;
use api::v1::region::{
    alter_request, region_request, AddColumn, AddColumns, AlterRequest, AlterRequests,
    RegionColumnDef, RegionRequest, RegionRequestHeader,
};
use api::v1::{self, AlterTableExpr};
use common_telemetry::tracing_context::TracingContext;
use store_api::storage::RegionId;

use crate::ddl::alter_logical_tables::AlterLogicalTablesProcedure;
use crate::error::Result;
use crate::peer::Peer;
use crate::rpc::router::{find_leader_regions, RegionRoute};

impl AlterLogicalTablesProcedure {
    pub(crate) fn make_request(
        &self,
        peer: &Peer,
        region_routes: &[RegionRoute],
    ) -> Result<RegionRequest> {
        let alter_requests = self.make_alter_region_requests(peer, region_routes)?;
        let request = RegionRequest {
            header: Some(RegionRequestHeader {
                tracing_context: TracingContext::from_current_span().to_w3c(),
                ..Default::default()
            }),
            body: Some(region_request::Body::Alters(alter_requests)),
        };

        Ok(request)
    }

    fn make_alter_region_requests(
        &self,
        peer: &Peer,
        region_routes: &[RegionRoute],
    ) -> Result<AlterRequests> {
        let tasks = &self.data.tasks;
        let regions_on_this_peer = find_leader_regions(region_routes, peer);
        let mut requests = Vec::with_capacity(tasks.len() * regions_on_this_peer.len());
        for (task, table) in self
            .data
            .tasks
            .iter()
            .zip(self.data.table_info_values.iter())
        {
            for region_number in &regions_on_this_peer {
                let region_id = RegionId::new(table.table_info.ident.table_id, *region_number);
                let request = make_alter_region_request(
                    region_id,
                    &task.alter_table,
                    table.table_info.ident.version,
                );
                requests.push(request);
            }
        }

        Ok(AlterRequests { requests })
    }
}

/// Makes an alter region request.
pub fn make_alter_region_request(
    region_id: RegionId,
    alter_table_expr: &AlterTableExpr,
    schema_version: u64,
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
        schema_version,
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
