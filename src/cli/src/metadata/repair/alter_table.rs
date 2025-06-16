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

use client::api::v1::alter_table_expr::Kind;
use client::api::v1::region::{region_request, AlterRequests, RegionRequest, RegionRequestHeader};
use client::api::v1::{AddColumn, AddColumns, AlterTableExpr};
use common_meta::ddl::alter_logical_tables::make_alter_region_request;
use common_meta::peer::Peer;
use common_meta::rpc::router::{find_leader_regions, RegionRoute};
use operator::expr_helper::column_schemas_to_defs;
use snafu::ResultExt;
use store_api::storage::{RegionId, TableId};
use table::metadata::RawTableInfo;

use crate::error::{CovertColumnSchemasToDefsSnafu, Result};

/// Generates alter table expression for all columns.
pub fn generate_alter_table_expr_for_all_columns(
    table_info: &RawTableInfo,
) -> Result<AlterTableExpr> {
    let schema = &table_info.meta.schema;

    let mut alter_table_expr = AlterTableExpr {
        catalog_name: table_info.catalog_name.to_string(),
        schema_name: table_info.schema_name.to_string(),
        table_name: table_info.name.to_string(),
        ..Default::default()
    };

    let primary_keys = table_info
        .meta
        .primary_key_indices
        .iter()
        .map(|i| schema.column_schemas[*i].name.clone())
        .collect::<Vec<_>>();

    let add_columns = column_schemas_to_defs(schema.column_schemas.clone(), &primary_keys)
        .context(CovertColumnSchemasToDefsSnafu)?;

    alter_table_expr.kind = Some(Kind::AddColumns(AddColumns {
        add_columns: add_columns
            .into_iter()
            .map(|col| AddColumn {
                column_def: Some(col),
                location: None,
                add_if_not_exists: true,
            })
            .collect(),
    }));

    Ok(alter_table_expr)
}

/// Makes an alter region request for a peer.
pub fn make_alter_region_request_for_peer(
    logical_table_id: TableId,
    alter_table_expr: &AlterTableExpr,
    schema_version: u64,
    peer: &Peer,
    region_routes: &[RegionRoute],
) -> Result<RegionRequest> {
    let regions_on_this_peer = find_leader_regions(region_routes, peer);
    let mut requests = Vec::with_capacity(regions_on_this_peer.len());
    for region_number in &regions_on_this_peer {
        let region_id = RegionId::new(logical_table_id, *region_number);
        let request = make_alter_region_request(region_id, alter_table_expr, schema_version);
        requests.push(request);
    }

    Ok(RegionRequest {
        header: Some(RegionRequestHeader::default()),
        body: Some(region_request::Body::Alters(AlterRequests { requests })),
    })
}
