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

use client::api::v1::region::{region_request, CreateRequests, RegionRequest, RegionRequestHeader};
use client::api::v1::CreateTableExpr;
use common_meta::ddl::create_logical_tables::create_region_request_builder;
use common_meta::ddl::utils::region_storage_path;
use common_meta::peer::Peer;
use common_meta::rpc::router::{find_leader_regions, RegionRoute};
use operator::expr_helper::column_schemas_to_defs;
use snafu::ResultExt;
use store_api::storage::{RegionId, TableId};
use table::metadata::RawTableInfo;

use crate::error::{CovertColumnSchemasToDefsSnafu, Result};

/// Generates a `CreateTableExpr` from a `RawTableInfo`.
pub fn generate_create_table_expr(table_info: &RawTableInfo) -> Result<CreateTableExpr> {
    let schema = &table_info.meta.schema;
    let primary_keys = table_info
        .meta
        .primary_key_indices
        .iter()
        .map(|i| schema.column_schemas[*i].name.clone())
        .collect::<Vec<_>>();

    let timestamp_index = schema.timestamp_index.as_ref().unwrap();
    let time_index = schema.column_schemas[*timestamp_index].name.clone();
    let column_defs = column_schemas_to_defs(schema.column_schemas.clone(), &primary_keys)
        .context(CovertColumnSchemasToDefsSnafu)?;
    let table_options = HashMap::from(&table_info.meta.options);

    Ok(CreateTableExpr {
        catalog_name: table_info.catalog_name.to_string(),
        schema_name: table_info.schema_name.to_string(),
        table_name: table_info.name.to_string(),
        desc: String::default(),
        column_defs,
        time_index,
        primary_keys,
        create_if_not_exists: true,
        table_options,
        table_id: None,
        engine: table_info.meta.engine.to_string(),
    })
}

/// Makes a create region request for a peer.
pub fn make_create_region_request_for_peer(
    logical_table_id: TableId,
    physical_table_id: TableId,
    create_table_expr: &CreateTableExpr,
    peer: &Peer,
    region_routes: &[RegionRoute],
) -> Result<RegionRequest> {
    let regions_on_this_peer = find_leader_regions(region_routes, peer);
    let mut requests = Vec::with_capacity(regions_on_this_peer.len());
    let request_builder =
        create_region_request_builder(create_table_expr, physical_table_id).unwrap();

    let catalog = &create_table_expr.catalog_name;
    let schema = &create_table_expr.schema_name;
    let storage_path = region_storage_path(catalog, schema);

    for region_number in &regions_on_this_peer {
        let region_id = RegionId::new(logical_table_id, *region_number);
        let region_request =
            request_builder.build_one(region_id, storage_path.clone(), &HashMap::new());
        requests.push(region_request);
    }

    Ok(RegionRequest {
        header: Some(RegionRequestHeader::default()),
        body: Some(region_request::Body::Creates(CreateRequests { requests })),
    })
}
