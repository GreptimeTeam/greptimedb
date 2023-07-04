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

use api::v1::meta::TableRouteValue;
use catalog::helper::{TableGlobalKey, TableGlobalValue};
use common_meta::key::TableRouteKey;
use common_meta::rpc::router::TableRoute;
use snafu::ResultExt;
use table::engine::TableReference;
use table::metadata::{RawTableInfo, TableId};

use crate::error::{self, Result};
use crate::service::router::create_table_global_value;

pub struct TableMetadata<'a> {
    pub table_global_key: TableGlobalKey,
    pub table_global_value: TableGlobalValue,
    pub table_route_key: TableRouteKey<'a>,
    pub table_route_value: TableRouteValue,
}

pub fn build_table_metadata_key(
    table_ref: TableReference<'_>,
    table_id: TableId,
) -> (TableGlobalKey, TableRouteKey) {
    let table_route_key = TableRouteKey {
        table_id: table_id as u64,
        catalog_name: table_ref.catalog,
        schema_name: table_ref.schema,
        table_name: table_ref.schema,
    };

    let table_global_key = TableGlobalKey {
        catalog_name: table_ref.catalog.to_string(),
        schema_name: table_ref.schema.to_string(),
        table_name: table_ref.table.to_string(),
    };

    (table_global_key, table_route_key)
}

pub fn build_table_metadata(
    table_ref: TableReference<'_>,
    table_route: TableRoute,
    table_info: RawTableInfo,
) -> Result<TableMetadata> {
    let table_id = table_info.ident.table_id;

    let (table_global_key, table_route_key) = build_table_metadata_key(table_ref, table_id);

    let (peers, table_route) = table_route
        .try_into_raw()
        .context(error::ConvertProtoDataSnafu)?;

    let table_route_value = TableRouteValue {
        peers,
        table_route: Some(table_route),
    };

    let table_global_value = create_table_global_value(&table_route_value, table_info)?;

    Ok(TableMetadata {
        table_global_key,
        table_global_value,
        table_route_key,
        table_route_value,
    })
}
