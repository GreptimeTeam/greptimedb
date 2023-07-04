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
use common_meta::peer::Peer;
use common_meta::rpc::router::TableRoute;
use common_procedure::error::Error as ProcedureError;
use snafu::{location, Location, ResultExt};
use table::engine::TableReference;
use table::metadata::{RawTableInfo, TableId};

use crate::error::{self, Error, Result};
use crate::service::router::create_table_global_value;

pub struct TableMetadata<'a> {
    pub table_global_key: TableGlobalKey,
    pub table_global_value: TableGlobalValue,
    pub table_route_key: TableRouteKey<'a>,
    pub table_route_value: TableRouteValue,
}

pub fn build_table_route_value(table_route: TableRoute) -> Result<TableRouteValue> {
    let (peers, table_route) = table_route
        .try_into_raw()
        .context(error::ConvertProtoDataSnafu)?;

    Ok(TableRouteValue {
        peers,
        table_route: Some(table_route),
    })
}

pub fn build_table_metadata_key(
    table_ref: TableReference<'_>,
    table_id: TableId,
) -> (TableGlobalKey, TableRouteKey) {
    let table_route_key = TableRouteKey {
        table_id,
        catalog_name: table_ref.catalog,
        schema_name: table_ref.schema,
        table_name: table_ref.table,
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

pub fn handle_request_datanode_error(datanode: Peer) -> impl FnOnce(client::error::Error) -> Error {
    move |err| {
        if matches!(err, client::error::Error::FlightGet { .. }) {
            error::RetryLaterSnafu {
                reason: format!("Failed to execute operation on datanode, source: {}", err),
            }
            .build()
        } else {
            error::Error::RequestDatanode {
                location: location!(),
                peer: datanode,
                source: err,
            }
        }
    }
}

pub fn handle_retry_error(e: Error) -> ProcedureError {
    if matches!(e, error::Error::RetryLater { .. }) {
        ProcedureError::retry_later(e)
    } else {
        ProcedureError::external(e)
    }
}
